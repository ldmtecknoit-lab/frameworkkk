import os, sys, asyncio, types, inspect, uuid, json
from framework.service.context import container
import framework.service.flow as flow
from framework.service.scheme import convert
from dependency_injector import providers

async def resource(path: str):
    """
    Funzione principale per il caricamento di risorse finanziarie.
    Orchestra il flusso attraverso una pipeline lineare di trasformazione.
    """
    # Gestione della Cache (Ottimizzazione)
    cache = container.module_cache()
    if path in cache:
        return {"success": True, "data": cache[path]}

    # Gestione Concorrenza (Prevenzione lock circolari)
    stack = container.loading_stack()
    if path in stack:
        while path in stack:
            await asyncio.sleep(0.01)
        if path in cache:
            return {"success": True, "data": cache[path]}

    stack.add(path)
    try:
        # Pipeline di Caricamento: Ogni step è una funzione atomica
        context = {"path": path}
        result = await flow.pipe(
            step_read_content,
            step_dispatch_parser,
            step_finalize_data,
            context=context
        )
        
        # Salvataggio in cache se il caricamento ha avuto successo
        if result and isinstance(result, dict) and result.get('success'):
            cache[path] = result['data']
        return result
        
    finally:
        stack.remove(path)

async def register(**config):
    """Registra un servizio o manager nel Dependency Injection container."""
    return await flow.pipe(
        step_load_service_module,
        step_inject_and_register,
        context=config
    )

async def step_init_python_runtime(ctx):
    """Prepara un ambiente isolato per il nuovo modulo Python."""
    path = ctx['path']
    module_id = f"mod_{uuid.uuid4().hex[:8]}"
    
    # Creazione dell'oggetto modulo
    module = types.ModuleType(module_id)
    module.__file__ = path
    
    # Registrazione negli indici di sistema per permettere la risoluzione di nomi
    sys.modules[module_id] = module
    sys.modules[f"loading:{path}"] = module
    
    return {"success": True, "module": module}

async def step_resolve_python_imports(ctx):
    """Analizza il codice e carica ricorsivamente le dipendenze in 'imports:'."""
    module = ctx['outputs'][-1].get('module')
    code = ctx['outputs'][0].get('raw_content')
    
    for line in code.splitlines():
        if 'imports:' in line:
            try:
                # Estrazione JSON dalla direttiva imports: {...};
                json_part = line.split('imports:')[1].split(';')[0]
                dep_map = await convert(json_part, dict, 'json')
                
                # Caricamento ricorsivo di ogni dipendenza
                for key, dep_path in dep_map.items():
                    res = await resource(dep_path)
                    setattr(module, key, res.get('data'))
            except Exception:
                pass # Gli import falliti vengono gestiti a runtime
            break
            
    return {"success": True, "module": module}

async def step_exec_python_code(ctx):
    """Esegue il codice sorgente nel contesto del modulo inizializzato."""
    path = ctx['path']
    code = ctx['outputs'][0].get('raw_content')
    module = ctx['outputs'][-1].get('module')
    
    try:
        exec(code, module.__dict__)
        return {"success": True, "module": module}
    except Exception as e:
        # Analisi dettagliata dell'eccezione per il debug
        analyze_exception(e, path, code)
        return {"success": False, "errors": [f"Errore esecuzione in {path}: {str(e)}"]}

async def step_apply_security_filtering(ctx):
    """Filtra i membri del modulo in base ai contratti e applica i decoratori transactional."""
    path = ctx['path']
    module = ctx['outputs'][-1].get('module') or ctx.get('data')

    # Saltiamo il filtraggio per i file di test o se richiesto caricamento 'raw'
    if '.test.' in path or ctx.get('raw'):
        return {"success": True, "data": module}

    # 1. Caricamento metadati per la validazione (Contratti e Test)
    test_meta = await helper_get_test_metadata(path)
    contract = await helper_get_contract(path)
    
    # 2. Verifica integrità degli hash (Contratto vs Codice Reale)
    integrity = await helper_verify_integrity(path, contract)
    
    # 3. Risoluzione mappatura esportazioni (Aliasing)
    exports = helper_resolve_exports(module, test_meta, contract)
    
    # 4. Individuazione simboli autorizzati all'esposizione
    allowed = helper_determine_allowed(path, exports, test_meta, integrity)

    # 5. Costruzione del Proxy Filtrato e Protetto
    proxy = types.ModuleType(f"filtered:{path}")
    proxy.__file__ = path
    proxy.language = getattr(module, 'language', language)

    for public_name in allowed:
        internal_name = exports.get(public_name, public_name)
        value = getattr(module, internal_name, None)
        
        if inspect.isclass(value):
            # Classi: i metodi pubblici vengono protetti da transazioni
            members = helper_wrap_class_methods(value, public_name, integrity)
            setattr(proxy, public_name, type(public_name, (value,), members))
        else:
            # Funzioni: vengono avvolte automaticamente in transazioni
            setattr(proxy, public_name, flow.transactional(value) if callable(value) else value)

    framework_log("INFO", f"✅ Modulo Validato: {path}", symbols=list(allowed))
    return {"success": True, "data": proxy}

async def step_finalize_data(ctx):
    """Step finale per estrarre il dato risultante dalla pipe."""
    last_output = ctx['outputs'][-1]
    if isinstance(last_output, dict) and 'data' in last_output:
        return last_output
    return {"success": True, "data": last_output}

async def helper_verify_integrity(path, contract):
    """Verifica che gli hash nel contratto corrispondano al codice attuale."""
    if not contract: return {}
    # Nota: eseguiamo generate_checksum senza rieseguire i test per evitare loop
    res = await generate_checksum(path, run_tests=False)
    current = res.get('data', {}).get(path, {})
    
    verified = {}
    for scope, methods in contract.items():
        matches = {m for m, h in methods.items() 
                   if current.get(scope, {}).get(m, {}).get('production') == h.get('production')}
        if matches: verified[scope] = matches
    return verified


# =====================================================================
# --- REGISTRAZIONE NEL CONTAINER ---
# =====================================================================

async def step_load_service_module(ctx):
    """Carica il modulo indicato nel percorso via pipeline."""
    res = await resource(ctx['path'])
    return {"success": res.get('success'), "module": res.get('data')}

async def step_inject_and_register(ctx):
    """Istanzia l'adapter e lo registra nel container dependency-injector."""
    module = ctx['outputs'][-1].get('module')
    name = ctx.get('service', ctx.get('name'))
    adapter = getattr(module, ctx.get('adapter', name))
    
    if not hasattr(container, name):
        setattr(container, name, providers.Singleton(list))
        
    deps = ctx.get('dependency_keys', [])
    if deps:
        # Registrazione come Factory con dipendenze risolte
        p = providers.Factory(adapter, **ctx.get('payload', {}), 
                              providers={k: getattr(container, k)() for k in deps})
        setattr(container, name, p)
    else:
        # Iniezione diretta dell'istanza nella lista dei provider
        instance = adapter(config=ctx.get('payload', {}))
        getattr(container, name)().append(instance)
    return True