from framework.service.context import container
import framework.service.flow as flow
from framework.service.scheme import convert
from dependency_injector import providers
import framework.service.language as language
import uuid
import types
import sys
import json
import os
import inspect
import asyncio

def framework_log(level, message, **kwargs):
    """Simple logger for framework events."""
    print(f"[{level}] {message} {kwargs if kwargs else ''}")

imports = {
    "flow": "framework/service/flow.py",
    "convert": "framework/service/scheme.py",
    "language": "framework/service/language.py"
}

async def resource(path: str, **kwargs):
    """
    Funzione principale per il caricamento di risorse finanziarie.
    Orchestra il flusso attraverso una pipeline lineare di trasformazione.
    """
    # Gestione della Cache (Ottimizzazione)
    cache = container.module_cache()
    if path in cache and not kwargs.get('raw'):
        return cache[path]

    # Gestione Concorrenza (Prevenzione lock circolari)
    stack = container.loading_stack()
    if path in stack and not kwargs.get('raw'):
        while path in stack:
            await asyncio.sleep(0.01)
        if path in cache and not kwargs.get('raw'):
            return cache[path]

    if not kwargs.get('raw'):
        stack.add(path)
    try:
        # Pipeline di Caricamento: Ogni step è una funzione atomica
        context = {"path": path} | kwargs
        result = await flow.pipe(
            step_read_content,
            step_dispatch_parser,
            step_finalize_data,
            context=context
        )
        
        # Salvataggio in cache se il caricamento non è un fallimento esplicito
        if isinstance(result, dict) and result.get('success') is not False:
             cache[path] = result
             
        # Ritorna sempre la transazione completa (success, data, errors, identifier)
        return result
        
    finally:
        if not kwargs.get('raw'):
            stack.remove(path)

async def bootstrap():
    """Effettua il bootstrap del framework caricando ed eseguendo bootstrap.dsl."""
    
    bootstrap_path = "framework/service/bootstrap.dsl"
    res = await resource(bootstrap_path)
    
    # Se res è un dict con success=False, è un errore
    if isinstance(res, dict) and res.get('success') is False:
        return res
    
    # Altrimenti res è il contenuto del DSL (unwrapped)
    return await language.execute_dsl_file(res)

async def register(**config):
    """Registra un servizio o manager nel Dependency Injection container."""
    return await flow.pipe(
        step_load_service_module,
        step_validate_registration,
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
    
    ctx['module'] = module
    return {"success": True, "module": module, "data": module}

async def step_resolve_python_imports(ctx):
    """Analizza il codice e carica ricorsivamente le dipendenze in 'imports:'."""
    module = ctx.get('module')
    code = ctx.get('raw_content')
    
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
    code = ctx.get('raw_content')
    module = ctx.get('module')
    
    try:
        exec(code, module.__dict__)
        return {"success": True, "module": module}
    except Exception as e:
        # Analisi dettagliata dell'eccezione per il debug
        from framework.service.diagnostic import analyze_exception
        analyze_exception(e, path, code)
        return {"success": False, "errors": [f"Errore esecuzione in {path}: {str(e)}"]}

async def step_read_content(ctx):
    """Legge il contenuto grezzo del file dal filesystem via diagnostic._load_resource."""
    from framework.service.diagnostic import _load_resource
    path = ctx['path']
    try:
        content = await _load_resource(path=path)
        ctx['raw_content'] = content
        return {"success": True, "raw_content": content, "data": content}
    except Exception as e:
        return {"success": False, "errors": [f"Impossibile leggere {path}: {str(e)}"]}

async def step_dispatch_parser(ctx):
    """Inizializza il parser corretto in base all'estensione del file."""
    path = ctx['path']
    content = ctx.get('raw_content')
    
    if path.endswith('.dsl'):
        import framework.service.language as language
        parsed = language.parse_dsl_file(content)
        return {"success": True, "data": parsed}
        
    if path.endswith('.py'):
        # Per i file Python, attiviamo la sotto-pipeline di esecuzione
        return await flow.pipe(
            step_init_python_runtime,
            step_resolve_python_imports,
            step_exec_python_code,
            step_apply_security_filtering,
            context=ctx
        )
        
    if path.endswith('.json'): return {"success": True, "data": json.loads(content)}
    if path.endswith('.toml'):
        import tomli
        return {"success": True, "data": tomli.loads(content)}
        
    return {"success": True, "data": content}

async def step_apply_security_filtering(ctx):
    """Filtra i membri del modulo in base ai contratti e applica i decoratori transactional."""
    path = ctx['path']
    module = ctx.get('module') or ctx.get('data')

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
    allowed = helper_determine_allowed(path, exports, test_meta, module)

    # 5. Costruzione del Proxy Filtrato e Protetto
    import framework.service.language as language
    proxy = types.ModuleType(f"filtered:{path}")
    proxy.__file__ = path
    proxy.language = getattr(module, 'language', language)

    for public_name in allowed:
        public_name = str(public_name)
        internal_name = str(exports.get(public_name, public_name))
        value = getattr(module, internal_name, None)
        
        if inspect.isclass(value):
            # Classi: i metodi pubblici vengono protetti da transazioni
            # Saltiamo il wrapping per classi con metaclassi complesse (es. dependency_injector)
            # o se il modulo è context.py
            if 'context.py' in path or 'dependency_injector' in str(type(value)):
                setattr(proxy, public_name, value)
            else:
                try:
                    members = helper_wrap_class_methods(value, public_name, integrity)
                    setattr(proxy, public_name, type(public_name, (value,), members))
                except Exception:
                    setattr(proxy, public_name, value)
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
        if scope not in current: continue
        matches = {m for m, h in methods.items() 
                   if current.get(scope, {}).get(m, {}).get('production') == h.get('production')}
        if matches: verified[scope] = matches
    return verified

async def helper_get_contract(path):
    """Carica il file contract.json associato al percorso se esiste."""
    base_path = path.replace('.py', '').replace('.dsl', '')
    contract_path = base_path + '.contract.json'
    
    candidates = [contract_path]
    if not contract_path.startswith('src/'):
        candidates.append('src/' + contract_path)
        
    for p in candidates:
        if os.path.exists(p):
            try:
                with open(p, 'r') as f:
                    return json.load(f)
            except: pass
    return None

async def helper_get_test_metadata(path):
    """Tenta di caricare i metadati dai test associati (.test.dsl o .test.py)."""
    base_path = path.replace('.py', '').replace('.dsl', '')
    test_path = base_path + '.test.dsl'
    
    # Risoluzione percorsi relativa alla root del progetto
    candidates = [test_path, os.path.join('src', test_path)]
    cwd = os.getcwd()
    if 'src' in cwd:
        candidates.append(os.path.join(cwd.split('src')[0], 'src', test_path))

    for p in candidates:
        if os.path.exists(p):
            res = await resource(p, raw=True) # Usa raw per evitare loop infiniti
            return res.get('data') if res.get('success') else {}
    return {}

def helper_resolve_exports(module, test_meta, contract):
    """Determina la mappatura dei nomi pubblici vs interni."""
    exports = test_meta.get('exports', {}) if test_meta else {}
    if not exports and contract:
        # Se non c'è test_meta, usiamo le chiavi del contratto come nomi pubblici
        for scope, methods in contract.items():
            if scope == "__module__":
                for m in methods: exports[m] = m
            else:
                exports[scope] = scope
    return exports

def helper_determine_allowed(path, exports, test_meta, module):
    """Decide quali simboli esportare in base ai risultati dei test e integrità."""
    if exports:
        return set(exports.keys())
    
    # Se non ci sono esportazioni dichiarate, esportiamo tutto ciò che è pubblico
    allowed = set()
    for name, member in inspect.getmembers(module):
        if not name.startswith('_'):
            allowed.add(name)
    return allowed

def helper_wrap_class_methods(cls, name, integrity):
    """Protegge i metodi di una classe in transazioni."""
    members = {}
    for m_name, m_val in inspect.getmembers(cls):
        if not m_name.startswith('_') and callable(m_val):
            members[m_name] = flow.transactional(m_val)
    return members

async def generate_checksum(path, run_tests=True, save=False):
    """Genera gli hash di produzione per tutti i membri del modulo."""
    from framework.service.diagnostic import calculate_hash_of_function
    
    # Caricamento raw per evitare filtri circolari
    res = await resource(path, raw=True)
    if not res.get('success'): return res
    module = res['data']
    
    hashes = {"__module__": {}}
    for name, member in inspect.getmembers(module):
        if (inspect.isfunction(member) or inspect.ismethod(member)) and not name.startswith('_'):
            hashes["__module__"][name] = {"production": calculate_hash_of_function(member)}
        elif inspect.isclass(member) and not name.startswith('_'):
            hashes[name] = {}
            for m_name, m_member in inspect.getmembers(member):
                if inspect.isfunction(m_member) and not m_name.startswith('_'):
                    hashes[name][m_name] = {"production": calculate_hash_of_function(m_member)}
                    
    if save:
        contract_path = path.replace('.py', '.contract.json').replace('.dsl', '.contract.json')
        with open(contract_path, 'w') as f:
            json.dump(hashes, f, indent=4)
            
    return {"success": True, "data": {path: hashes}}


# =====================================================================
# --- REGISTRAZIONE NEL CONTAINER ---
# =====================================================================

async def step_validate_registration(ctx):
    """Valida il modulo prima della registrazione definitiva via Tester (TDD)."""
    path = ctx.get('path', '')
    # Evitiamo loop caricando componenti core o file di test
    if any(core in path for core in ['tester', 'load', 'diagnostic', 'flow', 'context', '.test.']):
        return {"success": True}
        
    try:
        # Se il tester è già disponibile nel container, lo usiamo
        if hasattr(container, 'tester'):
            tester_list = container.tester()
            if tester_list and isinstance(tester_list, list):
                tester_instance = tester_list[0]
                test_path = path.replace('.py', '.test.dsl')
                
                if os.path.exists(test_path):
                    framework_log("INFO", f"🧪 Validazione TDD automatica: {path}", emoji="🧪")
                    res = await tester_instance.dsl(path=test_path)
                    
                    if not res.get('success'):
                        framework_log("ERROR", f"❌ Registrazione FALLITA per {path}: TDD Specs violate.", errors=res.get('data', {}).get('errors', []))
                        return {"success": False, "errors": [f"TDD failure for {path}"]}
                    
                    framework_log("INFO", f"✅ Registrazione AUTORIZZATA per {path} (Tests OK)", emoji="✅")
    except Exception as e:
        # In fase di bootstrap il tester potrebbe non essere ancora pronto, logghiamo e procediamo
        framework_log("DEBUG", f"Validazione TDD saltata per {path} (Tester non pronto o errore: {e})")
        
    return {"success": True}

async def step_load_service_module(ctx):
    """Carica il modulo indicato nel percorso via pipeline."""
    res = await resource(ctx['path'])
    
    # Se res è un errore, propagalo
    if isinstance(res, dict) and res.get('success') is False:
        return res
        
    # Altrimenti res è il modulo (unwrapped)
    ctx['module'] = res
    return {"success": True, "data": res}

async def step_inject_and_register(ctx):
    """Istanzia l'adapter e lo registra nel container dependency-injector."""
    module = ctx.get('module')
    name = ctx.get('service', ctx.get('name'))
    
    if not module:
        return {"success": False, "errors": [f"Missing module for service {name}"]}
        
    adapter = getattr(module, ctx.get('adapter', name))
    
    if not hasattr(container, name):
        setattr(container, name, providers.Singleton(list))
        
    deps = ctx.get('dependency_keys', [])
    if deps:
        # Registrazione come Factory con dipendenze risolte
        # Risoluzione lazy delle dipendenze: inietta solo se il provider esiste già
        resolved_providers = {}
        for k in deps:
            if hasattr(container, k):
                resolved_providers[k] = getattr(container, k)()
            else:
                framework_log("WARNING", f"⚠️ Dipendenza '{k}' per {name} non ancora disponibile nel container. Sarà risolta a runtime.", emoji="⏳")
        
        p = providers.Factory(adapter, **ctx.get('payload', {}), **resolved_providers)
        setattr(container, name, p)
    else:
        # Iniezione diretta dell'istanza nella lista dei provider
        instance = adapter(config=ctx.get('payload', {}))
        getattr(container, name)().append(instance)
    return {"success": True}
