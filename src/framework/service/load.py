from framework.service.context import container
import framework.service.flow as flow
from framework.service.scheme import convert
import types
from dependency_injector import providers
import framework.service.language as language

imports = {
    "flow": "framework/service/flow.py",
    "convert": "framework/service/scheme.py",
    "language": "framework/service/language.py",
    "context": "framework/service/context.py",
    "diagnostic": "framework/service/diagnostic.py",
    "factory": "framework/service/factory.py",
    "test": "framework/service/test.py",
    "telemetry": "framework/service/telemetry.py"
}

exports = ()


# =====================================================================
# --- REGISTRAZIONE NEL CONTAINER ---
# =====================================================================

async def step_inject_and_register(name):
    """Istanzia l'adapter e lo registra nel container dependency-injector."""
    # Note: ctx should be retrieved from somewhere or passed. 
    # In this framework, it's often in thread/task locals or passed in pipe context.
    pass

async def _load_dependencies(module: types.ModuleType, dependencies) -> None:
    """Risolve le dipendenze 'imports' definite in un modulo."""
    
    for key, import_path in dependencies.items():
        if isinstance(import_path, str) and import_path.endswith('.py'):
            if import_path in container.module_cache():
                value = container.module_cache()[import_path]
                setattr(module, key, value)
                continue

        res = await resource(path=import_path)
        value = res.get('data') if isinstance(res, dict) and 'data' in res else res
        setattr(module, key, value)
        container.module_cache()[import_path] = value

async def _register_dependency_in_container(module, path, name, services, payload):
    """Registra la classe/funzione nel container DI (come Factory o Singleton)."""
    obj_class = await flow.get(module, name)
    # Placeholder for more complex logic
    return {"success": True}

@flow.asynchronous()
async def resource(path: str):
    """Carica una risorsa (file, modulo, config)."""
    try:
        content = await flow._load_resource(path=path)
        if path.endswith('.py'):
            import importlib.util
            module_name = f"dynamic_{path.replace('/', '_').replace('.', '_')}"
            module = types.ModuleType(module_name)
            module.__file__ = path
            module.flow = flow
            module.container = container
            exec(content, module.__dict__)
            if hasattr(module, 'imports') and isinstance(module.imports, dict):
                await _load_dependencies(module, module.imports)
            return {"success": True, "data": module}
        return {"success": True, "data": content}
    except Exception as e:
        return {"success": False, "errors": [str(e)]}

@flow.asynchronous()
async def register(payload: dict):
    """Registra un servizio o un manager nel container."""
    path = payload.get('path')
    name = payload.get('service')
    if not path or not name:
        return {"success": False, "errors": ["Missing path or service name"]}
    
    res = await resource(path=path)
    if not res.get('success'):
        return res
    
    module = res.get('data')
    adapter = getattr(module, payload.get('adapter', 'adapter'), None)
    if not adapter:
        adapter = getattr(module, name, None)
    
    if not adapter:
        return {"success": False, "errors": [f"Adapter or class not found in {path}"]}

    if not hasattr(container, name):
        setattr(container, name, providers.Singleton(list))
        
    instance = adapter(config=payload.get('payload', {})) if isinstance(adapter, type) else adapter
    getattr(container, name)().append(instance)
    
    return {"success": True, "data": name}