import framework.service.load as load
from framework.service.context import container
import framework.service.flow as flow
import framework.service.language as language


class loader:
    """
    Manager that handles resource loading and bootstrapping by delegating to the load service.
    Utilizza self.dependencies per orchestrare il caricamento dello strato di servizi core.
    """
    def __init__(self, **constants):
        self.config = constants
        self.resources = {}
        self.services = {}
        # Definizione delle dipendenze dello strato di servizi core
        self.dependencies = {
            "context": [],
            "flow": ["context"],
            "scheme": ["flow"],
            "language": ["flow", "scheme"],
            "load": ["flow", "scheme", "language", "context"],
            "test": ["flow"],
            "factory": ["flow"],
            "diagnostic": ["flow"]
        }

    def _get_load_order(self):
        """Calcola l'ordine di caricamento tramite ordinamento topologico (DFS)."""
        order = []
        visited = set()
        stack = set()

        def visit(node):
            if node in stack:
                raise RuntimeError(f"Ciclo di dipendenze rilevato: {node}")
            if node not in visited:
                stack.add(node)
                for dep in self.dependencies.get(node, []):
                    visit(dep)
                stack.remove(node)
                visited.add(node)
                order.append(node)

        for node in list(self.dependencies.keys()):
            visit(node)
        return order

    async def _initialize_services(self):
        """Carica e inietta le dipendenze nei servizi core nell'ordine corretto."""
        order = self._get_load_order()
        
        for name in order:
            path = f"framework/service/{name}.py"
            # Carichiamo il servizio via load.resource per abilitare proxy e transazioni
            res = await load.resource(path=path)
            
            # Se res è un errore esplicito, logghiamo e proseguiamo (o interrompiamo se critico)
            if isinstance(res, dict) and res.get('success') is False:
                print(f"[ERROR] Impossibile caricare il servizio core '{name}': {res.get('errors')}")
                continue

            service_module = res.get('data', res) if isinstance(res, dict) else res
            
            # Iniezione delle dipendenze dichiarate per questo servizio
            for dep_name in self.dependencies.get(name, []):
                if dep_name in self.services:
                    # Iniezione diretta sul modulo caricato
                    setattr(service_module, dep_name, self.services[dep_name])
                    # print(f"[DEBUG] Iniettato {dep_name} in {name}")
            
            self.services[name] = service_module
            self.resources[path] = service_module
            
        return self.services

    async def resource(self, **kwargs):
        """
        Carica una risorsa delegando al servizio load (iniettato o globale).
        """
        load_service = self.services.get('load', load)
        
        path = kwargs.get('path')
        if not path:
            return {"success": False, "errors": ["Missing path"]}

        if path in self.resources:
            return {"success": True, "data": self.resources[path]}
        
        res = await load_service.resource(**kwargs)
        
        if isinstance(res, dict) and res.get('success') is False:
            return res
            
        data = res.get('data', res) if isinstance(res, dict) else res
        self.resources[path] = data
        return {"success": True, "data": data}

    async def bootstrap(self):
        """
        Bootstraps il framework inizializzando prima i servizi e poi eseguendo bootstrap.dsl.
        """
        # 1. Inizializzazione servizi core ordinata
        await self._initialize_services()
        
        # 2. Caricamento ed esecuzione del bootstrap applicativo
        print("[INFO] Avvio Bootstrap dello strato applicativo (DSL)...")
        
        res = await self.resource(path="framework/service/bootstrap.dsl")
        
        if res.get('success'):
            dsl_content = res.get('data')
            # Usiamo il servizio language iniettato se disponibile
            lang_service = self.services.get('language')
            if lang_service and hasattr(lang_service, 'execute_dsl_file'):
                return await lang_service.execute_dsl_file(dsl_content)
            else:
                return await language.execute_dsl_file(dsl_content)
        
        return res

    async def register(self, **kwargs):
        """Registra un servizio o manager nel Dependency Injection container."""
        return await flow.pipe(
            step_load_service_module,
            step_validate_registration,
            step_inject_and_register,
            context=config
        )