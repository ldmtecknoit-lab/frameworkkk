import asyncio
import importlib
from framework.service import language

class storekeeper():

    def __init__(self,**constants):
        self.providers = constants['providers']

    async def preparation(self, **constants):
        operations = []
        operation = constants.get('operation', 'read')
        repository_name = constants.get('repository', '')

        try:
            repository_module = await language.fetch(path=f"application/repository/{repository_name}.py")
            repository = repository_module.repository()
        except Exception as e:
            language.framework_log("ERROR", f"Errore durante il caricamento del modulo repository '{repository_name}': {e}", emoji="📦")
            return None, []
        
        
        # Recupera i requirements dal contesto
        requirements = language.get_requirements()
        
        # Filtra i provider in base ai requirements
        providers_list = self.providers.get('persistence', [])
        selected_providers = []
        
        if not requirements:
             selected_providers = providers_list
        else:
            for provider in providers_list:
                capabilities = getattr(provider, 'capabilities', {})
                match = True
                for req_key, req_val in requirements.items():
                    cap_val = capabilities.get(req_key)
                    if cap_val != req_val:
                        match = False
                        break
                if match:
                    selected_providers.append(provider)
            
            # Se nessun provider soddisfa i requisiti, fallback a tutti o logica specifica?
            # Per ora fallback a tutti se vuoto, o meglio loggare e non fare nulla?
            # Se l'utente chiede 'low latency' e nessuno lo ha, forse non vuole 'high latency'.
            # Manteniamo vuoto se non trovato, ma logghiamo.
            if not selected_providers:
                 language.framework_log("WARNING", f"Nessun provider soddisfa i requisiti: {requirements}", emoji="🚫")
        
        for provider in selected_providers:
            try:
                profile = provider.config.get('profile', '').upper()
                if not profile:
                    language.framework_log("WARNING", f"Provider {provider} non ha un profilo configurato.", emoji="⚠️")
                    continue

                if profile in repository.location:
                    try:
                        task_args = await repository.parameters(operation, profile, **constants)
                    except Exception as e:
                        language.framework_log("ERROR", f"Errore durante l'ottenimento dei parametri per {profile}: {e}", emoji="❌")
                        continue

                    # Controllo che il metodo esista nel provider
                    method = getattr(provider, operation, None)
                    if not callable(method):
                        language.framework_log("WARNING", f"Il metodo '{operation}' non è disponibile per il provider {profile}.", emoji="🚫")
                        continue

                    task = asyncio.create_task(method(**task_args), name=profile)
                    task.parameters = task_args
                    operations.append(task)
                else:
                    language.framework_log("DEBUG", f"Provider {provider} non ha un profilo trovato.", emoji="🔍")
            except Exception as e:
                language.framework_log("ERROR", f"Errore imprevisto durante la preparazione per il provider {provider}: {e}", emoji="🤯")
        return repository, operations
    
    # overview/view/get
    @language.asynchronous(inputs='storekeeper',outputs='transaction',managers=('executor',))
    async def overview(self, executor, **constants):
        repository,operations = await self.preparation(**constants|{'operation':'view'})
        return await executor.first_completed(operations=operations,success=repository.results)

    # gather/read/get
    @language.asynchronous(inputs='storekeeper',outputs='transaction',managers=('executor',))
    async def gather(self, executor, **constants):
        repository,operations = await self.preparation(**constants|{'operation':'read'})
        return await executor.first_completed(operations=operations,success=repository.results)
    
    # store/create/put
    @language.asynchronous(inputs='storekeeper',outputs='transaction',managers=('executor',))
    async def store(self, executor, **constants):
        repository,operations = await self.preparation(**constants|{'operation':'create'})
        return await executor.first_completed(operations=operations,success=repository.results)
    
    # remove/delete/delete
    @language.asynchronous(inputs='storekeeper',outputs='transaction',managers=('executor',))
    async def remove(self, executor, **constants):
        repository,operations = await self.preparation(**constants|{'operation':'delete'})
        return await executor.first_completed(operations=operations,success=repository.results)
    
    # change/update/patch
    @language.asynchronous(inputs='storekeeper',outputs='transaction',managers=('executor',))
    async def change(self,executor,**constants):
        repository,operations = await self.preparation(**constants|{'operation':'update'})
        return await executor.first_completed(operations=operations,success=repository.results)