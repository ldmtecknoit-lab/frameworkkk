import asyncio
from typing import List, Dict, Any, Callable
import re
import traceback
from framework.service.diagnostic import framework_log

class executor:
    def __init__(self, **constants):
        # actuator
        self.sessions: Dict[str, Any] = {}
        providers_data = constants.get('providers', [])
        if isinstance(providers_data, dict):
            self.providers = providers_data.get('actuator', [])
        else:
            self.providers = providers_data
    
    @flow.asynchronous(managers=('messenger',))
    async def action(self, messenger, **constants):
        #await asyncio.sleep(5)
        
        # Recupera i requirements dal contesto
        requirements = language.get_requirements()
        
        # Seleziona il provider migliore
        provider = self._select_provider(requirements)
        
        if not provider:
            # Fallback all'ultimo provider o gestisci errore
            provider = self.providers[-1] if self.providers else None
            
        if provider:
            await provider.actuate(**constants)
        else:
            await messenger.post(domain='error', message="Nessun provider disponibile per l'azione.")

    def _select_provider(self, requirements: Dict[str, Any]) -> Any:
        """Seleziona il provider che meglio soddisfa i requirements."""
        if not self.providers:
            return None
            
        if not requirements:
            return self.providers[-1] # Default behavior (last one) or first? Original code used -1.
            
        best_provider = None
        best_score = -1
        
        for provider in self.providers:
            score = 0
            capabilities = getattr(provider, 'capabilities', {})
            
            # Calcola score basato su requirements e capabilities
            # Esempio semplice: +1 per ogni match esatto
            match = True
            for req_key, req_val in requirements.items():
                cap_val = capabilities.get(req_key)
                if cap_val != req_val:
                    match = False
                    break
            
            if match:
                # Se tutti i requirements sono soddisfatti, questo è un candidato.
                # Potremmo avere logiche più complesse di scoring.
                return provider
                
        # Se nessun match esatto, ritorna l'ultimo (fallback) o None?
        # Per ora fallback all'ultimo come comportamento di default
        return self.providers[-1]

    @flow.asynchronous(managers=('messenger',))
    async def act(self, messenger, **constants) -> Dict[str, Any]:
        """
        Esegue una o più azioni (separate da '|') caricando dinamicamente i moduli corrispondenti.
        Supporta sia chiamate con parametri (es: create.note(param=1)) sia solo nome funzione (es: create.note).
        """
        value = constants.get('action', '') or constants.get('action', '')
        functions = value.split('|')
        lista = []

        for func in functions:
            func = func.strip()
            result = {}
            match = re.match(r"(\w+(?:\.\w+)*)(?:\((.*)\))?", func)
            if not match:
                continue
            key = match.group(1)
            params_str = match.group(2)
            if params_str:
                params = language.extract_params(f"{key}({params_str})")
            else:
                params = constants
            result[key] = params
            lista.append(result)

        results = []
        for n in lista:
            for name in n:
                await messenger.post(domain='debug', message=f"🔄 Caricamento dell'azione: {name}")
                parts = name.split('.')
                module_path = f"application.action.{parts[0]}"
                adapter = parts[0]
                func_name = parts[1] if len(parts) > 1 else parts[0]
                module = await language.load_module(
                    language,
                    path=module_path,
                    area='application',
                    service='action',
                    adapter=adapter
                )
                act_func = getattr(module, func_name)
                res = await act_func(**n[name])
                results.append({name: res})
                await messenger.post(domain='debug', message=f"✅ Azione '{name}' eseguita con successo.")

        return {"state": True, "result": results, "error": None}

    @flow.asynchronous(managers=('messenger',))
    async def first_completed(self, messenger, **constants):
        """Attende il primo task completato e restituisce il suo risultato."""
        operations = constants.get('operations', [])
        await messenger.post(domain='debug',message="⏳ Attesa della prima operazione completata...")

        while operations:
            finished, unfinished = await asyncio.wait(operations, return_when=asyncio.FIRST_COMPLETED)

            for operation in finished:
                transaction = operation.result()
                if transaction:
                    # framework_log("DEBUG", f"Transazione completata: {type(transaction)}", emoji="💼")
                    if 'success' in constants:
                        transaction = await constants['success'](transaction=transaction,profile=operation.get_name())
                    
                    # Ensure transaction is a dict to attach parameters
                    if isinstance(transaction, list):
                        transaction = {"success": True, "data": transaction, "errors": []}
                    
                    if isinstance(transaction, dict):
                        framework_log("DEBUG", f"✅ Executor: transazione valida trovata per {operation.get_name()}")
                        for task in unfinished:
                            task.cancel()
                        transaction.setdefault('parameters', getattr(operation, 'parameters', {}))
                        return transaction
                    
                    for task in unfinished:
                        task.cancel()
                    return {"success": True, "data": transaction, "errors": []}

                operations = unfinished

            error_msg = "⚠️ Nessuna transazione valida completata"
            await messenger.post(domain='debug',message=error_msg)
            return None

    @flow.asynchronous(managers=('messenger',))
    async def all_completed(self, messenger, **constants) -> Dict[str, Any]:
        tasks: List[asyncio.Future] = constants.get('tasks', [])
    
        # Lista per raccogliere i dettagli degli errori da ogni task
        detailed_errors = []
        
        # return_exceptions=True: le eccezioni sono restituite come risultati
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 1. Analisi dei Risultati Dettagliata
        for result in results:
            if isinstance(result, Exception):
                
                # Questa funzione stampa il traceback completo sul tuo log/console
                traceback.print_exception(type(result), result, result.__traceback__)
                
                # Un task è fallito. Registra il traceback completo.
                
                # Ottieni il traceback completo (come stringa)
                error_trace = traceback.format_exception(type(result), result, result.__traceback__)
                full_error_log = "".join(error_trace)
                
                # Aggiungi il dettaglio all'elenco degli errori
                detailed_errors.append(full_error_log)

        
        # Se ci sono errori dettagliati, il risultato complessivo è un fallimento logico
        if any(result.get('success', False) is not True for result in results):
            return {"success": False, "results": results, "errors": detailed_errors}
        
        return {"success": True, "results": results}

    @flow.asynchronous(managers=('messenger',))
    async def chain_completed(self, messenger, **constants) -> Dict[str, Any]:
        """Esegue i task in sequenza, aspettando il completamento di ciascuno prima di passare al successivo."""
        tasks = constants.get('tasks', [])
        results = []

        await messenger.post(domain='debug',message="🔄 Avvio esecuzione sequenziale delle operazioni...")

        try:
            for task in tasks:
                try:
                    result = await task(**constants)
                    results.append(result)
                    await messenger.post(domain='debug', message=f"✅ Task completato: {result}")
                except Exception as e:
                    await messenger.post(domain='debug', message=f"❌ Errore nel task {task}: {e}")

            return {"state": True, "result": results, "error": None}

        except Exception as e:
            error_msg = f"❌ Errore in chain_completed: {str(e)}"
            await messenger.post(domain='debug', message=error_msg)
            return {"state": False, "result": None, "error": error_msg}

    @flow.asynchronous(managers=('messenger',))
    async def together_completed(self, messenger, **constants) -> Dict[str, Any]:
        """Esegue tutti i task contemporaneamente senza attendere il completamento di tutti."""
        tasks = constants.get('tasks', [])

        await messenger.post(domain='debug', message="🚀 Avvio esecuzione simultanea delle operazioni...")

        try:
            for task in tasks:
                asyncio.create_task(task)

            await messenger.post(domain='debug', message="✅ Tutti i task sono stati avviati in background.")
            return {"state": True, "result": "Tasks avviati in background", "error": None}

        except Exception as e:
            error_msg = f"❌ Errore in together_completed: {str(e)}"
            await messenger.post(domain='debug', message=error_msg)
            return {"state": False, "result": None, "error": error_msg}