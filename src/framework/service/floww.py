import uuid
import asyncio
import functools
import inspect
import time
from typing import Any, Callable, Dict, List, Optional
from framework.service.context import container
from framework.service.diagnostic import framework_log, log_block, _load_resource, buffered_log, analyze_exception, _get_system_info
import framework.service.scheme as scheme

def merge_foreach_structure(data):
    # Verifichiamo se l'output contiene la struttura del figlio
    child = data.get('outputs')
    
    if isinstance(child, dict) and 'outputs' in child:
        # 1. 'outputs' del padre diventa la lista piatta dei risultati del figlio
        new_outputs = child.get('outputs', [])
        
        # 2. 'errors' del padre riceve anche gli errori del figlio (appiattiti)
        new_errors = data.get('errors', []) + child.get('errors', [])
        
        # 3. 'success' è True solo se entrambi sono True
        new_success = data.get('success', False) and child.get('success', False)
        
        # Aggiorniamo il dizionario originale mantenendo le chiavi intatte
        data['success'] = new_success
        data['outputs'] = new_outputs
        data['errors'] = new_errors
        
    return data

def flow(custom_filename: str = __file__, app_context = None, **constants):
    
    def decorator(function):
        if asyncio.iscoroutinefunction(function):
            @functools.wraps(function)
            async def wrapper(*args, **kwargs):
                start_time = time.perf_counter()
                try:
                    result = await function(*args, **kwargs)
                    end_time = time.perf_counter()
                    ok = {
                        'action': function.__name__,
                        'success': True,
                        'inputs': args,
                        'outputs': result,
                        'errors': [],
                        'time': str(end_time - start_time)
                    }
                    #print(result,"<-----------result")
                    return merge_foreach_structure(ok)
                except Exception as e:
                    end_time = time.perf_counter()
                    return {
                        'action': function.__name__,
                        'success': False,
                        'inputs': args,
                        'outputs': None,
                        'errors': [flow.__name__+":"+str(e)],
                        'time': str(end_time - start_time)
                    }
                finally:
                    pass
            return wrapper
        else:
            @functools.wraps(function)
            def wrapper(*args, **kwargs):
                try:
                    return action(function, args, kwargs)
                except Exception as e:
                    return e
                finally:
                    pass
            return wrapper
    return decorator    

@flow()
async def action(action, context=dict()) -> Any:
    if callable(action):
        action = (action,context.get('inputs',tuple()),context.get('options',dict()))
    fn, inputs, options = action
    #print(f"Action {type(fn)}:{fn} {type(inputs)}:{inputs} dict/{type(options)}:{options}")
    # It's a Python callable
    print(f"Action {type(fn)}:{fn}")
    if asyncio.iscoroutinefunction(fn):
        return await fn(*inputs)
    else:
        return fn(*inputs)

from collections import defaultdict

def aggregate_results(dict_list):
    aggregated = defaultdict(list)
    
    for entry in dict_list:
        for key, value in entry.items():
            aggregated[key].append(value)
            
    # Opzionale: Pulizia dei dati (es. se 'action' è sempre uguale, prendi solo il primo)
    final_data = dict(aggregated)
    
    # Esempio di post-elaborazione: 
    # Trasforma in valore singolo se tutti gli elementi sono identici (come 'action')
    for key in final_data:
        if all(x == final_data[key][0] for x in final_data[key]):
            final_data[key] = final_data[key][0]
            
    return final_data

# ------------ Iterazione ------------

@flow()
async def serial(data, act,context=dict()):
    outputs = []
    for item in data:
        output = await action(act, context|{'inputs': (item,)})
        outputs.append(output)
    return aggregate_results(outputs)

@flow()
async def parallel(*acts, **options):
    context = options.get('context',dict())
    # Avvia tutte le coroutine insieme
    tasks = [action(act, context) for act in acts]
    results = await asyncio.gather(*tasks)
    
    # Usi la tua funzione aggregate_results per unire tutto
    return aggregate_results(results)

# ------------ Decisione ------------

@flow()
async def sentry(condition, context=dict()):
    if not eval(condition,context):
        raise Exception(f"Condition not met: {condition}")
    else:
        return condition

@flow()
async def when(condition, step, context=dict()):
    # Se la condizione (funzione o booleano) è vera, esegue lo step
    should_run = await sentry(condition, context)
    if should_run.get('success', False):
        return await action(step, context)
    else:
        return should_run

@flow()
async def switch(cases: dict, context=None):
    """
    Seleziona ed esegue uno step tra molti in base al risultato di condition_fn.
    cases = {'valore1': step1, 'valore2': step2, 'default': step_default}
    """

    for case in cases:
        if case.lower() == 'true':
            continue
        pas = await when(case, cases[case], context)
        if pas.get('success', False):
            return pas
    
    return await when('true', cases['true'], context)

# ------------ Sequenza ------------

@flow()
async def pipeline(*acts, context=None):
    """
    Esegue una serie di azioni in sequenza, passando l'output 
    di una come input alla successiva.
    """
    last_output = None
    ctx = context if context is not None else {}
    pipeline_results = []

    for i, act in enumerate(acts):
        # Per il primo step usiamo il contesto originale.
        # Per i successivi, l'input è l'output dello step precedente.
        current_ctx = ctx if i == 0 else {**ctx, 'inputs': (last_output,)}
        
        # Eseguiamo l'azione
        result = await action(act, current_ctx)
        pipeline_results.append(result)

        # Se uno step fallisce, fermiamo la pipeline
        if not result.get('success', False):
            break
            
        # Aggiorniamo l'output per il prossimo step
        last_output = result.get('outputs')

    # Usiamo la tua funzione per aggregare la storia della pipeline
    return aggregate_results(pipeline_results)

# ------------ Resilienza ------------

@flow()
async def retry(act, *,retries=3, delay=1, context=dict()):
    last_result = None
    for i in range(retries):
        last_result = await action(act, context)
        if last_result.get('success', False):
            return last_result
        if i < retries - 1:
            await asyncio.sleep(delay * (i + 1)) # Backoff lineare
    return last_result

@flow()
async def timeout(act, seconds: float, context=None):
    ctx = context if context is not None else {}
    try:
        # Avviamo l'azione con un limite di tempo
        return await asyncio.wait_for(action(act, ctx), timeout=seconds)
    except asyncio.TimeoutError:
        return {
            'action': 'timeout',
            'success': False,
            'errors': [f"Action timed out after {seconds}s"],
            'outputs': None
        }

@flow()
async def catch(act, catch_act,context=dict()):
    # action = (action, inputs, options) - fn, inputs, options
    n1 = await action(act, context)
    if n1.get('success',False):
        return n1
    
    recovery = await action(catch_step, context)
    
    # Uniamo gli errori precedenti a quelli nuovi (se presenti)
    all_errors = n1.get('errors', []) + recovery.get('errors', [])
    
    # Restituiamo il risultato del recovery ma con la lista errori completa
    return recovery | {'errors': list(set(all_errors))} # set() opzionale per evitare duplicati