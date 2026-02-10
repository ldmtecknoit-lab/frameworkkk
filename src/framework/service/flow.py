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

def action(custom_filename: str = __file__, app_context = None, **constants):
    
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
                        'errors': [str(e)],
                        'time': str(end_time - start_time)
                    }
                finally:
                    pass
            return wrapper
        else:
            @functools.wraps(function)
            def wrapper(*args, **kwargs):
                try:
                    return function(*args, **kwargs)
                except Exception as e:
                    return e
                finally:
                    pass
            return wrapper
    return decorator    


def step(fn,*args, **kwargs) -> tuple: return (fn,args,kwargs)


async def act(step, context=dict()):
    

    function, inputs, schemes = step
    nn = []
    gg = {'@':context}
    if hasattr(inputs,'__iter__'):
        for i,x in enumerate(inputs):
            if isinstance(x,str) and x.startswith('@'):
                
                ss = scheme.get(gg,x)
                nn.append(ss)
            else:
                nn.append(inputs[i])
        inputs = tuple(nn)


    start_time = time.perf_counter()
    try:
        
        if asyncio.iscoroutinefunction(function):
            result = await function(*inputs,**schemes|context)
        else:
            result = function(*inputs,**schemes|context)
        end_time = time.perf_counter()

        ok = {
            'action': function.__name__,
            'success': True,
            'inputs': inputs,
            'outputs': result,
            'errors': [],
            'time': str(end_time - start_time)
        }
        return merge_foreach_structure(ok)
    except Exception as e:
        end_time = time.perf_counter()
        return {
            'action': function.__name__,
            'success': False,
            'inputs': inputs,
            'outputs': None,
            'errors': [str(e)],
            #'time': str(end_time - start_time)
        }

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

@action()
async def foreach(data, step, context=dict()):
    outputs = []
    errors = []

    for item in data:
        result = await act(step, context | {'inputs': (item,)})
        outputs.append(result.get('outputs'))
        errors.extend(result.get('errors', []))

    return {
        'outputs': outputs,
        'errors': errors,
        'success': all(e is None for e in errors)
    }

@action()
async def serial(data, action,context=dict()):
    outputs = []
    for item in data:
        output = await act(action, context|{'inputs': (item,)})
        outputs.append(output)
    return aggregate_results(outputs)

@action()
async def parallel(*acts, **options):
    context = options.get('context',dict())
    # Avvia tutte le coroutine insieme
    tasks = [act(action, context) for action in acts]
    results = await asyncio.gather(*tasks)
    
    # Usi la tua funzione aggregate_results per unire tutto
    return aggregate_results(results)

# ------------ Decisione ------------

async def assertt(condition, context=dict()):
    if not eval(condition, context):
        raise AssertionError(f"Assertion failed: {condition}")
    return condition

@action()
async def sentry(condition, context=dict()):
    if not eval(condition,context):
        raise Exception(f"Condition not met: {condition}")
    else:
        return condition

@action()
async def when(condition, step, context=dict()):
    # Se la condizione (funzione o booleano) è vera, esegue lo step
    should_run = await sentry(condition, context)
    if should_run.get('success', False):
        return await act(step, context)
    else:
        return should_run

@action()
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

@action()
async def passs(value=None, context=dict()):
    return value

@action()
async def pipeline(*acts, context={}):
    """
    Esegue una serie di azioni in sequenza, passando l'output 
    di una come input alla successiva.
    """
    last_output = None
    ctx = context if context is not None else {}
    pipeline_results = []

    for i, action in enumerate(acts):
        # Per il primo step usiamo il contesto originale.
        # Per i successivi, l'input è l'output dello step precedente.
        current_ctx = ctx if i == 0 else {**ctx, 'outputs':pipeline_results,'inputs': last_output}
        
        # Eseguiamo l'azione
        result = await act(action, current_ctx)
        pipeline_results.append(result)

        # Se uno step fallisce, fermiamo la pipeline
        if not result.get('success', False):
            #raise Exception(result.get('errors'))
            return result
            
        # Aggiorniamo l'output per il prossimo step
        last_output = result.get('outputs')

    # Usiamo la tua funzione per aggregare la storia della pipeline
    #print(pipeline_results)
    return aggregate_results(pipeline_results)

# ------------ Resilienza ------------

@action()
async def retry(action, *,retries=3, delay=1, context=dict()):
    last_result = None
    for i in range(retries):
        last_result = await act(action, context)
        if last_result.get('success', False):
            return last_result
        if i < retries - 1:
            await asyncio.sleep(delay * (i + 1)) # Backoff lineare
    return last_result

@action()
async def timeout(action, seconds: float, context=None):
    ctx = context if context is not None else {}
    try:
        # Avviamo l'azione con un limite di tempo
        return await asyncio.wait_for(act(action, ctx), timeout=seconds)
    except asyncio.TimeoutError:
        return {
            'action': 'timeout',
            'success': False,
            'errors': [f"Action timed out after {seconds}s"],
            'outputs': None
        }

@action()
def log(*a,**b):
    a = "".join(a)
    c = a.format(**b)
    print(c)
    return c


@action()
async def catch(action, catch_act=passs,context=dict()):
    # action = (action, inputs, options) - fn, inputs, options
    n1 = await act(action, context)
    if n1.get('success',False):
        return n1
    
    recovery = await act(catch_act, context|n1)
    
    # Uniamo gli errori precedenti a quelli nuovi (se presenti)
    all_errors = n1.get('errors', []) + recovery.get('errors', [])
    
    # Restituiamo il risultato del recovery ma con la lista errori completa
    return recovery | {'errors': list(set(all_errors))} # set() opzionale per evitare duplicati