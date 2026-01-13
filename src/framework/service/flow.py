import uuid
import sys
import asyncio
import functools
import inspect
import json
import time
from typing import Any, Callable, Dict, List, Optional
from framework.service.context import container
from framework.service.inspector import framework_log, log_block, _load_resource, buffered_log, analyze_exception, _get_system_info
from framework.service.scheme import convert, get, format, normalize, transform, put, route, mappa
from framework.service.telemetry import (
    MultiSpanContext, MockSpanContext, 
    get_transaction_id, set_transaction_id, get_requirements,
    _transaction_id, _requirements, _setup_transaction_context
)

async def _execute_step_internal(action_step, context=dict()) -> Any:
    """
    Esegue un'azione (funzione, args, kwargs) fornita da 'step', 
    senza il contesto completo del pipe.
    """
        
    # Handle literal values (strings, numbers, etc.) - just return them
    if not callable(action_step) and not isinstance(action_step, tuple):
        return {"success": True, "data": action_step, "errors": []}
    
    if callable(action_step):
        action_step = (action_step, (), {})

    fun = action_step[0]
    args = action_step[1] if len(action_step) > 1 else ()
    kwargs = action_step[2] if len(action_step) > 2 else {}
    if isinstance(fun, str):
        #Funzione da stringa (context lookup)
        fun = get({'@':context}, fun)
    
    aaa = []
    for arg in args:
        if isinstance(arg, str) and arg.strip().startswith("@"):
            aaa.append(get({'@':context}, arg))
        else:
            aaa.append(arg)
    args = tuple(aaa)

    kkk = {}
    for k, v in kwargs.items():
        if isinstance(v, str) and v.strip().startswith("@"):
            kkk[k] = get({'@':context}, v)
        else:
            kkk[k] = v
    kwargs = kkk

    if not isinstance(action_step, tuple) or len(action_step) < 2 or not callable(fun):
        step_repr = str(action_step)[:100]
        raise TypeError(f"L'azione fornita non è un formato step valido. Action: {step_repr}", fun, args, kwargs)

    if asyncio.iscoroutinefunction(fun):
        # Inspect the function to see if it accepts 'context'
        sig = inspect.signature(fun)
        if 'context' in sig.parameters:
            kwargs['context'] = context
        result = await fun(*args, **kwargs)
    else:
        # Inspect the function to see if it accepts 'context'
        sig = inspect.signature(fun)
        if 'context' in sig.parameters:
            kwargs['context'] = context
        result = fun(*args, **kwargs)
        if asyncio.iscoroutine(result):
            result = await result

    # Auto-wrapping in Transaction se non lo è già
    if isinstance(result, dict) and 'success' in result and ('data' in result or 'errors' in result):
        return result
    
    return {"success": True, "data": result, "errors": []}

def _prepare_action_context(custom_filename, **constants):
    """Prepara requirements, manager_names e schema path per il decoratore."""
    known_params = {'managers', 'outputs', 'inputs'}
    requirements = {k: v for k, v in constants.items() if k not in known_params}
    
    manager_names = list(constants.get('managers', []))
    
    output_schema_path = 'framework/scheme/transaction.json'
    if 'outputs' in constants and constants['outputs']:
         output_schema_path = constants['outputs']
         
    return requirements, manager_names, output_schema_path

def _execute_wrapper_sync(function, args, kwargs, manager_names, current_tx_id):
    """Esegue la funzione wrappata (sincrona) e arricchisce il risultato."""
    inject = []
    for m in manager_names:
        if hasattr(container, m):
            inject.append(getattr(container, m)())
        else:
            framework_log("WARNING", f"Manager '{m}' richiesto da {function.__name__} non trovato.")
            
    args_inject = list(args) + inject
    
    # Esecuzione diretta
    try:
        # Nota: qui non usiamo _execute_step_internal perché è async, 
        # ma ne replichiamo la logica di iniezione contesto se necessario
        sig = inspect.signature(function)
        if 'context' in sig.parameters and 'context' not in kwargs:
             kwargs['context'] = {}
             
        result = function(*args_inject, **kwargs)
        
        # Auto-wrapping
        if isinstance(result, dict) and 'success' in result and ('data' in result or 'errors' in result):
            transaction = result
        else:
            transaction = {"success": True, "data": result, "errors": []}
    except Exception as e:
        raise e

    transaction['identifier'] = current_tx_id
    return transaction

def _normalize_wrapper_sync(transaction, output_schema_path, wrapper_func, kwargs, current_tx_id):
    """Gestisce la normalizzazione sincrona (limitata o via helper)."""
    # Se lo schema è un path, in sincrono è complesso caricarlo async. 
    # Per ora gestiamo solo se è già un dict o lo saltiamo nel wrapper sincrono se non strettamente necessario.
    # In una versione più avanzata, potremmo pre-caricare gli schemi al bootstrap.
    if isinstance(output_schema_path, dict):
        # Implementazione ipotetica di normalize_sync
        pass
    
    return transaction

async def _execute_wrapper(function, args, kwargs, manager_names, current_tx_id):
    """Esegue la funzione wrappata e arricchisce il risultato."""
    # Resolve managers at runtime. Wait if they are missing
    inject = []
    for m in manager_names:
        attempts = 0
        while not hasattr(container, m) and attempts < 10:
            await asyncio.sleep(0.1)
            attempts += 1
        
        if hasattr(container, m):
            inject.append(getattr(container, m)())
        else:
            framework_log("WARNING", f"Manager '{m}' richiesto da {function.__name__} non trovato nel container.")
            
    args_inject = list(args) + inject
    step_tuple = (function, tuple(args_inject), kwargs)
    
    transaction = await _execute_step_internal(step_tuple)
    
    transaction['identifier'] = current_tx_id
    try:
        sys_info = _get_system_info()
        transaction['worker'] = f"{sys_info.get('hostname', 'unknown')}:{sys_info.get('process_id', '?')}"
    except Exception:
        pass
        
    return transaction

async def _normalize_wrapper(transaction, output_schema_path, wrapper_func, kwargs, current_tx_id):
    """Gestisce il caricamento dello schema e la normalizzazione."""
    target_schema = output_schema_path
    
    if isinstance(target_schema, str):
        try:
            schema_content = await _load_resource(path=target_schema)
            target_schema = json.loads(schema_content)
        except Exception as e:
            buffered_log("ERROR", f"Errore caricamento schema da {output_schema_path}: {e}")
            target_schema = None

    if target_schema and isinstance(target_schema, dict):
        try:
            meta = {
                "action": wrapper_func.__name__,
                "parameters": kwargs,
                "identifier": current_tx_id,
                "worker": transaction.get('worker', 'unknown')
            }
            return await normalize(meta | transaction, target_schema)
        except Exception as e:
            buffered_log("ERROR", f"Errore normalizzazione output in {wrapper_func.__name__}: {e}")
            return transaction
    
    return transaction

def _handle_wrapper_error(e, function, custom_filename, current_tx_id):
    """Gestisce le eccezioni e genera il report di errore."""
    error_details = str(e)
    try:
        source = inspect.getsource(function) if hasattr(function, '__code__') else ""
        report = analyze_exception(source, custom_filename)
        if report and 'EXCEPTION_DETAILS' in report:
            error_details = report['EXCEPTION_DETAILS']
    except Exception:
        pass 

    if not hasattr(container, 'messenger'):
        framework_log("ERROR", f"Eccezione in {function.__name__}: {e}", emoji="❌", exception=e)

    return {
        "success": False, 
        "errors": [error_details],
        "data": None,
        "action": function.__name__,
        "identifier": current_tx_id
    }

def action(custom_filename: str = __file__, app_context = None, **constants):
    """
    Decoratore polimorfico che fonde logica sincrona e asincrona.
    Gestisce iniezione dipendenze, telemetria, contratti e transazionalità.
    """
    requirements, manager_names, output_schema_path = _prepare_action_context(custom_filename, **constants)

    def decorator(function):
        if asyncio.iscoroutinefunction(function):
            @functools.wraps(function)
            async def wrapper(*args, **kwargs):
                wrapper._is_decorated = True
                current_tx_id, tx_token = _setup_transaction_context()
                req_token = _requirements.set(requirements)
                try:
                    telemetry_list = getattr(container, 'telemetry', lambda: [])()
                    span_name = f"async:{function.__name__}"
                    with MultiSpanContext(telemetry_list, span_name):
                        transaction = await _execute_wrapper(function, args, kwargs, manager_names, current_tx_id)
                        return await _normalize_wrapper(transaction, output_schema_path, wrapper, kwargs, current_tx_id)
                except Exception as e:
                    return _handle_wrapper_error(e, function, custom_filename, current_tx_id)
                finally:
                    _requirements.reset(req_token)
                    if tx_token: _transaction_id.reset(tx_token)
            return wrapper
        else:
            @functools.wraps(function)
            def wrapper(*args, **kwargs):
                wrapper._is_decorated = True
                current_tx_id, tx_token = _setup_transaction_context()
                req_token = _requirements.set(requirements)
                try:
                    telemetry_list = getattr(container, 'telemetry', lambda: [])()
                    span_name = f"sync:{function.__name__}"
                    with MultiSpanContext(telemetry_list, span_name):
                        transaction = _execute_wrapper_sync(function, args, kwargs, manager_names, current_tx_id)
                        # Normalizzazione sincrona (limitata se richiede I/O async)
                        return transaction
                except Exception as e:
                    return _handle_wrapper_error(e, function, custom_filename, current_tx_id)
                finally:
                    _requirements.reset(req_token)
                    if tx_token: _transaction_id.reset(tx_token)
            return wrapper
    return decorator

# Alias per compatibilità con il codice esistente
asynchronous = action
synchronous = action

def step(func, *args, **kwargs):
    return (func, args, kwargs)

async def pipe(*stages, context=dict()):
    """
    Orchestra un flusso dichiarativo, chiamando le funzioni in sequenza.
    """
    context |= {'outputs': []}
    stage_index = 0
    final_output = None
    
    with log_block(f"Pipe with {len(stages)} stages", level="TRACE", emoji="🚀"):
        # --- OpenTelemetry Hook ---
        telemetry_list = getattr(container, 'telemetry', lambda: [])()
        
        with MultiSpanContext(telemetry_list, "pipe_execution"):
            for stage_tuple in stages:
                stage_index += 1
                step_name = getattr(stage_tuple[0], '__name__', str(stage_tuple[0]))
                
                with log_block(f"Step {stage_index}: {step_name}", level="TRACE", emoji="👣"):
                    outcome = await _execute_step_internal(stage_tuple, context)
                
                if isinstance(outcome, dict) and outcome.get('success') is True and 'data' in outcome:
                    data_to_pass = outcome['data']
                else:
                    data_to_pass = outcome
                
                final_output = data_to_pass
                context['outputs'].append(data_to_pass)
            
    return final_output

async def safe(func: Callable, *args, **kwargs) -> Dict[str, Any]:
    try:
        if asyncio.iscoroutinefunction(func):
            data = await func(*args, **kwargs)
        else:
            data = func(*args, **kwargs)
            
        if isinstance(data, dict) and 'success' in data and 'data' in data:
            return data
            
        return {"success": True, "data": data, "errors": []}
    except Exception as e:
        return {
            "success": False, 
            "data": None, 
            "errors": [{"type": type(e).__name__, "message": str(e)}]
        }

def transactional(func):
    """
    Assicura che il risultato di una funzione sia sempre conforme a transaction.json.
    """
    if not callable(func) or getattr(func, '_is_transactional', False):
        return func

    if asyncio.iscoroutinefunction(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                result = await func(*args, **kwargs)
                if isinstance(result, dict) and 'success' in result and ('data' in result or 'errors' in result):
                    return result
                return {"success": True, "data": result, "errors": []}
            except Exception as e:
                return {"success": False, "data": None, "errors": [str(e)]}
        wrapper._is_transactional = True
        return wrapper
    else:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                result = func(*args, **kwargs)
                if inspect.isawaitable(result):
                    async def await_result():
                        try:
                            res = await result
                            if isinstance(res, dict) and 'success' in res and ('data' in res or 'errors' in res):
                                return res
                            return {"success": True, "data": res, "errors": []}
                        except Exception as e:
                            return {"success": False, "data": None, "errors": [str(e)]}
                    return await_result()
                
                if isinstance(result, dict) and 'success' in result and ('data' in result or 'errors' in result):
                    return result
                return {"success": True, "data": result, "errors": []}
            except Exception as e:
                return {"success": False, "data": None, "errors": [str(e)]}
        
        wrapper._is_transactional = True
        return wrapper

async def branch(on_success: Callable, on_failure: Callable, context=dict()):
    """
    Instrada il flusso basandosi sul campo 'ok' del risultato (result.json).
    """
    outcome = context.get('outputs', [None])[-1]
    
    with log_block("Branching", level="TRACE", emoji="📂"):
        if isinstance(outcome, dict) and outcome.get('success') is True:
            return await _execute_step_internal(on_success, context)
        else:
            return await _execute_step_internal(on_failure, context)

async def guard(condition: str, context=dict()) -> Optional[Dict[str, Any]]:
    import mistql
    
    try:
        safe_context = context
        if isinstance(context, dict):
            safe_context = context.copy()
            if 'outputs' in safe_context:
                safe_outputs = []
                for out in safe_context['outputs']:
                    if isinstance(out, (dict, list, str, int, float, bool, type(None))):
                        safe_outputs.append(out)
                    else:
                        safe_outputs.append(str(out))
                safe_context['outputs'] = safe_outputs
            
            for k, v in safe_context.items():
                if not isinstance(v, (dict, list, str, int, float, bool, type(None))):
                    safe_context[k] = str(v)

        if type(context) not in [str, int, float, bool,dict,list]:
            safe_context = str(context)
            
        wrapped_context = {'@': safe_context}
        result = mistql.query(condition, wrapped_context)
        
        if result:
            return {
                "success": True, 
                "data": context, 
                "errors": []
            }
        else:
            return {
                "success": False, 
                "data": context, 
                "errors": [{
                    "condition": condition,
                    "evaluated_result": result,
                    "context": safe_context
                }]
            }
    except Exception as e:
        return {
            "success": False,
            "data": None,
            "errors": [{
                "message": f"Errore nella valutazione MistQL: {str(e)}",
                "condition": condition,
                "exception": type(e).__name__
            }]
        }

async def fallback(primary_func, secondary_func, context=dict()) -> Dict[str, Any]:
    transaction = await _execute_step_internal(primary_func,context)
    if transaction['success']:
        return transaction
    transaction = await _execute_step_internal(secondary_func,context)  
    return transaction

async def switch(cases, context=dict()):
    case_list = []
    if isinstance(cases, dict):
        case_list = list(cases.items())
    else:
        case_list = cases

    for condition, action_step in case_list:
        guard_result = await guard(condition, context)
        success = guard_result.get("success", False)
        if success:
            return await _execute_step_internal(action_step,context)

async def work(workflow, context=dict()):
    current_tx_id, tx_token = _setup_transaction_context()
    if context is None:
        context = {}
    if 'identifier' not in context:
        context['identifier'] = current_tx_id

    try:
        authorized = False
        defender_service = None
        if hasattr(container, 'defender'):
            try:
                defender_service = container.defender()
            except Exception:
                defender_service = None
        
        if defender_service:
            wf_name = getattr(workflow, '__name__', str(workflow))
            check_ctx = context | {'workflow_name': wf_name, 'transaction_id': current_tx_id}
            authorized = await defender_service.check_permission(**check_ctx)
            if not authorized:
                framework_log("WARNING", f"Accesso negato da Defender per {wf_name}", emoji="⛔", data=check_ctx)
        else:
            is_system = context.get('system', False) or context.get('user') == 'system'
            wf_name = getattr(workflow, '__name__', str(workflow))
            if is_system or 'bootstrap' in wf_name:
                authorized = True
                framework_log("DEBUG", f"Defender offline: Accesso System concesso per {wf_name}.", emoji="🛡️")
            else:
                authorized = False
                framework_log("ERROR", f"Defender offline: Accesso User negato per {wf_name}.", emoji="⛔")

        if not authorized:
             raise PermissionError("Accesso negato: Permessi insufficienti o Defender non disponibile.")
        
        return await _execute_step_internal(workflow, context)

    except Exception as e:
        framework_log("ERROR", f"Errore avvio workflow: {e}", emoji="❌")
        raise
    finally:
        if tx_token:
            _transaction_id.reset(tx_token)

async def catch(try_step, catch_step,context=dict()):
    try:
        outcome = await _execute_step_internal(try_step,context)
    except Exception as e:
        outcome = {'success': False, 'errors': [str(e)]}
    framework_log("WARNING", f"Eccezione catturata da catch: {outcome}", emoji="🪝")
    if isinstance(outcome, dict) and outcome.get('success') is False:
        framework_log("WARNING", f"Fallimento nello step. Esecuzione del fallback: {outcome.get('errors')}", emoji="⚠️")
        return await _execute_step_internal(catch_step)
    return outcome

async def foreach(input_data, step_to_run, context=dict()) -> List[Any]:
    if isinstance(input_data, dict):
        items = list(input_data.values())
    elif isinstance(input_data, (list, tuple)):
        items = list(input_data)
    elif hasattr(input_data, '__iter__') and not isinstance(input_data, (str, bytes)):
        items = list(input_data)
    else:
        raise TypeError(f"foreach si aspetta una lista, tupla o dizionario, ricevuto: {type(input_data)}")
    
    results = []
    for item in items:
        if isinstance(step_to_run, tuple) and len(step_to_run) >= 1:
            fun = step_to_run[0]
            orig_args = step_to_run[1] if len(step_to_run) > 1 else ()
            orig_kwargs = step_to_run[2] if len(step_to_run) > 2 else {}
            action = (fun, (item,) + orig_args, orig_kwargs)
        else:
            action = (step_to_run, (item,), {})

        outcome = await _execute_step_internal(action, context=context.copy())
        
        if isinstance(outcome, dict) and 'success' in outcome:
            results.append(outcome.get('data'))
        else:
            results.append(outcome)
    return results

async def batch(*steps_to_run) -> Dict[str, Any]:
    if not steps_to_run:
        return {"success": True, "data": [], "errors": None}

    tasks = []
    with log_block(f"Batch with {len(steps_to_run)} steps", level="TRACE", emoji="🧬"):
        for action_step in steps_to_run:
            if hasattr(action_step, '__call__') or asyncio.iscoroutinefunction(action_step):
                 step_tuple = (action_step, (), {})
                 task = asyncio.create_task(_execute_step_internal(step_tuple))
            elif isinstance(action_step, tuple):
                 task = asyncio.create_task(_execute_step_internal(action_step))
            else:
                raise TypeError(f"batch supporta solo step (tuple) o callable, ricevuto: {type(action_step)}")
            tasks.append(task)
        
        raw_results = await asyncio.gather(*tasks, return_exceptions=True)
    
    successes = []
    failures = []
    for r in raw_results:
        if isinstance(r, Exception):
            failures.append({"type": type(r).__name__, "message": str(r)})
            continue
        if isinstance(r, dict):
            if r.get('success') is False: 
                failures.extend(r.get('errors', []))
            else:
                successes.append(r.get('data', r))
        else:
            successes.append(r)
    
    is_success = len(failures) == 0
    return {
        "success": is_success,
        "data": successes,
        "errors": failures
    }

async def race(*steps_to_run) -> Any:
    if not steps_to_run:
        return None

    tasks = []
    for action_step in steps_to_run:
        if hasattr(action_step, '__call__') or asyncio.iscoroutinefunction(action_step):
             step_tuple = (action_step, (), {})
             task = asyncio.create_task(_execute_step_internal(step_tuple))
        elif isinstance(action_step, tuple):
             task = asyncio.create_task(_execute_step_internal(action_step))
        else:
             raise TypeError(f"race supporta solo step (tuple) o callable, ricevuto: {type(action_step)}")
        tasks.append(task)

    try:
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        winner_task = done.pop()
        try:
            return winner_task.result()
        except Exception as e:
            return {"success": False, "errors": [str(e)], "type": "RaceWinnerError"}
    finally:
        for task in pending:
            task.cancel()

async def retry(action_step, attempts = 3, delay = 1.0, context=dict()) -> Any:
    last_outcome = None
    
    for attempt in range(attempts):
        framework_log("DEBUG", f"Tentativo {attempt + 1}/{attempts} per lo step...", emoji="🔄")
        outcome = await _execute_step_internal(action_step,context)
        last_outcome = outcome
        
        if not (isinstance(outcome, dict) and outcome.get('success') is False):
            framework_log("DEBUG", f"Step completato al tentativo {attempt + 1}.", emoji="✅")
            return outcome
        
        if attempt < attempts - 1:
            framework_log("WARNING", f"Fallimento. Attesa di {delay} secondi prima di riprovare.", emoji="⏳")
            await asyncio.sleep(delay)

    framework_log("ERROR", f"Fallimento definitivo dopo {attempts} tentativi.", emoji="❌")
    return last_outcome

async def timeout(action_step, max_seconds = 30.0, context=dict()) -> Any:
    try:
        task = asyncio.create_task(_execute_step_internal(action_step,context))
        return await asyncio.wait_for(task, timeout=max_seconds)
    except asyncio.TimeoutError:
        return {
            "success": False,
            "errors": [f"Timeout superato: lo step non è stato completato entro {max_seconds} secondi."],
            "type": "TimeoutError"
        }
    except Exception as e:
        return {
            "success": False,
            "errors": [f"Errore interno durante il timeout: {e}"],
            "type": "ExecutionError"
        }

async def throttle(action_step, rate_limit_ms = 1000, context=dict()) -> Any:
    fun = action_step[0]
    action_id = fun.__name__ 
    rate_limit_s = rate_limit_ms / 1000.0 
    current_time = time.time()
    
    last_execution_time = _throttle_state.get(action_id, 0)
    time_since_last_call = current_time - last_execution_time
    
    if time_since_last_call < rate_limit_s:
        wait_time = rate_limit_s - time_since_last_call
        print(f"THROTTLE: Limite raggiunto per {action_id}. Attesa di {wait_time:.3f}s...")
        await asyncio.sleep(wait_time)
        
    _throttle_state[action_id] = time.time()
    return await _execute_step_internal(action_step)

async def trigger(event_name, context=dict()) -> Dict[str, Any]:
    print(f"TRIGGER: Stage '{event_name}' in attesa di attivazione esterna...")
    if event_name not in _active_events:
        _active_events[event_name] = asyncio.Event()
    
    event_obj = _active_events[event_name]
    await event_obj.wait()

    payload = _event_payloads.pop(event_name, {"data": "Dati non disponibili o mancanti."})
    _active_events.pop(event_name, None)

    print(f"TRIGGER: Stage '{event_name}' attivato. Payload ricevuto.")
    return {
        "ok": True, 
        "data": payload
    }