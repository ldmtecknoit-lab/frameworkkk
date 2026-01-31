import uuid
import asyncio
import functools
import inspect
import time
from typing import Any, Callable, Dict, List, Optional
from framework.service.context import container
from framework.service.diagnostic import framework_log, log_block, _load_resource, buffered_log, analyze_exception, _get_system_info
import framework.service.scheme as scheme

def flow(custom_filename: str = __file__, app_context = None, **constants):
    
    def decorator(function):
        if asyncio.iscoroutinefunction(function):
            @functools.wraps(function)
            async def wrapper(*args, **kwargs):
                try:
                    result = await function(*args, **kwargs)
                    return {
                        'action': function.__name__,
                        'success': True,
                        'inputs': args,
                        'outputs': result,
                        'errors': []
                    }
                except Exception as e:
                    return {
                        'action': function.__name__,
                        'success': False,
                        'inputs': args,
                        'outputs': None,
                        'errors': [str(e)]
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

async def action(action, context=dict()) -> Any:
    fn, inputs, options = action
    # It's a Python callable
    if asyncio.iscoroutinefunction(fn):
        return await fn(*inputs, **options)
    else:
        return fn(*inputs, **options)


#@action(inputs=["try_step", "catch_step"], outputs=["results"])
@flow()
async def catch(try_step, catch_step,context=dict()):
    # action = (action, inputs, options) - fn, inputs, options
    if callable(try_step):
        try_step = (try_step,context.get('inputs',tuple()),context.get('options',dict()))
    if callable(catch_step):
        catch_step = (catch_step,context.get('inputs',tuple()),context.get('options',dict()))

    try:
        return await action(try_step, context)
    except Exception as e:
        return await action(catch_step, context)