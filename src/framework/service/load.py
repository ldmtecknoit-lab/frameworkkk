import os
import sys
import asyncio
import logging
import types
import inspect
import uuid
import json
import time
import re
from typing import Dict, Any, List, Optional, Set, Union
from framework.service.context import container
import framework.service.flow as flow
import framework.service.language as language
from framework.service.inspector import (
    analyze_module, framework_log, log_block, _load_resource, 
    analyze_exception, estrai_righe_da_codice, backend
)
from framework.service.scheme import convert
from dependency_injector import providers

# imports: {"flow": "framework/service/flow.py", "language": "framework/service/language.py"};

# =====================================================================
# --- DATA STRUCTURES (CONTEXTS) ---
# =====================================================================

class ValidationContext:
    def __init__(self, main_path: str, main_module: Any = None):
        self.main_path = main_path
        self.main_module = main_module
        self.contract_json_path = main_path.replace('.py', '.contract.json')
        self.external_contracts: Dict = {}
        self.test_path: Optional[str] = None
        self.test_code: Optional[str] = None
        self.contract_ana: Dict = {} 
        self.contract_module: Any = None
        self.is_dsl: bool = False
        self.exports_map: Dict = {}
        self.validated_methods: Dict[str, Set[str]] = {}
        self.allowed_exports: Set[str] = set()

# =====================================================================
# --- COMPONENT 1: CONTRACT ENGINE ---
# =====================================================================

class ContractEngine:
    @staticmethod
    async def load_context(main_path: str, main_module: Any = None) -> ValidationContext:
        ctx = ValidationContext(main_path, main_module)
        try:
            json_content = await _load_resource(path=ctx.contract_json_path)
            if json_content:
                ctx.external_contracts = await convert(json_content, dict, 'json')
        except Exception: pass

        dsl_path, py_path = main_path.replace('.py', '.test.dsl'), main_path.replace('.py', '.test.py')
        for p, dsl in [(dsl_path, True), (py_path, False)]:
            try:
                ctx.test_code = await _load_resource(path=p)
                if ctx.test_code:
                    ctx.test_path, ctx.is_dsl = p, dsl
                    break
            except Exception: continue

        if not ctx.test_code: return ctx

        if ctx.is_dsl:
            ctx.contract_ana = language.parse_dsl_file(ctx.test_code)
            ctx.contract_module = ctx.contract_ana
        else:
            # We load the test module as RAW (unfiltered) to use its metadata
            res = await _resource_internal(ctx.test_path, is_test_load=True)
            ctx.contract_module = res.get('data')
            ctx.contract_ana = analyze_module(ctx.test_code, ctx.test_path)
        return ctx

    @staticmethod
    def resolve_exports(ctx: ValidationContext):
        raw_exports = {}
        if ctx.is_dsl: 
            raw_exports = ctx.contract_module.get('exports', {})
        else: 
            raw_exports = getattr(ctx.contract_module, 'exports', {}) if isinstance(getattr(ctx.contract_module, 'exports', None), dict) else {}
        
        ctx.exports_map = {str(k): str(v) for k, v in raw_exports.items()}
        
        if not ctx.exports_map:
            if ctx.external_contracts:
                for k, v in ctx.external_contracts.items():
                    if k == '__module__' and isinstance(v, dict):
                        for m in v.keys(): ctx.exports_map[m] = m
                    else: ctx.exports_map[k] = k
            elif 'framework/service/' in ctx.main_path:
                for k in dir(ctx.main_module):
                    if not k.startswith('_'): ctx.exports_map[k] = k
        
        ctx.exports_map['language'] = 'language'
        return ctx.exports_map

    @staticmethod
    async def validate_hashes(ctx: ValidationContext):
        if not ctx.external_contracts: return {}
        # Avoid infinite recursion during self-checksumming
        ccc_env = await generate_checksum(ctx.main_path)
        ccc = ccc_env.get('data', {}).get(ctx.main_path, {})
        valid_map = {}
        for group_name, group_hashes in ctx.external_contracts.items():
            valid_set = set()
            for m_name, h_info in group_hashes.items():
                if not isinstance(h_info, dict): continue
                curr = ccc.get(group_name, {}).get(m_name, {})
                if curr.get('production') == h_info.get('production') and curr.get('test') == h_info.get('test'): 
                    valid_set.add(m_name)
            if valid_set: valid_map[group_name] = valid_set
        ctx.validated_methods = valid_map
        return valid_map

    @staticmethod
    def compute_allowed(ctx: ValidationContext):
        ctx.allowed_exports = {'language'}
        
        # Self-exposing core methods for load.py to avoid bootstrap deadlock
        if 'framework/service/load.py' in ctx.main_path:
            ctx.allowed_exports.update({'resource', 'bootstrap', 'register', 'generate_checksum'})

        if not ctx.external_contracts:
            for public, private in ctx.exports_map.items():
                if hasattr(ctx.main_module, private): ctx.allowed_exports.add(public)
            return ctx.allowed_exports

        if ctx.is_dsl:
            test_suite = ctx.contract_ana.get('test_suite', [])
            tested = {str(t.get('target')) for t in test_suite if isinstance(t, dict)}
            if not test_suite: tested = set(ctx.exports_map.keys())
            for public, private in ctx.exports_map.items():
                if (hasattr(ctx.main_module, private) or private in dir(ctx.main_module)) and public in tested:
                    if public in ctx.validated_methods.get('__module__', {}) or any(public in vm for tk, vm in ctx.validated_methods.items() if tk != '__module__'):
                        ctx.allowed_exports.add(public)
        else:
            tested_map = {(tk := '__module__' if mn == 'TestModule' else mn.replace('Test', '')): {fn.replace('test_', '') for fn in (d.get('data', {}).get('methods', {}) or {}).keys() if fn.startswith('test_')} for mn, d in ctx.contract_ana.items() if isinstance(d, dict)}
            for public, private in ctx.exports_map.items():
                val = getattr(ctx.main_module, private, None)
                if not val: continue
                is_tested = (
                    (inspect.isclass(val) and (tested_map.get(public) or ctx.validated_methods.get(public))) or
                    (inspect.isfunction(val) and (public in tested_map.get('__module__', {}) and public in ctx.validated_methods.get('__module__', {})))
                )
                if is_tested: ctx.allowed_exports.add(public)
        return ctx.allowed_exports

# =====================================================================
# --- COMPONENT 2: MODULE BUILDER ---
# =====================================================================

class ModuleBuilder:
    @staticmethod
    def build_proxy(ctx: ValidationContext) -> types.ModuleType:
        proxy = types.ModuleType(f"filtered:{ctx.main_path}")
        proxy.__file__ = ctx.main_path
        if hasattr(ctx.main_module, 'language'): proxy.language = ctx.main_module.language
        else: proxy.language = language
        for public_name in ctx.allowed_exports:
            private_name = str(ctx.exports_map.get(public_name, public_name))
            attr = getattr(ctx.main_module, private_name, None)
            if attr is None: continue
            if inspect.isclass(attr):
                methods_to_expose = ctx.validated_methods.get(public_name, set())
                # In auto-trust or for core load.py, expose all public methods
                if not ctx.external_contracts or 'framework/service/load.py' in ctx.main_path:
                    methods_to_expose = {m for m in dir(attr) if not m.startswith('_')}
                class_proxy = type(public_name, (object,), { m: getattr(attr, m) for m in methods_to_expose if hasattr(attr, m) })
                setattr(proxy, public_name, class_proxy)
            else: setattr(proxy, public_name, attr)
        return proxy

# =====================================================================
# --- PUBLIC INTERFACE FUNCTIONS ---
# =====================================================================

async def resource(path: str) -> Any:
    cache = container.module_cache()
    if path in cache: return {"data": cache[path], "success": True}
    loading_stack = container.loading_stack()
    if path in loading_stack:
        fake_name = f"loading:{path}"
        if fake_name in sys.modules: return {"data": sys.modules[fake_name], "success": True}
        while path in loading_stack: await asyncio.sleep(0.01)
        if path in cache: return {"data": cache[path], "success": True}
    loading_stack.add(path)
    try:
        res = await _resource_internal(path)
        if res.get('success') and res.get('data'):
            async with container.module_cache_lock(): cache[path] = res['data']
        return res
    finally:
        loading_stack.remove(path)

async def _resource_internal(path: str, is_test_load=False) -> Any:
    try:
        content = await _load_resource(path=path)
        if path.endswith('.json'): return {"data": await convert(content, dict, 'json'), "success": True}
        if path.endswith('.dsl'): return {"data": language.parse_dsl_file(content), "success": True}
        if not path.endswith('.py'): return {"data": content, "success": True}
        
        unique_name, fake_name = f"mod_{uuid.uuid4().hex[:8]}", f"loading:{path}"
        raw_module = await _load_python_module(unique_name, path, content, placeholder_name=fake_name)
        if '.test.' in path or is_test_load: return {"data": raw_module, "success": True}
        
        ctx = await ContractEngine.load_context(path, raw_module)
        ContractEngine.resolve_exports(ctx)
        await ContractEngine.validate_hashes(ctx)
        ContractEngine.compute_allowed(ctx)
        
        filtered = ModuleBuilder.build_proxy(ctx)
        framework_log("INFO", f"✅ Validated: {path}. Exposed: {list(ctx.allowed_exports)}", emoji="✅")
        return {"data": filtered, "success": True}
    except Exception as e:
        framework_log("ERROR", f"❌ Failed to load {path}: {e}")
        return {"data": None, "success": False, "errors": [str(e)]}

# =====================================================================
# --- INTERNAL HELPERS ---
# =====================================================================

async def _load_python_module(name: str, path: str, code: str, placeholder_name=None) -> Any:
    module = types.ModuleType(name)
    module.__file__ = path
    sys.modules[name] = module
    if placeholder_name: sys.modules[placeholder_name] = module
    
    deps = {}
    for line in code.splitlines():
        if 'imports:' in line:
            try: deps = await convert(line.split('imports:')[1].split(';')[0], dict, 'json')
            except: pass
            break
    if deps:
        for key, d_path in deps.items():
            res = await resource(d_path)
            setattr(module, key, res.get('data') if isinstance(res, dict) else res)
    try:
        exec(code, module.__dict__)
        return module
    except Exception as e:
        analyze_exception(e, path, code)
        raise ImportError(f"Execution failed for {path}: {e}")

async def run_dsl_test_suite(test_ana: Dict[str, Any], main_module: Any) -> Dict[str, Any]:
    suite = test_ana.get('test_suite', [])
    if not suite: return {'success': True, 'results': [], 'message': 'No tests to run.'}
    results, all_success = [], True
    from framework.service.language import DSLVisitor, dsl_functions
    visitor = DSLVisitor(dsl_functions)
    visitor.root_data = test_ana
    for i, t in enumerate(suite):
        if not isinstance(t, dict): continue
        target, args, expected = str(t.get('target')), t.get('input_args', ()), t.get('expected_output')
        func = getattr(main_module, target, None)
        if not func:
            all_success = False
            results.append({'test': i, 'target': target, 'success': False, 'error': f'Function {target} not found.'})
            continue
        async def resolve(item):
            if isinstance(item, (list, tuple)): return [await resolve(x) for x in item]
            return await visitor.visit(item)
        resolved_args = await resolve(args)
        if not isinstance(resolved_args, (list, tuple)): resolved_args = (resolved_args,)
        try:
            actual = await func(*resolved_args) if inspect.iscoroutinefunction(func) else func(*resolved_args)
            def normalize(v):
                if isinstance(v, (list, tuple)): return [normalize(x) for x in v]
                return v
            if normalize(actual) == normalize(expected): results.append({'test': i, 'target': target, 'success': True})
            else:
                all_success = False
                results.append({'test': i, 'target': target, 'success': False, 'error': f'Mismatch: expected {expected}, got {actual}'})
        except Exception as e:
            all_success = False
            results.append({'test': i, 'target': target, 'success': False, 'error': f'Runtime error: {e}'})
    return {'success': all_success, 'results': results}

async def generate_checksum(main_path: str, save: bool = False, run_tests: bool = False) -> Dict[str, Any]:
    main_code = await _load_resource(path=main_path)
    if not main_code: return {'success': False, 'error': 'Source file not found'}
    ctx = await ContractEngine.load_context(main_path)
    if not ctx.test_code: return {'success': False, 'error': 'No test file found'}
    if run_tests and ctx.is_dsl:
        # Use unfiltered load for test execution
        temp_mod = await _load_python_module("temp_check", main_path, main_code)
        test_res = await run_dsl_test_suite(ctx.contract_ana, temp_mod)
        if not test_res['success']: return {'success': False, 'error': 'Tests failed', 'results': test_res}
    
    static_ana, contract_hashes, test_hash = analyze_module(main_code, main_path), {}, await convert(ctx.test_code, str, 'hash')
    if ctx.is_dsl:
        for public, private in ctx.contract_ana.get('exports', {}).items():
            item = static_ana.get(str(private), {})
            if not item: continue
            if item.get('type') == 'function':
                code = estrai_righe_da_codice(main_code, item['data'].get('lineno', 0), item['data'].get('end_lineno', 0))
                contract_hashes.setdefault('__module__', {})[str(public)] = {'production': await convert(code, str, 'hash'), 'test': test_hash}
            elif item.get('type') == 'import':
                # For imports, we just hash the line and the test
                code = estrai_righe_da_codice(main_code, item['data'].get('lineno', 0), item['data'].get('lineno', 0))
                contract_hashes.setdefault('__module__', {})[str(public)] = {'production': await convert(code, str, 'hash'), 'test': test_hash}
            elif item.get('type') == 'class':
                methods = item['data'].get('methods', {})
                for m_name, m_data in methods.items():
                    if m_name.startswith('_'): continue
                    code = estrai_righe_da_codice(main_code, m_data.get('lineno', 0), m_data.get('end_lineno', 0))
                    contract_hashes.setdefault(str(public), {})[m_name] = {'production': await convert(code, str, 'hash'), 'test': test_hash}
    else:
        test_ana = analyze_module(ctx.test_code, ctx.test_path)
        for cls_name, cls_data in test_ana.items():
            if not (isinstance(cls_data, dict) and cls_data.get('type') == 'class'): continue
            target_group = '__module__' if cls_name == 'TestModule' else cls_name.replace('Test', '')
            methods = cls_data.get('data', {}).get('methods', {})
            for m_name, m_data in methods.items():
                if not m_name.startswith('test_'): continue
                raw_name = m_name.replace('test_', '')
                if target_group == '__module__': p_info = static_ana.get(raw_name, {}).get('data', {})
                else: p_info = static_ana.get(target_group, {}).get('data', {}).get('methods', {}).get(raw_name, {})
                if not p_info: continue
                p_code, t_code = estrai_righe_da_codice(main_code, p_info.get('lineno', 0), p_info.get('end_lineno', 0)), estrai_righe_da_codice(ctx.test_code, m_data.get('lineno', 0), m_data.get('end_lineno', 0))
                contract_hashes.setdefault(target_group, {})[raw_name] = {'production': await convert(p_code, str, 'hash'), 'test': await convert(t_code, str, 'hash')}
    
    if save and contract_hashes: 
        await backend(path=ctx.contract_json_path, content=json.dumps(contract_hashes, indent=4), mode='w')
    return {'data': {main_path: contract_hashes}, 'success': True, 'is_dsl': ctx.is_dsl}

async def register(**config: Any) -> Dict[str, Any]:
    path, service_name = config.get('path'), config.get('service', config.get('name'))
    adapter_name = config.get('adapter', service_name)
    init_payload, dependency_keys = config.get('payload', config.get('config', {})), config.get('dependency_keys')
    if not path or not service_name: raise ValueError(f"Incomplete DI config: {config}")
    async def _reg_task(context=None):
        raw_res = await resource(path)
        module = raw_res.get('data') if isinstance(raw_res, dict) else raw_res
        if isinstance(module, dict) and not module.get('success', True): raise ImportError(f"Load failed: {module.get('errors')}")
        target_attr = getattr(module, adapter_name)
        if not hasattr(container, service_name): setattr(container, service_name, providers.Singleton(list))
        if dependency_keys:
            deps = {k: getattr(container, k)() for k in dependency_keys if hasattr(container, k)}
            setattr(container, service_name, providers.Factory(target_attr, **init_payload, providers=deps))
        else:
            service_list = getattr(container, service_name)()
            service_list.append(target_attr(config=init_payload))
        return {"success": True}
    return await flow.pipe(flow.step(_reg_task), context=config)
async def bootstrap(): return await resource(path="framework/service/bootstrap.dsl")