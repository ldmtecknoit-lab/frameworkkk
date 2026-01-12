import asyncio
import sys
import os
import json
from framework.service.inspector import framework_log

imports = {
    'loader': 'framework/service/load.py',
    'flow': 'framework/service/flow.py',
    'language': 'framework/service/language.py'
}

def map_failed_tests(result):
    failed_set = set()
    all_issues = result.failures + result.errors
    for test, _ in all_issues:
        test_id: str = test.id()
        parts: list[str] = test_id.split('.')
        if len(parts) < 3: continue
        method_name: str = parts[-1]
        test_class_name: str = parts[-2]
        module_name: str = ".".join(parts[:-2])
        file_path: str = module_name
        try:
            modulo_obj = __import__(module_name, fromlist=[''])
            if hasattr(modulo_obj, '__file__'):
                path_assoluto = modulo_obj.__file__
                if path_assoluto.endswith(('.pyc', '.pyo')):
                    path_assoluto = path_assoluto[:-1]
                file_path = path_assoluto
        except Exception: pass
        failed_set.add((file_path, test_class_name, method_name))
    return failed_set

async def discover_and_run_tests():
    import unittest
    import framework.service.language as language
    import framework.service.load as loader
    
    test_dir = './src'
    test_suite = unittest.TestSuite()
    all_contract_hashes: dict[str, any] = {}

    for root, dirs, files in os.walk(test_dir):
        for file in files:
            if file.endswith(('.test.py', '.test.dsl')):
                module_path_rel = os.path.join(root, file).replace('./','')
                is_dsl = file.endswith('.test.dsl')
                ext = '.test.dsl' if is_dsl else '.test.py'
                main_path_rel = module_path_rel.replace(ext,'.py')
                
                framework_log("DEBUG", f"Analisi test per: {module_path_rel}", emoji="🔍")
                try:
                    # Carica senza salvare per ora (il salvataggio avverrà dopo i test unittest)
                    res = await loader.generate_checksum(main_path_rel, run_tests=is_dsl, save=False)
                    if not res.get('success'): continue
                    
                    hashes = res.get('data', {}).get(main_path_rel, {})
                    all_contract_hashes[main_path_rel] = hashes
                    
                    if not is_dsl:
                        module = await loader.resource(path=module_path_rel)
                        test_suite.addTest(unittest.defaultTestLoader.loadTestsFromModule(module))
                except Exception as e:
                    framework_log("ERROR", f"Errore analisi {module_path_rel}: {e}", emoji="❌")
                    continue
                    
    return all_contract_hashes, test_suite

def test(save=False):
    import unittest
    import asyncio
    
    framework_log("INFO", "🔍 Avvio test runner...", emoji="🧪")
    all_contract_hashes, suite_test = asyncio.run(discover_and_run_tests())
    
    test_count = suite_test.countTestCases()
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite_test)
    
    fail = map_failed_tests(result)
    success = result.wasSuccessful()
    
    if save:
        framework_log("INFO", "� Salvataggio contratti validati...", emoji="📝")
        for main_path, hashes in all_contract_hashes.items():
            # Per i test .py legacy, puliamo quelli falliti
            test_py_path = main_path.replace('.py', '.test.py')
            clean_hashes = hashes.copy()
            for f_path, f_class, f_meth in fail:
                if f_path == test_py_path or f_path.endswith(test_py_path):
                    tgt = '__module__' if 'TestModule' in f_class else f_class.replace('Test','')
                    m = f_meth.replace('test_','')
                    if tgt in clean_hashes and m in clean_hashes[tgt]:
                        del clean_hashes[tgt][m]
            
            if clean_hashes:
                json_path = main_path.replace('.py', '.contract.json')
                with open(json_path, "w") as f:
                    f.write(json.dumps(clean_hashes, indent=4))
                framework_log("INFO", f"✅ Salvato: {json_path}")

    return 0 if success else 1

def application(tester=None, **constants):
    args = constants.get('args', sys.argv)
    import framework.service.load as loader
    import framework.service.language as language
    import framework.service.flow as flow

    if '--generate-contract' in args:
        event_loop = asyncio.new_event_loop()
        async def regenerate_all():
            test_dir = './src'
            for root, _, files in os.walk(test_dir):
                for file in files:
                    if file.endswith('.py') and not file.endswith('.test.py') and not file.startswith('__'):
                        await loader.generate_checksum(os.path.join(root, file).replace('./',''), save=True)
        event_loop.run_until_complete(regenerate_all())
        return

    if '--test' in args or '--test-save' in args:
        sys.exit(test(save='--test-save' in args))

    # Boostrap
    event_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(event_loop)
    try:
        # Carica il bootstrap DSL
        bootstrap_res = event_loop.run_until_complete(loader.resource(path='framework/service/bootstrap.dsl'))
        parsed_dsl = bootstrap_res.get('data') if isinstance(bootstrap_res, dict) else bootstrap_res
        config_data = event_loop.run_until_complete(language.execute_dsl_file(parsed_dsl))
        
        if isinstance(config_data, dict) and '__triggers__' in config_data:
            for trigger_key, action in config_data['__triggers__']:
                if isinstance(trigger_key, (list, tuple)) and any(x == '*' for x in trigger_key):
                    event_loop.create_task(flow.cron(trigger_key, action, context={'system': True}))
                elif isinstance(trigger_key, tuple) and trigger_key[0] == 'CALL':
                    async def event_listener(tk, ac):
                        ctx = {'system': True}
                        while True:
                            try:
                                res = await language.DSLVisitor(language.dsl_functions).execute_call(tk, ctx)
                                if res and res.get('success') and res.get('data'):
                                    await language.DSLVisitor(language.dsl_functions).visit(ac, ctx | {'@event': res['data']})
                                await asyncio.sleep(1)
                            except Exception as e:
                                await asyncio.sleep(5)
                    event_loop.create_task(event_listener(trigger_key, action))
    except Exception as e:
        framework_log("ERROR", f"Crashed at bootstrap: {e}", emoji="💥")
    
    event_loop.run_forever()