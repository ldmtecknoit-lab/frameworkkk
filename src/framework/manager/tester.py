import unittest
import os
import types
import sys
import io
import asyncio
import inspect
from framework.service.diagnostic import framework_log
import framework.service.language as language
import framework.service.flow as flow

class tester():

    def __init__(self,**constants):
        self.sessions = dict()
        self.providers = constants.get('providers',[])

    async def discover_tests(self):
        # Pattern personalizzato per i test
        import framework.service.load as loader
        test_dir = './src'
        test_suite = unittest.TestSuite()
        
        # Scorri tutte le sottocartelle e i file
        for root, dirs, files in os.walk(test_dir):
            for file in files:
                if file.endswith('.test.py'):
                    # Crea il nome del modulo di test per ciascun file trovato
                    module_path = os.path.join(root, file).replace('./','')
                    
                    # Importa il modulo di test dinamicamente via loader
                    try:
                        res = await loader.resource(path=module_path)
                        if res.get('success'):
                            module = res['data']
                            # Aggiungi i test dal modulo
                            test_suite.addTest(unittest.defaultTestLoader.loadTestsFromModule(module))
                    except Exception as e:
                        framework_log("ERROR", f"Errore caricamento test {module_path}: {e}", emoji="❌")
        return test_suite

    async def run(self,**constants):
        framework_log("INFO", "Avvio esecuzione suite di test...", emoji="🧪")
        import framework.service.load as loader
        test_dir = './src'
        
        # Scorri tutte le sottocartelle e i file
        for root, dirs, files in os.walk(test_dir):
            for file in files:
                if file.endswith('.test.dsl'):
                    # Crea il nome del modulo di test per ciascun file trovato
                    module_path = os.path.join(root, file).replace('./','')
                    
                    # Importa il modulo di test dinamicamente via loader
                    parser = language.create_parser()
                    visitor = language.Interpreter(language.DSL_FUNCTIONS)
                    await flow.catch(
                        flow.step(flow.pipeline,
                            flow.step(loader.resource,path=module_path),
                            flow.step(language.parse,'@.inputs',parser),
                            #flow.step(visitor.run,'@.inputs'),
                            flow.step(flow.log,"--->: {inputs}  \n"),
                        ),
                        flow.step(flow.log,"Errore: {errors[0]} "),
                    )
    
    async def unittest2(self, code: str, **constants):
        def get_test_methods( suite):
            test_methods = []
            for test in suite:
                if hasattr(test, 'test_'):  # Verifica se è un caso di test
                    method_name = test._testMethodName
                    method = getattr(test, method_name, None)
                    if asyncio.iscoroutinefunction(method):  # Controlla se è una coroutine
                        test_methods.append((method_name, "async"))
                    else:
                        test_methods.append((method_name, "sync"))
            return test_methods
        #code = code.replace('unittest.IsolatedAsyncioTestCase','unittest.TestCase')
        # Crea un modulo Python temporaneo
        module_name = "__dynamic_test_module__"
        test_module = types.ModuleType(module_name)

        # Inietta le costanti nel contesto del modulo
        for key, value in constants.items():
            setattr(test_module, key, value)

        # Esegue il codice della stringa nel contesto del modulo
        exec(code, test_module.__dict__)

        # Registra il modulo temporaneamente in sys.modules
        sys.modules[module_name] = test_module

        # Trova le classi di test definite nel modulo
        loader = unittest.TestLoader()
        suite = loader.loadTestsFromModule(test_module)

        # Esegue i test e cattura l'output
        stream = io.StringIO()
        #runner = unittest.TextTestRunner(stream=stream, verbosity=2)
        #result = runner.run(suite)
        results = unittest.TestResult()
        for test in suite:
            #test.run(results)
            #print(get_test_methods(suite))
            lol = getattr(test, '_tests', [])
            for case in lol:
                print(case())
            #print(await test())

        # Risultati
        framework_log("INFO", "Risultati Test Unittest2", emoji="🧪", 
                      total=results.testsRun, 
                      errors=len(results.errors), 
                      failures=len(results.failures))

        # Rimuove il modulo temporaneo
        del sys.modules[module_name]

        # Stampa o restituisce i risultati
        print(stream.getvalue())
        #return result
        if results.failures or results.errors:
            return (False,results.testsRun,results.errors,results.failures)
        else:
            return (True,results.testsRun,results.errors,results.failures)


    async def unittest(self, code: str, **constants):
        '''module = types.ModuleType("dynamic_module")
        module.__dict__.update(constants)
        module.__dict__.update({
            'unittest': unittest,
            'asyncio': asyncio,
        })

        exec(code, module.__dict__)'''

        module = await language.load_module(language,code=code)

        test_classes = [
            cls for cls in module.__dict__.values()
            if inspect.isclass(cls) and issubclass(cls, unittest.TestCase)
        ]

        results = {
            "testsRun": 0,
            "errors": [],
            "failures": [],
            "successes": [],
            "setup":None,
            "teardown":None,
        }

        for TestClass in test_classes:
            test_methods = [
                name for name, func in inspect.getmembers(TestClass, predicate=inspect.isfunction)
                if name.startswith("test_")
            ]

            for method_name in test_methods:
                test_instance = TestClass(method_name)
                results["testsRun"] += 1
                test_id = f"{TestClass.__name__}.{method_name}"

                async def run_test():
                    if hasattr(test_instance, "setUp"):
                        results["setup"] = test_instance.setUp()
                    if hasattr(test_instance, "asyncSetUp"):
                        ok = await test_instance.asyncSetUp()
                        results["teardown"] = ok
                        

                    method = getattr(test_instance, method_name)
                    if inspect.iscoroutinefunction(method):
                        await method()
                    else:
                        method()

                    if hasattr(test_instance, "tearDown"):
                        test_instance.tearDown()
                    if hasattr(test_instance, "asyncTearDown"):
                        await test_instance.asyncTearDown()

                try:
                    await run_test()
                    results["successes"].append(test_id)
                except AssertionError as e:
                    results["failures"].append((test_id, str(e)))
                except Exception as e:
                    results["errors"].append((test_id, str(e)))

        # Determina lo stato complessivo
        if results["failures"] or results["errors"]:
            results["state"] = False
        else:
            results["state"] = True

        return results



    async def dsl(self, **kwargs):
        """
        Esegue i test definiti in un file DSL o in una struttura dati DSL.
        Supporta la verifica di hash (integrità) e casi di test TDD.
        """
        from framework.service.load import resource, helper_verify_integrity, helper_get_contract
        
        path = kwargs.get('path')
        parsed = kwargs.get('data') or kwargs.get('parsed')
        
        if path and not parsed:
            res = await resource(path)
            if res.get('success'):
                parsed = res.get('data')
        
        if not parsed:
             return {"success": False, "errors": ["DSL non caricabile o non fornito"]}

        # 1. Verifica Integrità (Hash) se possibile
        integrity_results = {}
        source_path = parsed.get('source') or (path.replace('.test.dsl', '.py') if path and '.test.dsl' in path else None)
        
        if source_path:
            try:
                contract = await helper_get_contract(source_path)
                if contract:
                    integrity_results = await helper_verify_integrity(source_path, contract)
            except Exception as e:
                framework_log("WARNING", f"Errore verifica integrità per {source_path}: {e}")

        # 2. Esecuzione Test Suite (TDD)
        test_suite = parsed.get('test_suite', [])
        if isinstance(test_suite, dict): test_suite = [test_suite]
        
        results = {
            "total": 0,
            "passed": 0,
            "failed": 0,
            "errors": [],
            "details": [],
            "integrity": integrity_results
        }
        
        visitor = language.DSLVisitor(language.dsl_functions)
        await visitor.run(parsed)

        for test in test_suite:
            if not isinstance(test, dict): continue
            results["total"] += 1
            target = test.get('target')
            args = test.get('input_args', ())
            if not isinstance(args, (list, tuple)):
                args = (args,)
            
            expected = test.get('expected_output')
            
            try:
                target_def = parsed.get(target)
                if isinstance(target_def, tuple) and len(target_def) == 3:
                    actual = await visitor.execute_dsl_function(target_def, args)
                else: 
                    actual = await visitor.visit(target_def)
                
                if actual == expected:
                    results["passed"] += 1
                    results["details"].append({"target": target, "status": "OK"})
                else:
                    results["failed"] += 1
                    results["details"].append({
                        "target": target, 
                        "status": "FAIL", 
                        "expected": expected, 
                        "actual": actual
                    })
            except Exception as e:
                results["failed"] += 1
                results["errors"].append({"target": target, "error": str(e)})
                results["details"].append({"target": target, "status": "ERROR", "message": str(e)})

        framework_log("INFO", f"DSL Test {path or 'Inline'}: {'PASSED' if results['failed'] == 0 else 'FAILED'}", 
                      total=results["total"], passed=results["passed"], failed=results["failed"])
        
        return {
            "success": results["failed"] == 0,
            "data": results
        }