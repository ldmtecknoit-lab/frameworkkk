from unittest import IsolatedAsyncioTestCase
import inspect

import asyncio

imports  = {
    #'flow': 'framework/service/flow.py',
    #'loader': 'framework/service/loader.py'
}

class Contract(IsolatedAsyncioTestCase):

    def setUp(self):
        #event_loop = asyncio.get_event_loop()
        #event_loop.create_task(loader.bootstrap())
        pass
    
    '''@classmethod
    def setUpClass(cls):
        """
        Configura il test adapter una sola volta per tutti i test.
        """
        cls.test = cls.adapter(config=cls.config)

        # Assicura che i metodi astratti siano implementati
        for method in cls.port.__abstractmethods__:
            if not hasattr(cls.test, method):
                raise AssertionError(f"{type(cls.test).__name__} non implementa il metodo astratto '{method}'")'''
    
    async def check_cases(self,action, cases):
        """
        Definisce i casi di test per il metodo 'get'.
        """

        is_action_async = inspect.iscoroutinefunction(action)

        

        for i, case in enumerate(cases):
            args = case.get('args', tuple())
            if not isinstance(args, tuple): args = (args,)
            kwargs = case.get('kwargs', {})
            with self.subTest(msg=f"Success Case {i+1}: get({args}, {kwargs})"):
                if case.get('error'):
                    with self.assertRaises(case['error']):
                        if is_action_async:
                            result = await action(*args, **kwargs)
                        else:
                            result = action(*args, **kwargs)
                else:
                    if is_action_async:
                        result = await action(*args, **kwargs)
                    else:
                        result = action(*args, **kwargs)
                
                if 'type' in case:
                    self.assertIsInstance(result, case['type'])
                elif 'equal' in case:
                    self.assertEqual(result, case['equal'])
                elif 'error' in case:
                    pass
                else:
                    self.fail(f"Caso di test malformato: {case}. Mancano 'type' o 'equal' per un caso di successo.")
