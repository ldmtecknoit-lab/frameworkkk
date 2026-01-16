import sys
import asyncio

class messenger():

    def __init__(self,**constants):
        #print('MES-',constants)
        self.providers = constants.get('providers', {}).get('message', [])
        pass

    @flow.asynchronous(inputs='messenger',managers=('executor',))
    async def post(self, executor, **constants):
        for provider in self.providers:
            await provider.post(**constants)

    @flow.asynchronous(inputs='messenger',managers=('executor',))
    async def read(self, executor, **constants):
        prohibited = constants['prohibited'] if 'prohibited' in constants else []
        allowed = constants['allowed'] if 'allowed' in constants else ['FAST']
        operations = []
        
        for provider in self.providers:
            profile = provider.config['profile'].upper()
            domain_provider = provider.config.get('domain','*').split(',')
            domain_message = constants.get('domain',[])
            task = asyncio.create_task(provider.read(location=profile,**constants))
            operations.append(task)
        
        return await executor.first_completed(operations=operations)
        '''finished, unfinished = await asyncio.wait(operations, return_when=asyncio.FIRST_COMPLETED)
        for operation in finished:
            return operation.result()
        #return finished[0].result()'''
        '''while operations:
            
            finished, unfinished = await asyncio.wait(operations, return_when=asyncio.FIRST_COMPLETED)
            
            
            for operation in finished:
                transaction = operation.result()
                if transaction['state']:
                    result = transaction['result']

                    for task in unfinished:
                        task.cancel()
                    if unfinished:
                        await asyncio.wait(unfinished)
                    
                    return result
                else:
                    if len(operations) == 1:
                        return transaction

            operations = unfinished'''