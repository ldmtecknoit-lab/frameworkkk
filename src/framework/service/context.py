import collections
import asyncio
import logging
from dependency_injector import containers, providers

class LogBuffer:
    """Buffer ibrido: Storico circolare (deque) + Real-time Streaming (Queue)."""
    def __init__(self, maxlen=1000):
        self._history = collections.deque(maxlen=maxlen)
        self._queue = asyncio.Queue()

    def append(self, log):
        self._history.append(log)
        try:
            # Distribuisce il log agli ascoltatori asincroni se presente un loop
            loop = asyncio.get_event_loop()
            if loop.is_running():
                self._queue.put_nowait(log)
        except Exception:
            pass

    def get_history(self, tx_id=None, limit=5):
        """Recupera gli ultimi N log, opzionalmente filtrati per Transaction ID."""
        if not tx_id:
            return list(self._history)[-limit:]
        filtered = [l for l in self._history if l.get('tx_id') == tx_id]
        return filtered[-limit:]

    async def get(self):
        """Consuma un log dalla coda asincrona (Streaming)."""
        return await self._queue.get()

    def __iter__(self):
        return iter(list(self._history))

    def __len__(self):
        return len(self._history)

class Container(containers.DynamicContainer):
    
    # Core components
    config = providers.Configuration()
    
    # Logging buffer (Hybrid LogBuffer)
    log_buffer = providers.Singleton(LogBuffer, maxlen=1000)
    
    # Module cache (replacing di['module_cache'])
    module_cache = providers.Singleton(dict)
    
    # Loading stack (replacing di['loading_stack'])
    loading_stack = providers.Singleton(set)
    
    module_cache_lock = providers.Singleton(asyncio.Lock)

# Global container instance
container = Container()
