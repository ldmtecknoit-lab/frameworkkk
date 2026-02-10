import asyncio
import datetime
from framework.service.flow import framework_log

# ============================================================================
# TRIGGER ENGINE
# ============================================================================

class TriggerEngine:
    """
    Responsible only for scheduling and executing DSL triggers
    (event-based and cron-based).
    """

    def __init__(self, visitor):
        self.visitor = visitor
        self.tasks = []

    # ----------------------------------------------------------------------

    def register_triggers(self, triggers, context):
        """
        triggers: list[(trigger_key, action)]
        """
        for trigger, action in triggers:
            if self._is_event(trigger):
                task = asyncio.create_task(
                    self._event_loop(trigger, action, context)
                )
            elif self._is_cron(trigger):
                task = asyncio.create_task(
                    self._cron_loop(trigger, action, context)
                )
            else:
                continue

            self.tasks.append(task)

    # ----------------------------------------------------------------------
    # TRIGGER TYPES
    # ----------------------------------------------------------------------

    def _is_event(self, trigger):
        return isinstance(trigger, tuple) and trigger[:1] == ('CALL',)

    def _is_cron(self, trigger):
        return isinstance(trigger, tuple) and '*' in trigger

    # ----------------------------------------------------------------------
    # EVENT LOOP
    # ----------------------------------------------------------------------

    async def _event_loop(self, call_node, action, ctx):
        name = call_node[1]
        framework_log("INFO", f"Event listener: {name}", emoji="👂")

        while True:
            try:
                result = await self.visitor.visit(call_node, ctx)

                if isinstance(result, dict) and result.get('success'):
                    event_ctx = {**ctx, '@event': result.get('data')}
                    await self.visitor.visit(action, event_ctx)
                else:
                    await asyncio.sleep(1)

            except asyncio.CancelledError:
                break
            except Exception as e:
                framework_log("ERROR", f"Event error: {e}", emoji="❌")
                await asyncio.sleep(5)

    # ----------------------------------------------------------------------
    # CRON LOOP
    # ----------------------------------------------------------------------

    async def _cron_loop(self, pattern, action, ctx):
        framework_log("INFO", f"Cron trigger: {pattern}", emoji="⏰")

        while True:
            try:
                now = datetime.datetime.now()
                current = (
                    now.minute,
                    now.hour,
                    now.day,
                    now.month,
                    now.weekday()
                )

                if all(p == '*' or str(p) == str(c) for p, c in zip(pattern, current)):
                    await self.visitor.visit(action, ctx)

                await asyncio.sleep(60 - now.second)

            except asyncio.CancelledError:
                break
            except Exception as e:
                framework_log("ERROR", f"Cron error: {e}", emoji="❌")
                await asyncio.sleep(60)

    # ----------------------------------------------------------------------

    async def shutdown(self):
        for task in self.tasks:
            task.cancel()

        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
