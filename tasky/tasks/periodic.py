# Copyright 2016 John Reese
# Licensed under the MIT license

import logging

from .task import Task

Log = logging.getLogger('tasky.tasks')


class PeriodicTask(Task):
    '''Run a method on the asyncio event loop every `INTERVAL` seconds.'''

    INTERVAL = 60.0

    async def run_task(self) -> None:
        '''Execute the task inside the asyncio event loop.  Track the time it
        takes to run, and log when it starts/stops.  After `INTERVAL` seconds,
        if/once the task has finished running, run it again until `stop()`
        is called.'''

        while self.running and not self.task.cancelled():
            Log.debug('executing periodic task %s', self.name)
            before = self.time()
            await self.run()
            total = self.time() - before
            Log.debug('finished periodic task %s in %.1f seconds',
                      self.name, total)

            sleep = self.INTERVAL - total
            if sleep > 0:
                await self.sleep(sleep)
