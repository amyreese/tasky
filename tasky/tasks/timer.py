# Copyright 2016 John Reese
# Licensed under the MIT license

import logging

from .task import Task

Log = logging.getLogger('tasky.tasks')


class TimerTask(Task):
    '''Run a method on the asyncio event loop exactly once after `DELAY`
    seconds.  Calling the `reset()` method will postpone execution, or re-queue
    execution if the timer has already completed.'''

    DELAY = 60.0

    async def run_task(self) -> None:
        '''Execute the task inside the asyncio event loop after `DELAY`
        seconds.  Track the time it takes to run, and log when it starts/stops.
        If/when `reset()` is called, reset the wait time to `DELAY` seconds.'''

        self.last_run = 0.0
        self.target = self.time() + self.DELAY

        while self.running and not self.task.cancelled():
            now = self.time()

            if now < self.target:
                sleep = self.target - now
                await self.sleep(sleep)

            elif self.last_run < self.target:
                Log.debug('executing timer task %s', self.name)
                self.last_run = self.time()
                await self.run()
                total = self.time() - self.last_run
                Log.debug('finished timer task %s in %.1f seconds',
                          self.name, total)

            else:
                sleep = min(5.0, self.DELAY)
                await self.sleep(sleep)

    def reset(self) -> None:
        '''Reset task execution to `DELAY` seconds from now.'''

        Log.debug('resetting timer task %s')
        self.target = self.time() + self.DELAY
