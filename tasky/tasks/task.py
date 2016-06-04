# Copyright 2016 John Reese
# Licensed under the MIT license

import asyncio
import logging

Log = logging.getLogger('tasky.tasks')


class Task(object):
    '''Run methods on the asyncio event loop and keep track of them.'''

    def __init__(self) -> None:
        '''Initialize task state.  Be sure to call `super().__init__()` if
        you need to override this method.'''

        self.task = None  # asyncio.Task
        self.tasky = None  # Tasky manager
        self.running = True

    @property
    def name(self) -> str:
        '''This task's name.'''
        return self.__class__.__name__

    @property
    def config(self) -> 'Config':
        '''Configuration object for this task.'''

        return self.tasky.config.get(self.__class__.__name__)

    @property
    def counters(self) -> 'Stats':

        return self.tasky.stats.task_counter(self)

    async def init(self) -> None:
        '''Override this method to initialize state for your task.'''

        pass

    async def execute(self, *args, **kwargs):
        '''Execute an arbitrary function outside the event loop using
        a shared Executor.'''

        return await self.tasky.execute(*args, **kwargs)

    async def run(self) -> None:
        '''Override this method to define what happens when your task runs.'''

        await self.sleep(1.0)

    async def run_task(self) -> None:
        '''Execute the task inside the asyncio event loop.  Track the time it
        takes to run, and log when it starts/stops.'''

        Log.debug('executing task %s', self.name)
        before = self.time()
        await self.run()
        after = self.time()
        total = after - before
        Log.debug('finished task %s in %.1f seconds', self.name, total)

    async def sleep(self, duration: float=0.0) -> None:
        '''Simple wrapper around `asyncio.sleep()`.'''
        duration = max(0, duration)

        if duration > 0:
            Log.debug('sleeping task %s for %.1f seconds', self.name, duration)
            await asyncio.sleep(duration)

    def time(self) -> float:
        '''Return the current time on the asyncio event loop.'''

        return self.tasky.loop.time()

    async def stop(self) -> None:
        '''Cancel the task if it hasn't yet started, or tell it to
        gracefully stop running if it has.'''

        Log.debug('stopping task %s', self.name)
        self.running = False
        self.task.cancel()
