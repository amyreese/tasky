# Copyright 2016 John Reese
# Licensed under the MIT license

import asyncio
import logging

Log = logging.getLogger('tasky')

RUNNING_TASKS = set()


class Task(object):
    '''Run methods on the asyncio event loop and keep track of them.'''

    @property
    def name(self):
        return self.__class__.__name__

    def __init__(self) -> None:
        '''Initialize task state.  Be sure to call `super().__init__()` if
        you need to override this method.'''

        self.task = None  # asyncio.Task
        self.tasky = None  # Tasky manager
        self.running = True

    async def run(self) -> None:
        '''Override this method to define what happens when your task runs.'''

        await asyncio.sleep(1.0)

    async def run_task(self) -> None:
        '''Execute the task inside the asyncio event loop.  Track the time it
        takes to run, and log when it starts/stops.'''

        loop = asyncio.get_event_loop()

        Log.debug('executing task %s', self.name)
        before = loop.time()
        await self.run()
        after = loop.time()
        total = after - before
        Log.debug('finished task %s in %d seconds', self.name, total)

    async def sleep(self, duration: float=0.0) -> None:
        '''Simple wrapper around `asyncio.sleep()`.'''
        duration = max(0, duration)

        Log.debug('sleeping task %s for %d seconds', self.name, duration)
        await asyncio.sleep(duration)

    def stop(self) -> None:
        '''Cancel the task if it hasn't yet started, or tell it to
        gracefully stop running if it has.'''

        Log.debug('stopping task %s', self.name)
        self.running = False
        self.task.cancel()


class OneShotTask(Task):
    '''Run an arbitrary, coroutine or blocking function exactly once.'''

    def __init__(self, fn, *args, **kwargs):
        super().__init__()

        self.fn = fn
        self.args = args
        self.kwargs = kwargs

    @property
    def name(self):
        return '{0}({1})'.format(self.__class__.__name__, self.fn.__name__)

    async def run(self) -> None:
        '''Run the requested function with the given arguments.'''

        if asyncio.iscoroutinefunction(self.fn):
            await self.fn(*self.args, **self.kwargs)
        else:
            self.fn(*self.args, **self.kwargs)


class PeriodicTask(Task):
    '''Run a method on the asyncio event loop every `INTERVAL` seconds.'''

    INTERVAL = 60.0

    async def run_task(self) -> None:
        '''Execute the task inside the asyncio event loop.  Track the time it
        takes to run, and log when it starts/stops.  After `INTERVAL` seconds,
        if/once the task has finished running, run it again until `stop()`
        is called.'''

        loop = asyncio.get_event_loop()

        while self.running and not self.task.cancelled():
            Log.debug('executing periodic task %s', self.name)
            before = loop.time()
            await self.run()
            total = loop.time() - before
            Log.debug('finished periodic task %s in %d seconds',
                      self.name, total)

            sleep = self.INTERVAL - total
            if sleep > 0:
                await self.sleep(sleep)


class TimerTask(Task):
    '''Run a method on the asyncio event loop exactly once after `DELAY`
    seconds.  Calling the `reset()` method will postpone execution, or re-queue
    execution if the timer has already completed.'''

    DELAY = 60.0

    async def run_task(self) -> None:
        '''Execute the task inside the asyncio event loop after `DELAY`
        seconds.  Track the time it takes to run, and log when it starts/stops.
        If/when `reset()` is called, reset the wait time to `DELAY` seconds.'''

        loop = asyncio.get_event_loop()

        self.loop = loop
        self.last_run = 0.0
        self.target = loop.time() + self.DELAY

        while self.running and not self.task.cancelled():
            now = loop.time()

            if now < self.target:
                sleep = self.target - now
                Log.debug('waiting %d seconds before executing task %s',
                          sleep, self.name)
                await asyncio.sleep(sleep)

            elif self.last_run < self.target:
                Log.debug('executing timer task %s', self.name)
                before = loop.time()
                self.last_run = before
                await self.run()
                total = loop.time() - before
                Log.debug('finished timer task %s in %d seconds',
                          self.name, total)

            else:
                sleep = min(5.0, self.DELAY)
                Log.debug('sleeping timer task %s for %d seconds',
                          self.name, sleep)
                await asyncio.sleep(sleep)

    def reset(self) -> None:
        '''Reset task execution to `DELAY` seconds from now.'''

        self.target = self.loop.time() + self.DELAY
