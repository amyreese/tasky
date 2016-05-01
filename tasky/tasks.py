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

        value = self.tasky.loop.time()
        Log.debug('current time is %.1f', value)
        return value

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

        delay = kwargs.pop('delay', 0.0)
        await self.sleep(delay)

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
