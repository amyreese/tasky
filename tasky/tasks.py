# Copyright 2016 John Reese
# Licensed under the MIT license

import asyncio
import logging

from typing import Any

Log = logging.getLogger('tasky')

RUNNING_TASKS = set()


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

        delay = self.kwargs.pop('delay', 0.0)
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


class QueueTask(Task):
    '''Run a method on the asyncio event loop for each item inserted into this
    task's work queue. Can use multiple "workers" to process the work queue.
    Failed work items (those generating exceptions) will be dropped -- workers
    must manually requeue any work items that need to be reprocessed.'''

    WORKERS = 1
    MAXSIZE = 0
    QUEUE = None
    OPEN = True

    def __init__(self, id: int=0):
        '''Initialize the shared work queue for all workers.'''

        super().__init__()

        if self.__class__.QUEUE is None:
            self.__class__.QUEUE = asyncio.Queue(self.MAXSIZE)

        self.id = max(0, id)

    @property
    def name(self):
        return '{0}({1})'.format(self.__class__.__name__, self.id)

    @classmethod
    def close(cls):
        '''Mark the queue as being "closed".  Once closed, workers will stop
        running once the work queue becomes empty.'''

        Log.debug('closing %s work queue', cls.__name__)
        cls.OPEN = False

    async def run(self, item: Any) -> None:
        '''Override this method to define what happens when your task runs.'''

        await self.sleep(1.0)

    async def run_task(self) -> None:
        '''Initialize the queue and spawn extra worker tasks if this if the
        first task.  Then wait for work items to enter the task queue, and
        execute the `run()` method with the current work item.'''

        if self.id == 0:
            for task_id in range(1, self.WORKERS):
                task = self.__class__(id=task_id)
                Log.debug('spawning %s', task.name)
                self.tasky.insert(task)

        while self.running and not self.task.cancelled():
            try:
                item = self.QUEUE.get_nowait()

                Log.debug('%s processing work item', self.name)
                await self.run(item)

                Log.debug('%s completed work item', self.name)
                self.QUEUE.task_done()

            except asyncio.QueueEmpty:
                if self.OPEN:
                    await self.sleep(0.05)

                else:
                    Log.debug('%s queue closed and empty, stopping', self.name)
                    return

            except:
                Log.exception('%s failed work item', self.name)
                self.QUEUE.task_done()
