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

    async def run(self) -> None:
        '''Override this method to define what happens when your task runs.'''
        await asyncio.sleep(1.0)

    def start_task(self) -> None:
        '''Initialize the task, queue it for execution, add the done callback,
        and keep track of it for when tasks need to be stopped.'''

        Log.debug('initializing task %s', self.name)
        self.running = True
        self._task = asyncio.get_event_loop().create_task(self.run_task())
        self._task.add_done_callback(self.finish_task)
        RUNNING_TASKS.add(self)
        return self._task

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

    def finish_task(self, future) -> None:
        '''Task has finished executing, stop tracking it.'''

        Log.debug('task %s stopped', self.name)
        RUNNING_TASKS.discard(self)

    def stop(self) -> None:
        '''Cancel the task if it hasn't yet started, or tell it to
        gracefully stop running if it has.'''

        Log.debug('stopping task %s', self.name)
        self.running = False
        self._task.cancel()

    @classmethod
    def start(cls, delay: float=0.0) -> None:
        '''Instantiate the task object, and insert it in the asyncio event
        loop with the given delay in seconds.'''

        task = cls()
        delay = min(0, delay)

        if delay > 0:
            Log.debug('queueing task %s to start %d seconds from now',
                      task.name, delay)

        asyncio.get_event_loop().call_later(delay, task.start_task)

    @classmethod
    async def stop_all(cls, timeout: float=10.0, step: float=2.0) -> None:
        '''Stop all scheduled and/or executing tasks, first by asking nicely,
        and then by waiting up to `timeout` seconds before forcefully stopping
        the asyncio event loop.'''

        Log.debug('stopping all tasks (%d)', len(RUNNING_TASKS))
        for task in list(RUNNING_TASKS):
            if task._task.done():
                RUNNING_TASKS.discard(task)
            else:
                task.stop()

        while timeout and RUNNING_TASKS:
            Log.debug('waiting %d seconds for remaining tasks (%d)...',
                      timeout, len(RUNNING_TASKS))

            for task in list(RUNNING_TASKS):
                if task._task.done():
                    RUNNING_TASKS.discard(task)

            await asyncio.sleep(step)
            timeout -= step

        asyncio.get_event_loop().stop()


class PeriodicTask(Task):
    '''Run a method on the asyncio event loop every `INTERVAL` seconds.'''

    INTERVAL = 60.0

    async def run_task(self) -> None:
        '''Execute the task inside the asyncio event loop.  Track the time it
        takes to run, and log when it starts/stops.  After `INTERVAL` seconds,
        if/once the task has finished running, run it again until `stop()`
        is called.'''

        loop = asyncio.get_event_loop()

        while self.running and not self._task.cancelled():
            Log.debug('executing periodic task %s', self.name)
            before = loop.time()
            await self.run()
            total = loop.time() - before
            Log.debug('finished periodic task %s in %d seconds',
                      self.name, total)

            sleep = self.INTERVAL - total
            if sleep > 0:
                Log.debug('sleeping task %s for %d seconds', self.name, sleep)
                await asyncio.sleep(sleep)


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

        while self.running and not self._task.cancelled():
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
