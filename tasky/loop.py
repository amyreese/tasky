# Copyright 2016 John Reese
# Licensed under the MIT license

import asyncio
import functools
import logging
import signal

from typing import List

from .tasks import Task, OneShotTask

Log = logging.getLogger('tasky')


class Tasky(object):
    '''Task management framework for asyncio'''

    def __init__(self, klass_list: List[Task]=None) -> None:
        '''Initialize Tasky and automatically start a list of tasks.
        One of the following methods must be called on the resulting objects
        to start the event loop: `run_forever()`, `run_until_complete()`, or
        `run_for_time()`.'''

        self.loop = asyncio.new_event_loop()
        self.loop.add_signal_handler(signal.SIGINT, self.ctrlc)
        asyncio.set_event_loop(self.loop)

        self.running_tasks = set()
        self.terminate_on_finish = False
        self.stop_attempts = 0

        if klass_list:
            for klass in klass_list:
                self.insert(klass)

    def insert(self, klass: Task, delay: float=0.0, *args, **kwargs) -> None:
        '''Insert the given task class into the Tasky event loop, with an
        optional delay in seconds.'''

        task = klass(*args, **kwargs)
        task.tasky = self

        delay = min(0, delay)

        if delay > 0:
            Log.debug('queueing task %s to start %d seconds from now',
                      task.name, delay)

        self.loop.call_later(delay, self.start_task, task)

    def execute(self, fn, delay: float=0.0, *args, **kwargs) -> None:
        '''Execute an arbitrary function or coroutine on the Tasky event loop,
        with an optional delay in seconds.'''

        self.insert(OneShotTask, delay, fn, *args, **kwargs)

    def run_forever(self) -> None:
        '''Execute the tasky/asyncio event loop until terminated.'''

        Log.debug('running event loop until terminated')
        self.loop.run_forever()
        self.loop.close()

    def run_until_complete(self, interval: float=2.0) -> None:
        '''Execute the tasky/asyncio event loop until all tasks finish.'''

        Log.debug('running event loop until all tasks completed')
        self.terminate_on_finish = True
        self.loop.run_forever()
        self.loop.close()

    def run_for_time(self, duration: float=10.0) -> None:
        '''Execute the tasky/asyncio event loop for `duration` seconds.'''

        async def sleepy():
            await asyncio.sleep(duration)

        Log.debug('running event loop for %d seconds', duration)
        try:
            self.loop.run_until_complete(sleepy())
            self.terminate()
            self.loop.run_forever()

        except RuntimeError as e:
            if not e.args[0].startswith('Event loop stopped'):
                raise

        finally:
            self.loop.close()

    def terminate(self, timeout: float=30.0, step: float=2.0) -> None:
        '''Stop all scheduled and/or executing tasks, first by asking nicely,
        and then by waiting up to `timeout` seconds before forcefully stopping
        the asyncio event loop.'''

        for task in list(self.running_tasks):
            if task.task.done():
                asyncio.wait([task.task])
                self.running_tasks.discard(task)
            else:
                task.stop()

        if timeout > 0 and self.running_tasks:
            Log.debug('waiting %d seconds for remaining tasks (%d)...',
                      timeout, len(self.running_tasks))

            timeout -= step
            return self.loop.call_later(step, self.terminate, timeout, step)

        if timeout > 0:
            Log.debug('all tasks completed, stopping event loop')
        else:
            Log.debug('timed out waiting for tasks, stopping event loop')

        self.loop.stop()

    def start_task(self, task: Task) -> None:
        '''Initialize the task, queue it for execution, add the done callback,
        and keep track of it for when tasks need to be stopped.'''

        Log.debug('initializing task %s', task.name)

        done_callback = functools.partial(self.finish_task, task)

        task.task = self.loop.create_task(task.run_task())
        task.task.add_done_callback(done_callback)

        self.running_tasks.add(task)

    def finish_task(self, task: Task, future: asyncio.Future) -> None:
        '''Task has finished executing, stop tracking it.'''

        Log.debug('task %s completed', task.name)
        asyncio.wait([task.task])
        self.running_tasks.discard(task)

        if self.terminate_on_finish:
            if not self.running_tasks:
                Log.debug('all tasks finished, terminating')
                self.terminate()

    def ctrlc(self) -> None:
        if self.stop_attempts < 1:
            Log.info('stopping main loop')
            self.stop_attempts += 1
            self.terminate()
        else:
            Log.info('force stopping event loop')
            self.loop.stop()
