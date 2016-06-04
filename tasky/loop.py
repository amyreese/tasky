# Copyright 2016 John Reese
# Licensed under the MIT license

import asyncio
import functools
import logging
import signal
import time

from concurrent.futures import CancelledError, Executor, ThreadPoolExecutor
from typing import Any, List

from .config import Config
from .tasks import Task
from .stats import Stats, DictWrapper

try:
    # if uvlib/uvloop is available, awesome!
    import uvloop

except ImportError:
    uvloop = None

Log = logging.getLogger('tasky')


class Tasky(object):
    '''Task management framework for asyncio'''

    def __init__(self,
                 task_list: List[Task]=None,
                 config: Config=None,
                 stats: Stats=None,
                 executor: Executor=None,
                 debug: bool=False) -> None:
        '''Initialize Tasky and automatically start a list of tasks.
        One of the following methods must be called on the resulting objects
        to start the event loop: `run_forever()`, `run_until_complete()`, or
        `run_for_time()`.'''

        if uvloop:
            Log.debug('using uvloop event loop')
            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

        self.loop = asyncio.new_event_loop()
        self.loop.add_signal_handler(signal.SIGINT, self.ctrlc)
        self.loop.set_exception_handler(self.exception)
        asyncio.set_event_loop(self.loop)

        if debug:
            Log.debug('enabling asyncio debug mode')
            self.loop.set_debug(True)

        self.running_tasks = set()
        self.terminate_on_finish = False
        self.stop_attempts = 0

        self.task_list = task_list
        self.executor = executor

        if not stats:
            stats = Stats()
        self.stats = stats

        if not config:
            config = Config()
        self._config = config

    @property
    def config(self) -> Config:
        '''Return configuration data for the root service.'''

        return self._config

    @property
    def counters(self) -> DictWrapper:
        '''Dict-like structure for tracking global stats.'''

        return self.stats.global_counter()

    def task(self, name_or_class: Any) -> Task:
        '''Return a running Task object matching the given name or class.'''

        for task in self.running_tasks:
            if task.name == name_or_class or task.__class__ == name_or_class:
                return task

        return None

    async def init(self) -> None:
        '''Initialize configuration and start tasks.'''

        self._config = await self.insert(self._config)
        self.stats = await self.insert(self.stats)

        if not self.executor:
            max_workers = self.config.get('executor_workers')
            self.executor = ThreadPoolExecutor(max_workers=max_workers)

        if self.task_list:
            for task in self.task_list:
                await self.insert(task)

        self.counters['alive_since'] = time.time()

    async def insert(self, task: Task) -> None:
        '''Insert the given task class into the Tasky event loop.'''

        if not isinstance(task, Task):
            task = task()

        task.tasky = self
        await task.init()

        task.task = asyncio.ensure_future(self.start_task(task))
        self.running_tasks.add(task)

        return task

    async def execute(self, fn, *args, **kwargs) -> None:
        '''Execute an arbitrary function outside the event loop using
        a shared Executor.'''

        fn = functools.partial(fn, *args, **kwargs)
        return await self.loop.run_in_executor(self.executor, fn)

    def run_forever(self) -> None:
        '''Execute the tasky/asyncio event loop until terminated.'''

        Log.debug('running event loop until terminated')
        asyncio.ensure_future(self.init())
        self.loop.run_forever()
        self.loop.close()

    def run_until_complete(self) -> None:
        '''Execute the tasky/asyncio event loop until all tasks finish.'''

        Log.debug('running event loop until all tasks completed')
        self.terminate_on_finish = True
        asyncio.ensure_future(self.init())
        self.loop.run_forever()
        self.loop.close()

    def run_for_time(self, duration: float=10.0) -> None:
        '''Execute the tasky/asyncio event loop for `duration` seconds.'''

        Log.debug('running event loop for %.1f seconds', duration)
        try:
            asyncio.ensure_future(self.init())
            self.loop.run_until_complete(asyncio.sleep(duration))
            self.terminate()
            self.loop.run_forever()

        except RuntimeError as e:
            if not e.args[0].startswith('Event loop stopped'):
                raise

        finally:
            self.loop.close()

    def terminate(self, timeout: float=30.0, step: float=1.0) -> None:
        '''Stop all scheduled and/or executing tasks, first by asking nicely,
        and then by waiting up to `timeout` seconds before forcefully stopping
        the asyncio event loop.'''

        Log.debug('stopping tasks')
        for task in list(self.running_tasks):
            if task.task.done():
                Log.debug('task %s already stopped', task.name)
                self.running_tasks.discard(task)
            else:
                Log.debug('asking %s to stop', task.name)
                asyncio.ensure_future(task.stop())

        if timeout > 0 and self.running_tasks:
            Log.debug('waiting %.1f seconds for remaining tasks (%d)...',
                      timeout, len(self.running_tasks))

            timeout -= step
            return self.loop.call_later(step, self.terminate, timeout, step)

        if timeout > 0:
            Log.debug('all tasks completed, stopping event loop')
        else:
            Log.debug('timed out waiting for tasks, stopping event loop')

        self.loop.stop()

    async def start_task(self, task: Task) -> None:
        '''Initialize the task, queue it for execution, add the done callback,
        and keep track of it for when tasks need to be stopped.'''

        try:
            Log.debug('task %s starting', task.name)
            before = time.time()
            task.counters['last_run'] = before

            self.running_tasks.add(task)
            await task.run_task()
            Log.debug('task %s completed', task.name)

        except CancelledError:
            Log.debug('task %s cancelled', task.name)

        except Exception:
            Log.exception('unhandled exception in task %s', task.name)

        finally:
            self.running_tasks.discard(task)

            after = time.time()
            total = after - before
            task.counters['last_completed'] = after
            task.counters['duration'] = total

        if self.terminate_on_finish:
            if not self.running_tasks:
                Log.debug('all tasks finished, terminating')
                self.terminate()

    def exception(self, loop: asyncio.BaseEventLoop, context: dict) -> None:
        '''Log unhandled exceptions from anywhere in the event loop.'''

        Log.error('unhandled exception: %s', context['exception'])

    def ctrlc(self) -> None:
        '''Handle the user pressing Ctrl-C by stopping tasks nicely at first,
        then forcibly upon further presses.'''

        if self.stop_attempts < 1:
            Log.info('stopping main loop')
            self.stop_attempts += 1
            self.terminate()
        else:
            Log.info('force stopping event loop')
            self.loop.stop()
