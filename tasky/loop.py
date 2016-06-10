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
                 config: Config=Config,
                 stats: Stats=Stats,
                 executor: Executor=None,
                 monitor: bool=True,
                 debug: bool=False) -> None:
        '''Initialize Tasky and automatically start a list of tasks.
        One of the following methods must be called on the resulting objects
        to start the event loop: `run_forever()`, `run_until_complete()`, or
        `run_for_time()`.'''

        if uvloop:
            Log.debug('using uvloop event loop')
            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

        self.loop = asyncio.new_event_loop()
        self.loop.add_signal_handler(signal.SIGINT, self.sigint)
        self.loop.add_signal_handler(signal.SIGTERM, self.sigterm)
        self.loop.set_exception_handler(self.exception)
        asyncio.set_event_loop(self.loop)

        if debug:
            Log.debug('enabling asyncio debug mode')
            self.loop.set_debug(True)

        self.all_tasks = {}
        self.running_tasks = set()
        self.initial_tasks = list(task_list)

        self.configuration = config
        self.stats = stats
        self.executor = executor

        self.monitor = monitor
        self.terminate_on_finish = False
        self.stop_attempts = 0

    @property
    def config(self) -> Any:
        '''Return configuration data for the root service.'''

        return self.configuration.global_config()

    @property
    def counters(self) -> DictWrapper:
        '''Dict-like structure for tracking global stats.'''

        return self.stats.global_counter()

    def task(self, name_or_class: Any) -> Task:
        '''Return a running Task object matching the given name or class.'''

        if name_or_class in self.all_tasks:
            return self.all_tasks[name_or_class]

        try:
            return self.all_tasks.get(name_or_class.__class__.__name__, None)

        except AttributeError:
            return None

    async def init(self) -> None:
        '''Initialize configuration and start tasks.'''

        self.stats = await self.insert(self.stats)
        self.configuration = await self.insert(self.configuration)

        if not self.executor:
            try:
                max_workers = self.config.get('executor_workers')
            except Exception:
                max_workers = None

            self.executor = ThreadPoolExecutor(max_workers=max_workers)

        for task in self.initial_tasks:
            await self.insert(task)

        if self.monitor:
            self.monitor = asyncio.ensure_future(self.monitor_tasks())

        self.counters['alive_since'] = time.time()

    async def insert(self, task: Task) -> None:
        '''Insert the given task class into the Tasky event loop.'''

        if not isinstance(task, Task):
            task = task()

        if task.name not in self.all_tasks:
            task.tasky = self
            self.all_tasks[task.name] = task

            await task.init()

        elif task != self.all_tasks[task.name]:
            raise Exception('Duplicate task %s' % task.name)

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
        self.monitor = False
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

    def terminate(self, *, force: bool=False, timeout: float=30.0,
                  step: float=1.0) -> None:
        '''Stop all scheduled and/or executing tasks, first by asking nicely,
        and then by waiting up to `timeout` seconds before forcefully stopping
        the asyncio event loop.'''

        if isinstance(self.monitor, asyncio.Future):
            self.monitor.cancel()

        Log.debug('stopping tasks')
        for task in list(self.running_tasks):
            if task.task.done():
                Log.debug('task %s already stopped', task.name)
                self.running_tasks.discard(task)
            else:
                Log.debug('asking %s to stop', task.name)
                asyncio.ensure_future(task.stop(force=force))

        if timeout > 0 and self.running_tasks:
            Log.debug('waiting %.1f seconds for remaining tasks (%d)...',
                      timeout, len(self.running_tasks))

            timeout -= step
            fn = functools.partial(self.terminate, force=force,
                                   timeout=timeout, step=step)
            return self.loop.call_later(step, fn)

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

        if self.terminate_on_finish and not self.running_tasks:
            Log.debug('all tasks finished, terminating')
            self.terminate()

    async def monitor_tasks(self, interval: float=10.0) -> None:
        '''Monitor all known tasks for run state.  Ensure that enabled tasks
        are running, and that disabled tasks are stopped.  Should not be used
        with Tasky.run_until_complete().'''

        Log.debug('monitor running')
        while True:
            try:
                await asyncio.sleep(interval)

                for name, task in self.all_tasks.items():
                    if task.enabled:
                        if task not in self.running_tasks:
                            Log.debug('task %s enabled, restarting', task.name)
                            await self.insert(task)

                    else:
                        if task in self.running_tasks:
                            Log.debug('task %s disabled, stopping', task.name)
                            await task.stop()

            except CancelledError:
                Log.debug('monitor cancelled')
                return

            except Exception:
                Log.exception('monitoring exception')

    def exception(self, loop: asyncio.BaseEventLoop, context: dict) -> None:
        '''Log unhandled exceptions from anywhere in the event loop.'''

        Log.error('unhandled exception: %s', context['message'])
        if 'exception' in context:
            Log.error('  %s', context['exception'])

    def sigint(self) -> None:
        '''Handle the user pressing Ctrl-C by stopping tasks nicely at first,
        then forcibly upon further presses.'''

        if self.stop_attempts < 1:
            Log.info('gracefully stopping tasks')
            self.stop_attempts += 1
            self.terminate()

        elif self.stop_attempts < 2:
            Log.info('forcefully cancelling tasks')
            self.stop_attempts += 1
            self.terminate(force=True)

        else:
            Log.info('forcefully stopping event loop')
            self.loop.stop()

    def sigterm(self) -> None:
        '''Handle SIGTERM from the system by stopping tasks gracefully.
        Repeated signals will be ignored while waiting for tasks to finish.'''

        if self.stop_attempts < 1:
            Log.info('received SIGTERM, gracefully stopping tasks')
            self.stop_attempts += 1
            self.terminate()

        else:
            Log.info('received SIGTERM, bravely waiting for tasks')
