# Copyright 2016 John Reese
# Licensed under the MIT license

import asyncio
import logging

from typing import Any

from .task import Task

Log = logging.getLogger('tasky.tasks')


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

    async def init(self) -> None:
        if self.id == 0:
            Log.debug('initializing %s', self.name)

            for task_id in range(1, self.WORKERS):
                task = self.__class__(id=task_id)
                Log.debug('spawning %s', task.name)
                await self.tasky.insert(task)

    async def run(self, item: Any) -> None:
        '''Override this method to define what happens when your task runs.'''

        await self.sleep(1.0)

    async def run_task(self) -> None:
        '''Initialize the queue and spawn extra worker tasks if this if the
        first task.  Then wait for work items to enter the task queue, and
        execute the `run()` method with the current work item.'''

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
