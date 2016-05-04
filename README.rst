Tasky
=====

Python asyncio framework for task-based execution.

.. image:: https://travis-ci.org/jreese/tasky.svg?branch=master
    :target: https://travis-ci.org/jreese/tasky


Overview
--------

Tasky provides a framework for using Python's asyncio module to encapsulate
execution of your program or service as a set of distinct "tasks" with a
variety of execution styles, including "periodic" tasks, timers, and more.

Tasks are defined by subclassing the appropriate type and implementing a
``run()`` method.  Tasky runs tasks on the asyncio event loop, but also keeps
track of which tasks are running, and can terminate automatically when all
tasks are completed or after a predetermined amount of time.


Usage
-----

The simplest type of task executes the ``run()`` once and then completes.
Hello World in Tasky can be accomplished with the following code::

    class HelloWorld(Task):
        async def run(self):
            print('Hello world!')

    Tasky([HelloWorld]).run_until_complete()

Note the use of ``async def``.  All tasks are coroutines, meaning they have
full access to the asyncio event loop.

Another common pattern is to execute code every X number of seconds, a
"periodic" task similar to a cron job.  In Tasky, this is possible by
subclassing ``PeriodicTask`` and defining your job ``INTERVAL``::

    class Counter(PeriodicTask):
        INTERVAL = 1.0

        value = 0

        async def run(self):
            value += 1
            print(self.value)

    Tasky([Counter]).run_for_time(10)

Note the use of ``run_for_time()``.  This will gracefully stop the Tasky
event loop after the given number of seconds have passed.  The periodic task
will automatically stop running, giving us the expected output of counting
to ten.

The third type of common task is a timer.  The ``run()`` method is only
executed once after a defined delay.  If the timer is reset after execution
completes, then the timer will be executed again.  Otherwise, resets simply
increase the time before execution back to the originally defined delay::

    class Celebrate(TimerTask):
        DELAY = 10

        async def run(self):
            print('Surprise!')

    Tasky([Counter, Celebrate]).run_for_time(10)

Note that we're now starting multiple tasks as once.  The counter output from
the previous example is accompanied by the message "Surprise!" at the end.

The last major task is a queue consumer.  A shared work queue is created for
one or more worker tasks of the same class, and the ``run()`` method is then
called for every work item popped from the queue.  Any task can insert work
items directly from the class definition, or call ``QueueTask.close()`` to
signal that workers should stop once the shared work queue becomes empty::

    class QueueConsumer(QueueTask):
        WORKERS = 2
        MAXSIZE = 5

        async def run(self, item):
            print('consumer got {}'.format(item))
            await self.sleep(0.1)

    class QueueProducer(Task):
        async def run(self):
            for i in range(10):
                item = random.randint(0, 100)
                await QueueConsumer.QUEUE.put(item)
                print('producer put {}'.format(i))
            QueueConsumer.close()

    Tasky([QueueConsumer, QueueProducer]).run_until_complete()

Note that if work items need to be reprocessed, they should be manually
inserted back into the shared queue by the worker.


Install
-------

Tasky depends on syntax changes introduced in Python 3.5.
You can install it from PyPI with the following command::

    $ pip install tasky


License
-------

Copyright 2016 John Reese, and licensed under the MIT license.
See the ``LICENSE`` file for details.
