=======
Arbiter
=======
Arbiter is a 2.7, 3.3+ compatible concurrent task runner that automatically
handles resolving dependency between tasks::

    from arbiter import create_task, run_tasks

    results = run_tasks(
        [
            create_task('foo', foo_function, args=('bar', 'baz')),
            create_task('lorem', lorem_function, dependencies=['foo']),
            create_task('ipsum', ipsum_function, dependencies=['foo']),
        ]
    )


Installation
============
Arbiter is not yet on PyPI, so it has to be installed manually.

Prerequisites
-------------
The task-runner is implemented using `concurrent.futures`. If running on
Python 2, the backport from python 3 will need to be installed::

    $ pip install futures

Manual Installation
-------------------
To install manually::

    $ git clone https://github.com/invenia/Arbiter
    $ cd Arbiter
    $ python setup.py install

Usage
=====
Tasks
-----
Tasks can be created using the `create_task` function::

    from arbiter import create_task

    task = create_task('name', function)

Create takes a name (which must be unique among tasks) and a function
representing the task. The function will be considered successful if it runs
without raising an exception. Arguments can be passed to the function using the
`args` and `kwargs` arguments::

    create_task('name', function, args=('foo', 'bar'), kwargs={'baz': 'qux'})

Any tasks that need to run to completion prior to your task, specify them using
the `dependencies` argument::

    create_task('bar', function, dependencies=['foo'])

Any number of dependencies may be supplied.

It's quite possible that your task might require multiple tries to succeed. To
automatically force a task to retry using the `retries` and `delay` arguments::

    create_task('failable', sketchy_function, replies=3, delay=timedelta(seconds=5))

If `delay` isn't given, the function will retry immediately.

Running Tasks
-------------

The `run_tasks` function will run a list of tasks, handling their
dependencies::

    from arbiter import run_tasks

    results = run_tasks(task_list)

By default, run_tasks will run the tasks with one worker thread. workers can be
increased using the `max_workers` argument, and `processes=True` will run the
tasks using working processes instead of threads (NOTE: some tasks may only run
using threads).

`run_tasks` returns a dict of `Result` objects. `result.success` is a boolean
value signifying whether the task completed. If successful, `result.value` will
contain the task's return value. If the task failed, `result.value` will
contain the exception that was thrown that caused the task to fail.

Two custom exceptions exist to signify errors that prevented the task from
running at all. `arbiter.exceptions.FailedDependencyError` represents a task
that had a dependency that failed.
`arbiter.exceptions.UnsatisfiedDependencyError` represents a task that had a
dependency that doesn't exist, or can't be run because of a circular
dependency.

As long as task functions are guaranteed to eventually complete or fail,
Arbiter will eventually complete.

Forthcoming features
====================
Features that will, eventually, be in arbiter:

 * timeouts for running tasks

 * passing results from dependent tasks

 * open sourcing

 * availability on PyPI

License
=======
Arbiter is not yet provided under an open-source license
