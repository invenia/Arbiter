=======
Arbiter
=======
.. image:: https://travis-ci.org/invenia/Arbiter.svg?branch=master
  :target: https://travis-ci.org/invenia/Arbiter?branch=master
.. image:: https://coveralls.io/repos/invenia/Arbiter/badge.png?branch=master
  :target: https://coveralls.io/r/invenia/Arbiter?branch=master

Arbiter is a task-scheduler that resolves task dependencies. Given a set of
tasks and their dependencies, Arbiter will run the tasks such that no task is
run before a dependency has already successfully run.

Installation
============
Arbiter is available on PyPI. To install::

    $ pip install arbiter

Usage
=====

Creating Tasks
--------------

A task must take a function. All other positional and keyword arguemnts (except 'name', 'handler' and 'dependencies') 
will be passed through to the function at execution time.::

    from arbiter import create_task

    # A task
    task = create_task(len, [1, 2, 3])


Naming a task. NOTE: If a name isn't provided a unique name will be generated with the function name and a uuid.::

    # A task with a name
    named_task = create_task(len, [1, 2, 3], name='foo')


Passing tasks as arguments. When a positional or keyword argument is a task, 
the return value from that task will be passed as a parameter to your function.::

    # A task with dependent data.
    dependent_task = create_task(print, named_task)


Defining arbitrary dependencies. If you want a task to run after other ones without dependending on 
their return values just provide a tuple of their names to `create_task`.::

    # A task with arbitrary dependencies.
    dependent_task2 = create_task(myfunc, name='car', dependencies=('foo', 'bar'))



Running Tasks
--------------

To run tasks::

    from arbiter.sync import run_tasks

    results = run_tasks(tasks)


Tasks can also be run asynchronously::

    from arbiter.async import run_tasks

    results = run_tasks(tasks, max_workers=5)



Retrying Tasks
---------------

Define your own retry conditions::
    
    from arbiter.utils import RetryCondition()

    def retry_on_value_error(exc):
        if isinstance(exc, ValueError) and exc.args[0] == 'do_retry':
            return True
        else:
            return False

    cond = RetryCondition(retry_on_value_error)


Pass a retry handler to a task.::
    
    from datetime import timedelta

    from arbiter.utils import retry_handler

    retry_task = create_task(
        myfunc,
        handler=retry_handler(
            retries=5,
            delay=timedelta(10),
            conditions=[cond]
        )
    )


NOTE: you can also define your own handlers to pass to `create_task`, so long as the 
handler is a function that takes the task function as its only argument.


Alternatively, you can decorate your task functions ahead of time.::

    from datetime import timedelta

    from arbieter.utils import retry

    @retry(retries=5, delay=timedelta=(10), conditions=[cond])
    def myfunc():
        ...



License
=======
Arbiter is provided under an MIT License.
