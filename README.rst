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

To create a task::

    from arbiter import create_task

    task = create_task(name, function)

    # A task with dependencies
    dependent task = create_task(name, function, (dependency, dependency2))

To run tasks::

    from arbiter.sync import run_tasks

    results = run_tasks(tasks)

Tasks can be run asynchronously::

    from arbiter.async import run_tasks

    results = run_tasks(tasks, max_workers=5)

License
=======
Arbiter is provided under an MIT License.
