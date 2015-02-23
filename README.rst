=======
Arbiter
=======
.. image:: https://travis-ci.org/invenia/Arbiter.svg?branch=master
  :target: https://travis-ci.org/invenia/Arbiter?branch=master
.. image:: https://coveralls.io/repos/invenia/Arbiter/badge.png?branch=master
  :target: https://coveralls.io/r/invenia/Arbiter?branch=master

Arbiter is a 2.6+, 3.3+ compatible task-dependency solver. Given a set of
tasks and their dependencies, arbiter calculates what tasks are currently
runnable::

    from arbiter import Arbiter

    arbiter = Arbiter(
        tasks={
            'foo': None,
            'bar': ['foo'],
            'alpha': None,
            'beta': ['alpha'],
            'bravo': ['alpha'],
            'lorem': ['foo', 'alpha'],
        }
    )

    arbiter.runnable  # {'foo', 'alpha'}

    arbiter.start_task('alpha')
    arbiter.end_task('alpha', success=True)

    arbiter.runnable  # {'foo', 'beta', 'bravo'}

Arbiter is not a task-runner (there are already enough frameworks for
concurrently running tasks), it is a solver that can be used in conjunction
with an existing task scheduler in order to run your tasks in an efficient
manner.

Installation
============
Arbiter is available on PyPI. To install::

    $ pip install arbiter

Manual Installation
-------------------
To install manually::

    $ git clone https://github.com/invenia/Arbiter
    $ cd Arbiter
    $ python setup.py install


License
=======
Arbiter is provided under an MIT License.
