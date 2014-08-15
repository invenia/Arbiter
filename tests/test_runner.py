"""
Tests for the runner submodule.
"""
from concurrent.futures import Future
from os import getpid
from threading import current_thread

from nose.tools import (assert_true, assert_false, assert_equal,
                        assert_not_equal, assert_raises)


def function_call(fail=False):
    """
    A multiprocessing function.
    """
    if fail:
        raise Exception()

    return (getpid(), current_thread().name)


def test_run_tasks():
    """
    Test the run_tasks function.
    """
    from arbiter.exceptions import (
        FailedDependencyError,
        UnsatisfiedDependencyError,
    )
    from arbiter.runner import run_tasks
    from arbiter.task import create_task

    def function(fail=False, exception=Exception, **kwargs):
        """
        A function for testing run_tasks.
        """
        if fail:
            raise exception()

        return kwargs

    # no dependencies
    completed, failed = run_tasks(
        [
            create_task(
                'foo',
                function,
                kwargs={'bar': 'baz', 'lorem': 'ipsum'},
            ),
            create_task(
                'bar',
                function,
                args=(True, ImportError),
            ),
        ],
    )

    assert_equal(
        completed,
        {
            'foo': {'bar': 'baz', 'lorem': 'ipsum'},
        }
    )

    assert_equal(set(failed.keys()), {'bar'})
    assert_true(isinstance(failed['bar'], ImportError))

    # die on bad exceptions
    with assert_raises(KeyboardInterrupt):
        run_tasks(
            [
                create_task('foo', function, args=(True, KeyboardInterrupt))
            ]
        )

    with assert_raises(SystemExit):
        run_tasks(
            [
                create_task('foo', function, args=(True, SystemExit))
            ]
        )

    # dependencies and failures
    # Things this tests:
    #  - tasks with no dependencies
    #  - tasks with a successful dependency
    #  - tasks with unsuccessful dependency
    #  - tasks dependent on non-existent tasks
    #  - tasks dependent on themselves
    #  - tasks dependent in a chain
    #  - tasks with multiple dependencies
    #  - out-of-order tasks
    completed, failed = run_tasks(
        [
            create_task(
                'bar',
                function,
                dependencies=['foo']
            ),
            create_task(
                'baz',
                function,
                args=(True, ImportError),
                dependencies=['bar']
            ),
            create_task(
                'bell',
                function,
                dependencies=['bar', 'charlie']
            ),
            create_task(
                'bravo',
                function,
                dependencies=['alpha']
            ),
            create_task(
                'cage',
                function,
                dependencies=['travolta']
            ),
            create_task(
                'charlie',
                function,
                dependencies=['bravo']
            ),
            create_task(
                'danger doom',
                function,
                dependencies=['mf doom', 'danger mouse']
            ),
            create_task(
                'danger mouse',
                function,
            ),
            create_task(
                'foo',
                function,
                kwargs={'bar': 'baz', 'lorem': 'ipsum'},
            ),
            create_task(
                'mf doom',
                function,
                kwargs={'mm': 'food'},
            ),
            create_task(
                'oroboros',
                function,
                dependencies=['oroboros']
            ),
            create_task(
                'qux',
                function,
                dependencies=['baz']
            ),
            create_task(
                'travolta',
                function,
                dependencies=['cage']
            ),
        ],
    )

    assert_equal(
        completed,
        {
            'foo': {'bar': 'baz', 'lorem': 'ipsum'},
            'bar': {},
            'mf doom': {'mm': 'food'},
            'danger mouse': {},
            'danger doom': {}
        }
    )

    assert_equal(
        set(failed.keys()),
        {
            'baz',
            'qux',
            'bravo',
            'charlie',
            'oroboros',
            'travolta',
            'cage',
            'bell',
        }
    )

    assert_true(isinstance(failed['baz'], ImportError))
    assert_equal(failed['baz'].args, ())

    assert_true(isinstance(failed['qux'], FailedDependencyError))
    assert_equal(failed['qux'].args, ('baz',))

    assert_true(isinstance(failed['bravo'], UnsatisfiedDependencyError))
    assert_equal(failed['bravo'].args, ({'alpha'},))

    assert_true(isinstance(failed['charlie'], UnsatisfiedDependencyError))
    assert_equal(failed['charlie'].args, ({'bravo'},))

    assert_true(isinstance(failed['oroboros'], UnsatisfiedDependencyError))
    assert_equal(failed['oroboros'].args, ({'oroboros'},))

    assert_true(isinstance(failed['travolta'], UnsatisfiedDependencyError))
    assert_equal(failed['travolta'].args, ({'cage'},))

    assert_true(isinstance(failed['cage'], UnsatisfiedDependencyError))
    assert_equal(failed['cage'].args, ({'travolta'},))

    assert_true(isinstance(failed['bell'], UnsatisfiedDependencyError))
    assert_equal(failed['bell'].args, ({'charlie'},))

    # duplicate name
    with assert_raises(TypeError):
        run_tasks(
            [
                create_task('foo', function),
                create_task('foo', function),
            ]
        )


def test_run_executor():
    """
    Test that run_tasks works with both threads and processes.
    """
    from arbiter.runner import run_tasks
    from arbiter.task import create_task

    pid = getpid()

    # 1 thread
    completed, failed = run_tasks(
        [
            create_task('foo', function_call),
            create_task('bar', function_call),
            create_task('baz', function_call),
        ],
    )

    assert_equal(set(completed.keys()), {'foo', 'bar', 'baz'})

    for name in {'foo', 'bar', 'baz'}:
        assert_equal(completed[name][0], pid)
        assert_not_equal(completed[name][1], 'MainThread')

    # 1 process
    completed, failed = run_tasks(
        [
            create_task('foo', function_call),
            create_task('bar', function_call),
            create_task('baz', function_call),
        ],
        processes=True
    )

    assert_equal(set(completed.keys()), {'foo', 'bar', 'baz'})

    main_pid = getpid()

    for name in {'foo', 'bar', 'baz'}:
        assert_not_equal(completed[name][0], main_pid)
        assert_equal(completed[name][1], 'MainThread')

    # multiple threads
    completed, failed = run_tasks(
        [
            create_task(num, function_call) for num in range(1000)
        ],
        max_workers=5,
    )

    assert_equal(set(completed.keys()), {num for num in range(1000)})

    pids = set()
    threads = set()
    for pid, thread in completed.values():
        pids.add(pid)
        threads.add(thread)

    assert_equal(pids, {main_pid})

    assert_true('MainThread' not in threads)
    assert_true(1 < len(threads) <= 5)  # Eventually, replace with better test

    # multiple processes
    completed, failed = run_tasks(
        [
            create_task(num, function_call) for num in range(1000)
        ],
        max_workers=5,
        processes=True
    )

    assert_equal(set(completed.keys()), {num for num in range(1000)})

    pids = set()
    threads = set()
    for pid, thread in completed.values():
        pids.add(pid)
        threads.add(thread)

    assert_true(main_pid not in pids)
    assert_true(1 < len(pids) <= 5)  # Eventually, replace with better test

    assert_equal(threads, {'MainThread'})
