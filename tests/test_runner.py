"""
Tests for the runner submodule.
"""
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
    from arbiter.runner import run_tasks, Result
    from arbiter.task import create_task

    def function(fail=False, exception=Exception, **kwargs):
        """
        A function for testing run_tasks.
        """
        if fail:
            raise exception()

        return kwargs

    # no dependencies
    results = run_tasks(
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
        set(results.keys()),
        {'foo', 'bar'}
    )

    assert_equal(
        results['foo'],
        Result(True, {'bar': 'baz', 'lorem': 'ipsum'})
    )

    assert_false(results['bar'].success)
    assert_true(isinstance(results['bar'].value, ImportError))

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
    results = run_tasks(
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
        {name: result.value for name, result in results.items()
         if result.success},
        {
            'foo': {'bar': 'baz', 'lorem': 'ipsum'},
            'bar': {},
            'mf doom': {'mm': 'food'},
            'danger mouse': {},
            'danger doom': {}
        }
    )

    assert_equal(
        {name for name, result in results.items() if not result.success},
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

    assert_true(isinstance(results['baz'].value, ImportError))
    assert_equal(results['baz'].value.args, ())

    assert_true(isinstance(results['qux'].value, FailedDependencyError))
    assert_equal(results['qux'].value.args, ('baz',))

    assert_true(isinstance(results['bravo'].value, UnsatisfiedDependencyError))
    assert_equal(results['bravo'].value.args, ({'alpha'},))

    assert_true(
        isinstance(results['charlie'].value, UnsatisfiedDependencyError)
    )
    assert_equal(results['charlie'].value.args, ({'bravo'},))

    assert_true(
        isinstance(results['oroboros'].value, UnsatisfiedDependencyError)
    )
    assert_equal(results['oroboros'].value.args, ({'oroboros'},))

    assert_true(
        isinstance(results['travolta'].value, UnsatisfiedDependencyError)
    )
    assert_equal(results['travolta'].value.args, ({'cage'},))

    assert_true(isinstance(results['cage'].value, UnsatisfiedDependencyError))
    assert_equal(results['cage'].value.args, ({'travolta'},))

    assert_true(isinstance(results['bell'].value, UnsatisfiedDependencyError))
    assert_equal(results['bell'].value.args, ({'charlie'},))

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

    main_pid = getpid()

    # 1 thread
    results = run_tasks(
        [
            create_task('foo', function_call),
            create_task('bar', function_call),
            create_task('baz', function_call),
        ],
    )

    assert_equal(set(results.keys()), {'foo', 'bar', 'baz'})

    for result in results.values():
        assert_true(result.success)
        assert_equal(result.value[0], main_pid)
        assert_not_equal(result.value[1], 'MainThread')

    # 1 process
    results = run_tasks(
        [
            create_task('foo', function_call),
            create_task('bar', function_call),
            create_task('baz', function_call),
        ],
        processes=True
    )

    assert_equal(set(results.keys()), {'foo', 'bar', 'baz'})

    for result in results.values():
        assert_true(result.success)
        assert_not_equal(result.value[0], main_pid)
        assert_equal(result.value[1], 'MainThread')

    # multiple threads
    results = run_tasks(
        [
            create_task(num, function_call) for num in range(1000)
        ],
        max_workers=5,
    )

    assert_equal(len(results), 1000)

    pids = set()
    threads = set()
    for num in range(1000):
        result = results[num]
        assert_true(result.success)

        pids.add(result.value[0])
        threads.add(result.value[1])

    assert_equal(pids, {main_pid})
    assert_true('MainThread' not in threads)

    assert_true(1 < len(threads) <= 5)  # Eventually, replace with better test

    # multiple processes
    results = run_tasks(
        [
            create_task(num, function_call) for num in range(1000)
        ],
        max_workers=5,
        processes=True
    )

    assert_equal(len(results), 1000)

    pids = set()
    threads = set()
    for num in range(1000):
        result = results[num]
        assert_true(result.success)

        pids.add(result.value[0])
        threads.add(result.value[1])

    assert_true(main_pid not in pids)
    assert_equal(threads, {'MainThread'})

    assert_true(1 < len(pids) <= 5)  # Eventually, replace with better test
