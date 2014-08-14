"""
Tests for the runner submodule.
"""
from concurrent.futures import Future
from os import getpid
from threading import current_thread

from nose.tools import (assert_true, assert_false, assert_equal,
                        assert_not_equal, assert_raises)

RESULTS = {}
def procable_call(name, succeed=True):
    """
    Add a name to the call stack.
    """
    RESULTS[name] = (getpid(), current_thread().name)

    return succeed


def test_task_runner():
    """
    Test the task_runner function
    """
    from arbiter.runner import task_runner

    def function(should_raise=False, exception=Exception, **kwargs):
        """
        A function for testing when_ready
        """
        if should_raise:
            raise exception('foo')

        return kwargs

    # No dependencies
    assert_equal(
        task_runner([], function, (False,), {'foo': 'bar', 'lorem': 'ipsum'}),
        {'foo': 'bar', 'lorem': 'ipsum'}
    )

    # make sure exceptions don't fall through
    assert_false(
        task_runner([], function, (True,), {'foo': 'bar', 'lorem': 'ipsum'}),
        {'foo': 'bar', 'lorem': 'ipsum'}
    )

    # make sure this isn't true for KeyboardInterrupt
    with assert_raises(KeyboardInterrupt):
        task_runner([], function, (True, KeyboardInterrupt))

    with assert_raises(SystemExit):
        task_runner([], function, (True, SystemExit))

    # Failed dependency
    futures = []
    for result in (True, True, True, False, True):
        future = Future()
        future.set_result(result)
        futures.append(future)

    assert_false(task_runner(futures, function, (False,),
                             {'foo': 'bar', 'lorem': 'ipsum'}))
    assert_false(task_runner(futures, function, (True,),
                             {'foo': 'bar', 'lorem': 'ipsum'}))
    assert_false(task_runner(futures, function, (True, KeyboardInterrupt)))
    assert_false(task_runner(futures, function, (True, SystemExit)))

    # All successful dependencies
    futures = []
    for result in (True, True, True, True):
        future = Future()
        future.set_result(result)
        futures.append(future)

    assert_equal(
        task_runner(
            futures, function, (False,), {'foo': 'bar', 'lorem': 'ipsum'}
        ),
        {'foo': 'bar', 'lorem': 'ipsum'}
    )
    assert_false(
        task_runner(
            futures, function, (True,), {'foo': 'bar', 'lorem': 'ipsum'}
        )
    )

    with assert_raises(KeyboardInterrupt):
        task_runner([], function, (True, KeyboardInterrupt))

    with assert_raises(SystemExit):
        task_runner([], function, (True, SystemExit))


def test_run_tasks():
    """
    Test the run_tasks function.
    """
    from arbiter.runner import run_tasks
    from arbiter.task import create_task

    call_stack = []
    def call(name, succeed=True):
        """
        Add a name to the call stack.
        """
        call_stack.append(name)

        return succeed

    # all successful
    completed, failed = run_tasks([
        create_task('foo', call, args=('foo',)),
        create_task('bar', call, dependencies=['foo'], args=('bar',)),
        create_task('baz', call, dependencies=['bar'], args=('baz',)),
        create_task('lorem', call, args=('lorem',)),
        create_task('ipsum', call, dependencies=['lorem'], args=('ipsum',)),
    ])

    assert_equal(
        completed,
        {'foo', 'bar', 'baz', 'lorem', 'ipsum'}
    )

    assert_equal(failed, set())

    assert_equal(
        set(call_stack),
        {'foo', 'bar', 'baz', 'lorem', 'ipsum'}
    )

    assert_true(call_stack.index('foo') < call_stack.index('bar'))
    assert_true(call_stack.index('bar') < call_stack.index('baz'))
    assert_true(call_stack.index('lorem') < call_stack.index('ipsum'))

    # some failures
    call_stack = []
    completed, failed = run_tasks([
        create_task('foo', call, args=('foo',)),
        create_task('bar', call, dependencies=['foo'], args=('bar', False)),
        create_task('baz', call, dependencies=['bar'], args=('baz',)),
        create_task('lorem', call, args=('lorem', False)),
        create_task('ipsum', call, dependencies=['lorem'], args=('ipsum',)),
    ])

    assert_equal(completed, {'foo'})

    assert_equal(failed, {'bar', 'baz', 'lorem', 'ipsum'})

    assert_equal(
        set(call_stack),
        {'foo', 'bar', 'lorem'}
    )

    assert_true(call_stack.index('foo') < call_stack.index('bar'))

    # unrunnable tests & out of order
    call_stack = []
    completed, failed = run_tasks([
        create_task('ipsum', call, dependencies=['lorem'], args=('ipsum',)),
        create_task('bar', call, dependencies=['foo'], args=('bar',)),
        create_task('foo', call, args=('foo',)),
        create_task('baz', call, dependencies=['bar'], args=('baz',)),
        create_task('lorem', call, args=('lorem', False)),
        create_task('bravo', call, dependencies=['alpha'], args=('bravo',)),
    ])

    assert_equal(completed, {'foo', 'bar', 'baz'})

    assert_equal(failed, {'lorem', 'ipsum', 'bravo'})

    assert_equal(
        set(call_stack),
        {'foo', 'bar', 'baz', 'lorem'}
    )

    assert_true(call_stack.index('foo') < call_stack.index('bar'))
    assert_true(call_stack.index('bar') < call_stack.index('baz'))


def test_run_executor():
    """
    Test that run_tasks properly uses threads/processes.
    """
    global RESULTS

    from arbiter.runner import run_tasks
    from arbiter.task import create_task

    # 1 thread
    completed, failed = run_tasks(
        [
            create_task('foo', procable_call, args=('foo',)),
            create_task(
                'bar', procable_call, dependencies=['foo'], args=('bar',)
            ),
            create_task(
                'baz', procable_call, dependencies=['bar'], args=('baz',)
            ),
            create_task('lorem', procable_call, args=('lorem',)),
            create_task(
                'ipsum', procable_call, dependencies=['lorem'], args=('ipsum',)
            ),
        ],
    )

    assert_equal(
        completed,
        {'foo', 'bar', 'baz', 'lorem', 'ipsum'}
    )

    assert_equal(failed, set())

    assert_equal(
        set(RESULTS.keys()),
        {'foo', 'bar', 'baz', 'lorem', 'ipsum'}
    )

    expected_pid = getpid()
    for pid, thread_name in RESULTS.values():
        assert_equal(pid, expected_pid)
        assert_not_equal(thread_name, 'MainThread')

    # 1 proc
    # RESULTS = {}
    # completed, failed = run_tasks(
    #     [
    #         create_task('foo', procable_call, args=('foo',)),
    #         create_task(
    #             'bar', procable_call, dependencies=['foo'], args=('bar',)
    #         ),
    #         create_task(
    #             'baz', procable_call, dependencies=['bar'], args=('baz',)
    #         ),
    #         create_task('lorem', procable_call, args=('lorem',)),
    #         create_task(
    #             'ipsum', procable_call, dependencies=['lorem'], args=('ipsum',)
    #         ),
    #     ],
    #     processes=True,
    # )

    # assert_equal(
    #     completed,
    #     {'foo', 'bar', 'baz', 'lorem', 'ipsum'}
    # )

    # assert_equal(failed, set())

    # assert_equal(
    #     set(call_stack.keys()),
    #     {'foo', 'bar', 'baz', 'lorem', 'ipsum'}
    # )

    # expected_pid = getpid()
    # for pid, thread_name in RESULTS.values():
    #     assert_not_equal(pid, expected_pid)
    #     assert_equal(thread_name, 'MainThread')
