"""
Tests for the synchronous task runner.
"""
from nose.tools import assert_equals


def test_empty():
    """
    Solve no tasks
    """
    from arbiter.sync import run_tasks

    results = run_tasks(())

    assert_equals(results.completed, frozenset())
    assert_equals(results.failed, frozenset())


def test_no_dependencies():
    """
    run dependency-less tasks
    """
    from arbiter.sync import run_tasks
    from arbiter.task import create_task

    executed_tasks = set()

    def make_task(name, dependencies=(), succeed=True):
        """
        Make a task
        """
        return create_task(
            name=name,
            function=lambda: executed_tasks.add(name) or succeed,
            dependencies=dependencies,
        )

    results = run_tasks(
        (
            make_task('foo'),
            make_task('bar'),
            make_task('baz'),
            make_task('fail', succeed=False)
        )
    )

    assert_equals(executed_tasks, frozenset(('foo', 'bar', 'baz', 'fail')))
    assert_equals(results.completed, frozenset(('foo', 'bar', 'baz')))
    assert_equals(results.failed, frozenset(('fail',)))


def test_chain():
    """
    run a dependency chain
    """
    from arbiter.sync import run_tasks
    from arbiter.task import create_task

    executed_tasks = set()

    def make_task(name, dependencies=(), succeed=True):
        """
        Make a task
        """
        return create_task(
            name=name,
            function=lambda: executed_tasks.add(name) or succeed,
            dependencies=dependencies,
        )

    results = run_tasks(
        (
            make_task('foo'),
            make_task('bar', ('foo',)),
            make_task('baz', ('bar',), succeed=False),
            make_task('qux', ('baz',)),
        )
    )

    assert_equals(executed_tasks, frozenset(('foo', 'bar', 'baz',)))
    assert_equals(results.completed, frozenset(('foo', 'bar',)))
    assert_equals(results.failed, frozenset(('baz', 'qux')))


def test_tree():
    """
    run a dependency tree
    """
    from arbiter.sync import run_tasks
    from arbiter.task import create_task

    executed_tasks = set()

    def make_task(name, dependencies=(), succeed=True):
        """
        Make a task
        """
        return create_task(
            name=name,
            function=lambda: executed_tasks.add(name) or succeed,
            dependencies=dependencies,
        )

    results = run_tasks(
        (
            make_task('foo'),
            make_task('bar', ('foo',)),
            make_task('baz', ('bar',), False),
            make_task('qux', ('baz',)),
            make_task('bell', ('bar',)),
            make_task('alugosi', ('bell',), False),
            make_task('lorem'),
            make_task('ipsum', ('lorem',)),
            make_task('ouroboros', ('ouroboros',)),
            make_task('tick', ('tock',)),
            make_task('tock', ('tick',)),
            make_task('success', ('foo', 'lorem')),
            make_task('failed', ('qux', 'lorem')),
        )
    )

    assert_equals(
        executed_tasks,
        frozenset(
            (
                'foo', 'bar', 'baz', 'bell', 'alugosi',
                'lorem', 'ipsum', 'success'
            )
        )
    )
    assert_equals(
        results.completed,
        frozenset(
            ('foo', 'bar', 'bell', 'lorem', 'ipsum', 'success')
        )
    )
    assert_equals(
        results.failed,
        frozenset(
            ('baz', 'qux', 'alugosi', 'ouroboros', 'tick', 'tock', 'failed')
        )
    )


def test_retry():
    """
    runs a dependency tree with a retriable task.
    """
    from arbiter.sync import run_tasks
    from arbiter.task import create_task

    executed_tasks = set()

    def retrier(name, responses):
        """
        retrier closure allows us to define a retriable function
        """
        return lambda: executed_tasks.add(name) or responses.pop(0)

    def make_task(name, dependencies=(), responses=None, retries=0, delay=0):
        """
        Make a task.
        """
        if responses is None:
            responses = [True]

        return create_task(
            name=name,
            function=retrier(name, responses),
            dependencies=dependencies,
            retries=retries,
            delay=delay,
        )

    # baz - passes by using a retry which returns True,
    # alugosi - fails cause we forgot to set a retry value.
    # ipsum - fails cause both attempts return False
    results = run_tasks(
        (
            make_task('foo'),
            make_task('bar', ('foo',)),
            make_task('baz', ('bar',), [False, True], 1, 0.25),
            make_task('qux', ('baz',)),
            make_task('bell', ('bar',)),
            make_task('alugosi', ('bell',), [False, True]),
            make_task('lorem'),
            make_task('ipsum', ('lorem',), [False, False], 1, 0.25),
            make_task('ouroboros', ('ouroboros',)),
            make_task('tick', ('tock',)),
            make_task('tock', ('tick',)),
            make_task('success', ('foo', 'lorem')),
            make_task('failed', ('alugosi', 'lorem')),
        )
    )

    assert_equals(
        executed_tasks,
        frozenset(
            (
                'foo', 'bar', 'baz', 'qux', 'bell', 'alugosi',
                'lorem', 'ipsum', 'success'
            )
        )
    )
    assert_equals(
        results.completed,
        frozenset(
            ('foo', 'bar', 'baz', 'qux', 'bell', 'lorem', 'success')
        )
    )
    assert_equals(
        results.failed,
        frozenset(
            ('alugosi', 'ipsum', 'ouroboros', 'tick', 'tock', 'failed')
        )
    )
