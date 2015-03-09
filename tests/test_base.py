"""
Tests for the scheduler module.
"""
from nose.tools import assert_equals


def test_empty():
    """
    Solve no tasks
    """
    from arbiter.base import Arbiter

    results = Arbiter(()).run()

    assert_equals(results.completed, frozenset())
    assert_equals(results.failed, frozenset())


def test_no_dependencies():
    """
    run dependency-less tasks
    """
    from arbiter.base import Arbiter, Task

    executed_tasks = set()

    def make_task(name, succeed=True):
        """
        Make a task
        """
        return Task(
            name=name,
            function=lambda: executed_tasks.add(name) or succeed,
            dependencies=(),
        )

    results = Arbiter(
        (
            make_task('foo'),
            make_task('bar'),
            make_task('baz'),
            make_task('fail', False)
        )
    ).run()

    assert_equals(executed_tasks, frozenset(('foo', 'bar', 'baz', 'fail')))
    assert_equals(results.completed, frozenset(('foo', 'bar', 'baz')))
    assert_equals(results.failed, frozenset(('fail',)))


def test_chain():
    """
    run a dependency chain
    """
    from arbiter.base import Arbiter, Task

    executed_tasks = set()

    def make_task(name, dependencies=(), succeed=True):
        """
        Make a task
        """
        return Task(
            name=name,
            function=lambda: executed_tasks.add(name) or succeed,
            dependencies=dependencies,
        )

    results = Arbiter(
        (
            make_task('foo'),
            make_task('bar', ('foo',)),
            make_task('baz', ('bar',), False),
            make_task('qux', ('baz',)),
        )
    ).run()

    assert_equals(executed_tasks, frozenset(('foo', 'bar', 'baz',)))
    assert_equals(results.completed, frozenset(('foo', 'bar',)))
    assert_equals(results.failed, frozenset(('baz', 'qux')))


def test_tree():
    """
    run a dependency tree
    """
    from arbiter.base import Arbiter, Task

    executed_tasks = set()

    def make_task(name, dependencies=(), succeed=True):
        """
        Make a task
        """
        return Task(
            name=name,
            function=lambda: executed_tasks.add(name) or succeed,
            dependencies=dependencies,
        )

    results = Arbiter(
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
    ).run()

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
