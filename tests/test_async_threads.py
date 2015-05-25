"""
Tests for the asynchronous task runner (using threads).
"""
from nose.tools import assert_equals


def test_empty():
    """
    Solve no tasks (with threads)
    """
    from arbiter.async import run_tasks

    results = run_tasks((), 2)

    assert_equals(results.completed, frozenset())
    assert_equals(results.failed, frozenset())


def test_no_dependencies():
    """
    run dependency-less tasks (with threads)
    """
    from arbiter.async import run_tasks
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
        ),
        2
    )

    assert_equals(executed_tasks, frozenset(('foo', 'bar', 'baz', 'fail')))
    assert_equals(results.completed, frozenset(('foo', 'bar', 'baz')))
    assert_equals(results.failed, frozenset(('fail',)))


def test_chain():
    """
    run a dependency chain (with threads)
    """
    from arbiter.async import run_tasks
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
        ),
        2
    )

    assert_equals(executed_tasks, frozenset(('foo', 'bar', 'baz',)))
    assert_equals(results.completed, frozenset(('foo', 'bar',)))
    assert_equals(results.failed, frozenset(('baz', 'qux')))


def test_tree():
    """
    run a dependency tree (with threads)
    """
    from arbiter.async import run_tasks
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
            make_task('success', ('foo', 'lorem')),
            make_task('failed', ('qux', 'lorem')),
        ),
        2
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
            ('baz', 'qux', 'alugosi', 'failed')
        )
    )
