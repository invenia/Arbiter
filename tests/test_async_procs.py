"""
Tests for the asynchronous task runner (using processes).
"""
from nose.tools import assert_equals


def test_empty():
    """
    Solve no tasks (with processes)
    """
    from arbiter.async import run_tasks

    results = run_tasks((), 2, use_processes=True)

    assert_equals(results.completed, frozenset())
    assert_equals(results.failed, frozenset())


def test_no_dependencies():
    """
    run dependency-less tasks (with processes)
    """
    from arbiter.async import run_tasks
    from arbiter.task import create_task

    def make_task(name, dependencies=(), should_succeed=True):
        """
        Make a task that should succeed
        """
        function = succeed if should_succeed else fail

        return create_task(name, function, dependencies)

    results = run_tasks(
        (
            make_task('foo'),
            make_task('bar'),
            make_task('baz'),
            make_task('fail', should_succeed=False)
        ),
        2,
        use_processes=True,
    )

    assert_equals(results.completed, frozenset(('foo', 'bar', 'baz')))
    assert_equals(results.failed, frozenset(('fail',)))


def test_chain():
    """
    run a dependency chain (with processes)
    """
    from arbiter.async import run_tasks
    from arbiter.task import create_task

    def make_task(name, dependencies=(), should_succeed=True):
        """
        Make a task that should succeed
        """
        function = succeed if should_succeed else fail

        return create_task(name, function, dependencies)

    results = run_tasks(
        (
            make_task('foo'),
            make_task('bar', ('foo',)),
            make_task('baz', ('bar',), should_succeed=False),
            make_task('qux', ('baz',)),
        ),
        2,
        use_processes=True,
    )

    assert_equals(results.completed, frozenset(('foo', 'bar',)))
    assert_equals(results.failed, frozenset(('baz', 'qux')))


def test_tree():
    """
    run a dependency tree (with processes)
    """
    from arbiter.async import run_tasks
    from arbiter.task import create_task

    def make_task(name, dependencies=(), should_succeed=True):
        """
        Make a task that should succeed
        """
        function = succeed if should_succeed else fail

        return create_task(name, function, dependencies)

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
        2,
        use_processes=True,
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


def succeed():
    """
    A task that succeeds
    """
    return True


def fail():
    """
    A task that fails
    """
