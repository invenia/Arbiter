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

    executed_tasks = set()

    def make_task(name, dependencies=(), should_succeed=True):
        """
        Make a task
        """
        from arbiter.task import create_task

        function = succeed if should_succeed else fail

        return create_task(
            lambda: executed_tasks.add(name) or function(),
            name=name,
            dependencies=dependencies
        )

    results = run_tasks(
        (
            make_task('foo'),
            make_task('bar'),
            make_task('baz'),
            make_task('fail', should_succeed=False)
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

    executed_tasks = set()

    def make_task(name, dependencies=(), should_succeed=True):
        """
        Make a task
        """
        from arbiter.task import create_task

        function = succeed if should_succeed else fail

        return create_task(
            lambda: executed_tasks.add(name) or function(),
            name=name,
            dependencies=dependencies
        )

    results = run_tasks(
        (
            make_task('foo'),
            make_task('bar', ('foo',)),
            make_task('baz', ('bar',), should_succeed=False),
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

    executed_tasks = set()

    def make_task(name, dependencies=(), should_succeed=True):
        """
        Make a task
        """
        from arbiter.task import create_task

        function = succeed if should_succeed else fail

        return create_task(
            lambda: executed_tasks.add(name) or function(),
            name=name,
            dependencies=dependencies
        )

    results = run_tasks(
        (
            make_task('foo'),
            make_task('bar', ('foo',)),
            make_task('baz', ('bar',), should_succeed=False),
            make_task('qux', ('baz',)),
            make_task('bell', ('bar',)),
            make_task('alugosi', ('bell',), should_succeed=False),
            make_task('lorem'),
            make_task('ipsum', ('lorem',)),
            make_task('ouroboros', ('ouroboros',)),
            make_task('tick', ('tock',)),
            make_task('tock', ('tick',)),
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
            ('baz', 'qux', 'alugosi', 'ouroboros', 'tick', 'tock', 'failed')
        )
    )


def test_with_data():
    """
    Pass data.
    """
    from arbiter.async import run_tasks
    from arbiter.task import create_task

    data = [4, 5, 6]

    def myfunc(val=-1):
        """
        Modify some data which will be passed from another task.
        """
        data.append(val)

    foo = create_task(len, [1, 2], name='foo')
    bar = create_task(myfunc, name='bar', val=foo)
    results = run_tasks((foo, bar), 2)

    assert_equals(results.exceptions, [])
    assert_equals(results.completed, frozenset(('foo', 'bar')))
    assert_equals(data, [4, 5, 6, 2])


def succeed():
    """
    A task that succeeds
    """
    return True


def fail():
    """
    A task that fails
    """
    raise Exception("Failure Test")
