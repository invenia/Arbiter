"""
Tests for the solver module.
"""
from nose.tools import assert_equals, assert_true, assert_raises


def test_empty():
    """
    Create an empty Solver.
    """
    from arbiter.solver import Solver

    solver = Solver()

    assert_equals(solver.completed, frozenset())
    assert_equals(solver.failed, frozenset())
    assert_equals(solver.runnable(), frozenset())
    assert_true(solver.start_task() is None)

    solver.remove_unrunnable()

    assert_equals(solver.completed, frozenset())
    assert_equals(solver.failed, frozenset())
    assert_equals(solver.runnable(), frozenset())
    assert_true(solver.start_task() is None)

    solver.fail_remaining()

    assert_equals(solver.completed, frozenset())
    assert_equals(solver.failed, frozenset())
    assert_equals(solver.runnable(), frozenset())
    assert_true(solver.start_task() is None)


def test_add_task():
    """
    Add a task to Solver.
    """
    from arbiter.solver import Solver

    solver = Solver()

    # no dependencies
    solver.add_task('foo')

    assert_equals(solver.completed, frozenset())
    assert_equals(solver.failed, frozenset())
    assert_equals(solver.runnable(), frozenset(('foo',)))

    # 1 dependency
    solver.add_task('bar', ('foo',))

    assert_equals(solver.completed, frozenset())
    assert_equals(solver.failed, frozenset())
    assert_equals(solver.runnable(), frozenset(('foo',)))

    # non-added dependency
    solver.add_task('ipsum', ('lorem',))

    assert_equals(solver.completed, frozenset())
    assert_equals(solver.failed, frozenset())
    assert_equals(solver.runnable(), frozenset(('foo',)))

    # invalid tasks

    # invalid name
    assert_raises(ValueError, solver.add_task, None)
    assert_raises(ValueError, solver.add_task, set())

    assert_equals(solver.completed, frozenset())
    assert_equals(solver.failed, frozenset())
    assert_equals(solver.runnable(), frozenset(('foo',)))

    # invalid dependency
    solver.add_task('failed', (None,))

    assert_equals(solver.completed, frozenset())
    assert_equals(solver.failed, frozenset(('failed',)))
    assert_equals(solver.runnable(), frozenset(('foo',)))

    # circular dependencies
    solver.add_task('ouroboros', ('ouroboros',))

    assert_equals(solver.completed, frozenset())
    assert_equals(solver.failed, frozenset(('failed', 'ouroboros')))
    assert_equals(solver.runnable(), frozenset(('foo',)))

    # dependency made circular
    solver.add_task('tick', ('tock',))

    assert_equals(solver.completed, frozenset())
    assert_equals(solver.failed, frozenset(('failed', 'ouroboros')))
    assert_equals(solver.runnable(), frozenset(('foo',)))

    solver.add_task('tock', ('tick',))

    assert_equals(solver.completed, frozenset())
    assert_equals(
        solver.failed, frozenset(('failed', 'ouroboros', 'tick', 'tock'))
    )
    assert_equals(solver.runnable(), frozenset(('foo',)))

    at_init = Solver(
        tasks={
            'foo': (),
            'bar': ('foo',),
            'ipsum': ('lorem',),
            'ouroboros': ('ouroboros',),
            'tick': ('tock',),
            'tock': ('tick',),
        }
    )

    assert_equals(at_init.completed, frozenset())
    assert_equals(
        at_init.failed, frozenset(('ouroboros', 'tick', 'tock'))
    )
    assert_equals(at_init.runnable(), frozenset(('foo',)))


def test_remove_unrunnable():
    """
    remove unrunnable Solver tasks
    """
    from arbiter.solver import Solver

    solver = Solver(
        tasks={
            'foo': (),
            'bar': ('foo',),
            'baz': ('bar',),
            'ipsum': ('lorem',),
            'dolor': ('ipsum',),
            'sit': ('dolor', 'stand'),
            'stand': (),
        }
    )

    assert_equals(solver.completed, frozenset())
    assert_equals(solver.failed, frozenset())
    assert_equals(solver.runnable(), frozenset(('foo', 'stand')))

    solver.remove_unrunnable()

    assert_equals(solver.completed, frozenset())
    assert_equals(solver.failed, frozenset(('ipsum', 'dolor', 'sit')))
    assert_equals(solver.runnable(), frozenset(('foo', 'stand')))


def test_start_task():
    """
    Start a task
    """
    from arbiter.solver import Solver

    solver = Solver(
        tasks={
            'foo': (),
            'fighters': ('foo',),
            'bar': ('foo',),
            'baz': ('bar',),
            'bell': ('bar',),
            'node': (),
        }
    )

    assert_equals(solver.completed, frozenset())
    assert_equals(solver.failed, frozenset())
    assert_equals(solver.runnable(), frozenset(('foo', 'node')))

    # start a specific task
    assert_equals(solver.start_task('node'), 'node')

    assert_equals(solver.completed, frozenset())
    assert_equals(solver.failed, frozenset())
    assert_equals(solver.runnable(), frozenset(('foo',)))

    # start tasks invalidly
    assert_raises(ValueError, solver.start_task, 'node')
    assert_raises(ValueError, solver.start_task, 'bar')
    assert_raises(ValueError, solver.start_task, 'fake')

    assert_equals(solver.completed, frozenset())
    assert_equals(solver.failed, frozenset())
    assert_equals(solver.runnable(), frozenset(('foo',)))

    # is node still stoppable
    solver.end_task('node')

    assert_equals(solver.completed, frozenset(('node',)))
    assert_equals(solver.failed, frozenset())
    assert_equals(solver.runnable(), frozenset(('foo',)))

    # start an arbitrary task
    assert_equals(solver.start_task(), 'foo')

    assert_equals(solver.completed, frozenset(('node',)))
    assert_equals(solver.failed, frozenset())
    assert_equals(solver.runnable(), frozenset())

    # no startable tasks
    assert_true(solver.start_task() is None)

    assert_equals(solver.completed, frozenset(('node',)))
    assert_equals(solver.failed, frozenset())
    assert_equals(solver.runnable(), frozenset())

    # start an arbitrary task
    solver.end_task('foo')

    assert_true(solver.start_task() in frozenset(('bar', 'fighters')))

    assert_equals(solver.completed, frozenset(('node', 'foo')))
    assert_equals(solver.failed, frozenset())
    assert_true(
        solver.runnable(),
        frozenset((frozenset(('boo',)), frozenset(('fighters',))))
    )


def test_end_task():
    """
    End a task
    """
    from arbiter.solver import Solver

    solver = Solver(
        tasks={
            'foo': (),
            'fighters': ('foo',),
            'bar': ('foo',),
            'baz': ('bar',),
            'qux': ('baz',),
            'bell': ('bar',),
        }
    )

    assert_equals(solver.completed, frozenset())
    assert_equals(solver.failed, frozenset())
    assert_equals(solver.runnable(), frozenset(('foo',)))

    # end a task
    solver.start_task('foo')
    solver.end_task('foo')

    assert_equals(solver.completed, frozenset(('foo',)))
    assert_equals(solver.failed, frozenset())
    assert_equals(solver.runnable(), frozenset(('bar', 'fighters')))

    # invalid ends
    assert_raises(KeyError, solver.end_task, 'foo')
    assert_raises(KeyError, solver.end_task, 'bar')
    assert_raises(KeyError, solver.end_task, 'baz')

    assert_equals(solver.completed, frozenset(('foo',)))
    assert_equals(solver.failed, frozenset())
    assert_equals(solver.runnable(), frozenset(('bar', 'fighters')))

    # fail a task
    solver.start_task('bar')
    solver.end_task('bar', False)

    assert_equals(solver.completed, frozenset(('foo',)))
    assert_equals(solver.failed, frozenset(('bar', 'baz', 'qux', 'bell')))
    assert_equals(solver.runnable(), frozenset(('fighters',)))


def test_fail_remaining():
    """
    Stop the solver
    """
    from arbiter.solver import Solver

    solver = Solver(
        tasks={
            'foo': (),
            'fighters': ('foo',),
            'bar': ('foo',),
            'baz': ('bar',),
            'qux': ('baz',),
            'bell': ('bar',),
            'node': (),
        }
    )

    solver.start_task('foo')
    solver.end_task('foo')
    solver.start_task('bar')

    solver.fail_remaining()

    assert_equals(solver.completed, frozenset(('foo',)))
    assert_equals(
        solver.failed,
        frozenset(('bar', 'baz', 'qux', 'bell', 'fighters', 'node'))
    )
    assert_equals(solver.runnable(), frozenset())

    # did that break adding tasks?
    solver.add_task('restart')

    assert_equals(solver.completed, frozenset(('foo',)))
    assert_equals(
        solver.failed,
        frozenset(('bar', 'baz', 'qux', 'bell', 'fighters', 'node'))
    )
    assert_equals(solver.runnable(), frozenset(('restart',)))


def test_context_manager():
    """
    use an Solver in the context manager
    """
    from arbiter.solver import Solver

    completed = set()
    failed = set()
    tasks = {
        'foo': (),
        'bar': ('foo',),
        'baz': ('bar',),
        'bell': ('bar',),
        'lorem': (),
        'ipsum': ('lorem',),
        'node': (),
        'failed': ('fake',),
    }

    with Solver(tasks=tasks, completed=completed, failed=failed) as solver:
        assert_equals(completed, frozenset())
        assert_equals(failed, frozenset(('failed',)))

        solver.start_task('foo')
        solver.end_task('foo')

        assert_equals(completed, frozenset(('foo',)))
        assert_equals(failed, frozenset(('failed',)))

        solver.start_task('lorem')
        solver.end_task('lorem', False)

        assert_equals(completed, frozenset(('foo',)))
        assert_equals(failed, frozenset(('failed', 'lorem', 'ipsum')))

        solver.start_task('bar')

    assert_equals(completed, frozenset(('foo',)))
    assert_equals(
        failed,
        frozenset(('failed', 'lorem', 'ipsum', 'bar', 'baz', 'bell', 'node'))
    )
