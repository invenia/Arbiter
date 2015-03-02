"""
Tests for the solver module.
"""
from nose.tools import assert_equals, assert_true, assert_raises


def test_empty():
    """
    Create an empty Arbiter.
    """
    from arbiter.solver import Arbiter

    arbiter = Arbiter()

    assert_equals(arbiter.completed, frozenset())
    assert_equals(arbiter.failed, frozenset())
    assert_equals(arbiter.runnable(), frozenset())
    assert_true(arbiter.start_task() is None)

    arbiter.remove_unrunnable()

    assert_equals(arbiter.completed, frozenset())
    assert_equals(arbiter.failed, frozenset())
    assert_equals(arbiter.runnable(), frozenset())
    assert_true(arbiter.start_task() is None)

    arbiter.fail_remaining()

    assert_equals(arbiter.completed, frozenset())
    assert_equals(arbiter.failed, frozenset())
    assert_equals(arbiter.runnable(), frozenset())
    assert_true(arbiter.start_task() is None)


def test_add_task():
    """
    Add a task to Arbiter.
    """
    from arbiter.solver import Arbiter

    arbiter = Arbiter()

    # no dependencies
    arbiter.add_task('foo')

    assert_equals(arbiter.completed, frozenset())
    assert_equals(arbiter.failed, frozenset())
    assert_equals(arbiter.runnable(), frozenset(('foo',)))

    # 1 dependency
    arbiter.add_task('bar', ('foo',))

    assert_equals(arbiter.completed, frozenset())
    assert_equals(arbiter.failed, frozenset())
    assert_equals(arbiter.runnable(), frozenset(('foo',)))

    # non-added dependency
    arbiter.add_task('ipsum', ('lorem',))

    assert_equals(arbiter.completed, frozenset())
    assert_equals(arbiter.failed, frozenset())
    assert_equals(arbiter.runnable(), frozenset(('foo',)))

    # invalid tasks

    # invalid name
    assert_raises(ValueError, arbiter.add_task, None)
    assert_raises(ValueError, arbiter.add_task, set())

    assert_equals(arbiter.completed, frozenset())
    assert_equals(arbiter.failed, frozenset())
    assert_equals(arbiter.runnable(), frozenset(('foo',)))

    # invalid dependency
    arbiter.add_task('failed', (None,))

    assert_equals(arbiter.completed, frozenset())
    assert_equals(arbiter.failed, frozenset(('failed',)))
    assert_equals(arbiter.runnable(), frozenset(('foo',)))

    # circular dependencies
    arbiter.add_task('ouroboros', ('ouroboros',))

    assert_equals(arbiter.completed, frozenset())
    assert_equals(arbiter.failed, frozenset(('failed', 'ouroboros')))
    assert_equals(arbiter.runnable(), frozenset(('foo',)))

    # dependency made circular
    arbiter.add_task('tick', ('tock',))

    assert_equals(arbiter.completed, frozenset())
    assert_equals(arbiter.failed, frozenset(('failed', 'ouroboros')))
    assert_equals(arbiter.runnable(), frozenset(('foo',)))

    arbiter.add_task('tock', ('tick',))

    assert_equals(arbiter.completed, frozenset())
    assert_equals(
        arbiter.failed, frozenset(('failed', 'ouroboros', 'tick', 'tock'))
    )
    assert_equals(arbiter.runnable(), frozenset(('foo',)))

    at_init = Arbiter(
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
    remove unrunnable Arbiter tasks
    """
    from arbiter.solver import Arbiter

    arbiter = Arbiter(
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

    assert_equals(arbiter.completed, frozenset())
    assert_equals(arbiter.failed, frozenset())
    assert_equals(arbiter.runnable(), frozenset(('foo', 'stand')))

    arbiter.remove_unrunnable()

    assert_equals(arbiter.completed, frozenset())
    assert_equals(arbiter.failed, frozenset(('ipsum', 'dolor', 'sit')))
    assert_equals(arbiter.runnable(), frozenset(('foo', 'stand')))


def test_start_task():
    """
    Start a task
    """
    from arbiter.solver import Arbiter

    arbiter = Arbiter(
        tasks={
            'foo': (),
            'fighters': ('foo',),
            'bar': ('foo',),
            'baz': ('bar',),
            'bell': ('bar',),
            'node': (),
        }
    )

    assert_equals(arbiter.completed, frozenset())
    assert_equals(arbiter.failed, frozenset())
    assert_equals(arbiter.runnable(), frozenset(('foo', 'node')))

    # start a specific task
    assert_equals(arbiter.start_task('node'), 'node')

    assert_equals(arbiter.completed, frozenset())
    assert_equals(arbiter.failed, frozenset())
    assert_equals(arbiter.runnable(), frozenset(('foo',)))

    # start tasks invalidly
    assert_raises(ValueError, arbiter.start_task, 'node')
    assert_raises(ValueError, arbiter.start_task, 'bar')
    assert_raises(ValueError, arbiter.start_task, 'fake')

    assert_equals(arbiter.completed, frozenset())
    assert_equals(arbiter.failed, frozenset())
    assert_equals(arbiter.runnable(), frozenset(('foo',)))

    # is node still stoppable
    arbiter.end_task('node')

    assert_equals(arbiter.completed, frozenset(('node',)))
    assert_equals(arbiter.failed, frozenset())
    assert_equals(arbiter.runnable(), frozenset(('foo',)))

    # start an arbitrary task
    assert_equals(arbiter.start_task(), 'foo')

    assert_equals(arbiter.completed, frozenset(('node',)))
    assert_equals(arbiter.failed, frozenset())
    assert_equals(arbiter.runnable(), frozenset())

    # no startable tasks
    assert_true(arbiter.start_task() is None)

    assert_equals(arbiter.completed, frozenset(('node',)))
    assert_equals(arbiter.failed, frozenset())
    assert_equals(arbiter.runnable(), frozenset())

    # start an arbitrary task
    arbiter.end_task('foo')

    assert_true(arbiter.start_task() in frozenset(('bar', 'fighters')))

    assert_equals(arbiter.completed, frozenset(('node', 'foo')))
    assert_equals(arbiter.failed, frozenset())
    assert_true(
        arbiter.runnable(),
        frozenset((frozenset(('boo',)), frozenset(('fighters',))))
    )


def test_end_task():
    """
    End a task
    """
    from arbiter.solver import Arbiter

    arbiter = Arbiter(
        tasks={
            'foo': (),
            'fighters': ('foo',),
            'bar': ('foo',),
            'baz': ('bar',),
            'qux': ('baz',),
            'bell': ('bar',),
        }
    )

    assert_equals(arbiter.completed, frozenset())
    assert_equals(arbiter.failed, frozenset())
    assert_equals(arbiter.runnable(), frozenset(('foo',)))

    # end a task
    arbiter.start_task('foo')
    arbiter.end_task('foo')

    assert_equals(arbiter.completed, frozenset(('foo',)))
    assert_equals(arbiter.failed, frozenset())
    assert_equals(arbiter.runnable(), frozenset(('bar', 'fighters')))

    # invalid ends
    assert_raises(KeyError, arbiter.end_task, 'foo')
    assert_raises(KeyError, arbiter.end_task, 'bar')
    assert_raises(KeyError, arbiter.end_task, 'baz')

    assert_equals(arbiter.completed, frozenset(('foo',)))
    assert_equals(arbiter.failed, frozenset())
    assert_equals(arbiter.runnable(), frozenset(('bar', 'fighters')))

    # fail a task
    arbiter.start_task('bar')
    arbiter.end_task('bar', False)

    assert_equals(arbiter.completed, frozenset(('foo',)))
    assert_equals(arbiter.failed, frozenset(('bar', 'baz', 'qux', 'bell')))
    assert_equals(arbiter.runnable(), frozenset(('fighters',)))


def test_fail_remaining():
    """
    Stop the arbiter
    """
    from arbiter.solver import Arbiter

    arbiter = Arbiter(
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

    arbiter.start_task('foo')
    arbiter.end_task('foo')
    arbiter.start_task('bar')

    arbiter.fail_remaining()

    assert_equals(arbiter.completed, frozenset(('foo',)))
    assert_equals(
        arbiter.failed,
        frozenset(('bar', 'baz', 'qux', 'bell', 'fighters', 'node'))
    )
    assert_equals(arbiter.runnable(), frozenset())

    # did that break adding tasks?
    arbiter.add_task('restart')

    assert_equals(arbiter.completed, frozenset(('foo',)))
    assert_equals(
        arbiter.failed,
        frozenset(('bar', 'baz', 'qux', 'bell', 'fighters', 'node'))
    )
    assert_equals(arbiter.runnable(), frozenset(('restart',)))


def test_context_manager():
    """
    use an Arbiter in the context manager
    """
    from arbiter.solver import Arbiter

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

    with Arbiter(tasks=tasks, completed=completed, failed=failed) as arbiter:
        assert_equals(completed, frozenset())
        assert_equals(failed, frozenset(('failed',)))

        arbiter.start_task('foo')
        arbiter.end_task('foo')

        assert_equals(completed, frozenset(('foo',)))
        assert_equals(failed, frozenset(('failed',)))

        arbiter.start_task('lorem')
        arbiter.end_task('lorem', False)

        assert_equals(completed, frozenset(('foo',)))
        assert_equals(failed, frozenset(('failed', 'lorem', 'ipsum')))

        arbiter.start_task('bar')

    assert_equals(completed, frozenset(('foo',)))
    assert_equals(
        failed,
        frozenset(('failed', 'lorem', 'ipsum', 'bar', 'baz', 'bell', 'node'))
    )
