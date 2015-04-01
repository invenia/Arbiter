"""
Tests for the scheduler module.
"""
from nose.tools import assert_equals, assert_true, assert_false, assert_raises

from arbiter import task


def create_task(name, dependencies=()):
    """
    Create a task
    """
    return task.create_task(name, None, dependencies)


def test_empty():
    """
    Create an empty Scheduler.
    """
    from arbiter.scheduler import Scheduler

    scheduler = Scheduler()

    assert_equals(scheduler.completed, frozenset())
    assert_equals(scheduler.failed, frozenset())
    assert_equals(scheduler.running, frozenset())
    assert_equals(scheduler.runnable, frozenset())
    assert_true(scheduler.is_finished())
    assert_true(scheduler.start_task() is None)

    scheduler.remove_unrunnable()

    assert_equals(scheduler.completed, frozenset())
    assert_equals(scheduler.failed, frozenset())
    assert_equals(scheduler.running, frozenset())
    assert_equals(scheduler.runnable, frozenset())
    assert_true(scheduler.is_finished())
    assert_true(scheduler.start_task() is None)

    scheduler.fail_remaining()

    assert_equals(scheduler.completed, frozenset())
    assert_equals(scheduler.failed, frozenset())
    assert_equals(scheduler.running, frozenset())
    assert_equals(scheduler.runnable, frozenset())
    assert_true(scheduler.is_finished())
    assert_true(scheduler.start_task() is None)


def test_add_task():
    """
    Add a task to Scheduler.
    """
    from arbiter.scheduler import Scheduler

    scheduler = Scheduler()

    # no dependencies
    scheduler.add_task(create_task('foo'))

    assert_equals(scheduler.completed, frozenset())
    assert_equals(scheduler.failed, frozenset())
    assert_equals(scheduler.running, frozenset())
    assert_equals(scheduler.runnable, frozenset(('foo',)))
    assert_false(scheduler.is_finished())

    # 1 dependency
    scheduler.add_task(create_task('bar', ('foo',)))

    assert_equals(scheduler.completed, frozenset())
    assert_equals(scheduler.failed, frozenset())
    assert_equals(scheduler.running, frozenset())
    assert_equals(scheduler.runnable, frozenset(('foo',)))
    assert_false(scheduler.is_finished())

    # non-added dependency
    scheduler.add_task(create_task('ipsum', ('lorem',)))

    assert_equals(scheduler.completed, frozenset())
    assert_equals(scheduler.failed, frozenset())
    assert_equals(scheduler.running, frozenset())
    assert_equals(scheduler.runnable, frozenset(('foo',)))
    assert_false(scheduler.is_finished())

    # invalid tasks

    # invalid name
    assert_raises(ValueError, scheduler.add_task, create_task(None))
    assert_raises(ValueError, scheduler.add_task, create_task(set()))

    assert_equals(scheduler.completed, frozenset())
    assert_equals(scheduler.failed, frozenset())
    assert_equals(scheduler.running, frozenset())
    assert_equals(scheduler.runnable, frozenset(('foo',)))
    assert_false(scheduler.is_finished())

    # invalid dependency
    scheduler.add_task(create_task('failed', (None,)))

    assert_equals(scheduler.completed, frozenset())
    assert_equals(scheduler.failed, frozenset(('failed',)))
    assert_equals(scheduler.running, frozenset())
    assert_equals(scheduler.runnable, frozenset(('foo',)))
    assert_false(scheduler.is_finished())

    # circular dependencies
    scheduler.add_task(create_task('ouroboros', ('ouroboros',)))

    assert_equals(scheduler.completed, frozenset())
    assert_equals(scheduler.failed, frozenset(('failed', 'ouroboros')))
    assert_equals(scheduler.running, frozenset())
    assert_equals(scheduler.runnable, frozenset(('foo',)))
    assert_false(scheduler.is_finished())

    # dependency made circular
    scheduler.add_task(create_task('tick', ('tock',)))

    assert_equals(scheduler.completed, frozenset())
    assert_equals(scheduler.failed, frozenset(('failed', 'ouroboros')))
    assert_equals(scheduler.running, frozenset())
    assert_equals(scheduler.runnable, frozenset(('foo',)))
    assert_false(scheduler.is_finished())

    scheduler.add_task(create_task('tock', ('tick',)))

    assert_equals(scheduler.completed, frozenset())
    assert_equals(
        scheduler.failed, frozenset(('failed', 'ouroboros', 'tick', 'tock'))
    )
    assert_equals(scheduler.running, frozenset())
    assert_equals(scheduler.runnable, frozenset(('foo',)))
    assert_false(scheduler.is_finished())

    at_init = Scheduler(
        tasks=(
            create_task('foo'),
            create_task('bar', ('foo',)),
            create_task('ipsum', ('lorem',)),
            create_task('ouroboros', ('ouroboros',)),
            create_task('tick', ('tock',)),
            create_task('tock', ('tick',)),
        )
    )

    assert_equals(at_init.completed, frozenset())
    assert_equals(
        at_init.failed, frozenset(('ouroboros', 'tick', 'tock'))
    )
    assert_equals(at_init.runnable, frozenset(('foo',)))
    assert_false(at_init.is_finished())


def test_remove_unrunnable():
    """
    remove unrunnable Scheduler tasks
    """
    from arbiter.scheduler import Scheduler

    scheduler = Scheduler(
        tasks=(
            create_task('foo'),
            create_task('bar', ('foo',)),
            create_task('baz', ('bar',)),
            create_task('ipsum', ('lorem',)),
            create_task('dolor', ('ipsum',)),
            create_task('sit', ('dolor', 'stand')),
            create_task('stand'),
        )
    )

    assert_equals(scheduler.completed, frozenset())
    assert_equals(scheduler.failed, frozenset())
    assert_equals(scheduler.running, frozenset())
    assert_equals(scheduler.runnable, frozenset(('foo', 'stand')))
    assert_false(scheduler.is_finished())

    scheduler.remove_unrunnable()

    assert_equals(scheduler.completed, frozenset())
    assert_equals(scheduler.failed, frozenset(('ipsum', 'dolor', 'sit')))
    assert_equals(scheduler.running, frozenset())
    assert_equals(scheduler.runnable, frozenset(('foo', 'stand')))
    assert_false(scheduler.is_finished())


def test_start_task():
    """
    Start a task
    """
    from arbiter.scheduler import Scheduler

    scheduler = Scheduler(
        tasks=(
            create_task('foo'),
            create_task('fighters', ('foo',)),
            create_task('bar', ('foo',)),
            create_task('baz', ('bar',)),
            create_task('bell', ('bar',)),
            create_task('node'),
        )
    )

    assert_equals(scheduler.completed, frozenset())
    assert_equals(scheduler.failed, frozenset())
    assert_equals(scheduler.running, frozenset())
    assert_equals(scheduler.runnable, frozenset(('foo', 'node')))
    assert_false(scheduler.is_finished())

    # start a specific task
    assert_equals(scheduler.start_task('node').name, 'node')

    assert_equals(scheduler.completed, frozenset())
    assert_equals(scheduler.failed, frozenset())
    assert_equals(scheduler.running, frozenset(('node',)))
    assert_equals(scheduler.runnable, frozenset(('foo',)))
    assert_false(scheduler.is_finished())

    # start tasks invalidly
    assert_raises(ValueError, scheduler.start_task, 'node')
    assert_raises(ValueError, scheduler.start_task, 'bar')
    assert_raises(ValueError, scheduler.start_task, 'fake')

    assert_equals(scheduler.completed, frozenset())
    assert_equals(scheduler.failed, frozenset())
    assert_equals(scheduler.running, frozenset(('node',)))
    assert_equals(scheduler.runnable, frozenset(('foo',)))
    assert_false(scheduler.is_finished())

    # is node still stoppable
    scheduler.end_task('node')

    assert_equals(scheduler.completed, frozenset(('node',)))
    assert_equals(scheduler.failed, frozenset())
    assert_equals(scheduler.running, frozenset())
    assert_equals(scheduler.runnable, frozenset(('foo',)))
    assert_false(scheduler.is_finished())

    # start an arbitrary task
    assert_equals(scheduler.start_task().name, 'foo')

    assert_equals(scheduler.completed, frozenset(('node',)))
    assert_equals(scheduler.failed, frozenset())
    assert_equals(scheduler.running, frozenset(('foo',)))
    assert_equals(scheduler.runnable, frozenset())
    assert_false(scheduler.is_finished())

    # no startable tasks
    assert_true(scheduler.start_task() is None)

    assert_equals(scheduler.completed, frozenset(('node',)))
    assert_equals(scheduler.failed, frozenset())
    assert_equals(scheduler.running, frozenset(('foo',)))
    assert_equals(scheduler.runnable, frozenset())
    assert_false(scheduler.is_finished())

    # start an arbitrary task
    scheduler.end_task('foo')

    started = scheduler.start_task()

    assert_true(started.name in frozenset(('bar', 'fighters')))

    assert_equals(scheduler.completed, frozenset(('node', 'foo')))
    assert_equals(scheduler.failed, frozenset())

    if started.name == 'bar':
        assert_equals(scheduler.running, frozenset(('bar',)))
        assert_equals(scheduler.runnable, frozenset(('fighters',)))
    else:
        assert_equals(scheduler.running, frozenset(('fighters',)))
        assert_equals(scheduler.runnable, frozenset(('bar',)))
        assert_false(scheduler.is_finished())


def test_end_task():
    """
    End a task
    """
    from arbiter.scheduler import Scheduler

    scheduler = Scheduler(
        tasks=(
            create_task('foo'),
            create_task('fighters', ('foo',)),
            create_task('bar', ('foo',)),
            create_task('baz', ('bar',)),
            create_task('qux', ('baz',)),
            create_task('bell', ('bar',)),
        )
    )

    assert_equals(scheduler.completed, frozenset())
    assert_equals(scheduler.failed, frozenset())
    assert_equals(scheduler.running, frozenset())
    assert_equals(scheduler.runnable, frozenset(('foo',)))
    assert_false(scheduler.is_finished())

    # end a task
    scheduler.start_task('foo')
    scheduler.end_task('foo')

    assert_equals(scheduler.completed, frozenset(('foo',)))
    assert_equals(scheduler.failed, frozenset())
    assert_equals(scheduler.running, frozenset())
    assert_equals(scheduler.runnable, frozenset(('bar', 'fighters')))
    assert_false(scheduler.is_finished())

    # invalid ends
    assert_raises(KeyError, scheduler.end_task, 'foo')
    assert_raises(KeyError, scheduler.end_task, 'bar')
    assert_raises(KeyError, scheduler.end_task, 'baz')

    assert_equals(scheduler.completed, frozenset(('foo',)))
    assert_equals(scheduler.failed, frozenset())
    assert_equals(scheduler.running, frozenset())
    assert_equals(scheduler.runnable, frozenset(('bar', 'fighters')))
    assert_false(scheduler.is_finished())

    # fail a task
    scheduler.start_task('bar')
    scheduler.end_task('bar', False)

    assert_equals(scheduler.completed, frozenset(('foo',)))
    assert_equals(scheduler.failed, frozenset(('bar', 'baz', 'qux', 'bell')))
    assert_equals(scheduler.running, frozenset())
    assert_equals(scheduler.runnable, frozenset(('fighters',)))
    assert_false(scheduler.is_finished())


def test_fail_remaining():
    """
    Stop the scheduler
    """
    from arbiter.scheduler import Scheduler

    scheduler = Scheduler(
        tasks=(
            create_task('foo'),
            create_task('fighters', ('foo',)),
            create_task('bar', ('foo',)),
            create_task('baz', ('bar',)),
            create_task('qux', ('baz',)),
            create_task('bell', ('bar',)),
            create_task('node'),
        )
    )

    scheduler.start_task('foo')
    scheduler.end_task('foo')
    scheduler.start_task('bar')

    scheduler.fail_remaining()

    assert_equals(scheduler.completed, frozenset(('foo',)))
    assert_equals(
        scheduler.failed,
        frozenset(('bar', 'baz', 'qux', 'bell', 'fighters', 'node'))
    )
    assert_equals(scheduler.running, frozenset())
    assert_equals(scheduler.runnable, frozenset())
    assert_true(scheduler.is_finished())

    # did that break adding tasks?
    scheduler.add_task(create_task('restart'))

    assert_equals(scheduler.completed, frozenset(('foo',)))
    assert_equals(
        scheduler.failed,
        frozenset(('bar', 'baz', 'qux', 'bell', 'fighters', 'node'))
    )
    assert_equals(scheduler.running, frozenset())
    assert_equals(scheduler.runnable, frozenset(('restart',)))
    assert_false(scheduler.is_finished())


def test_context_manager():
    """
    use an Scheduler in the context manager
    """
    from arbiter.scheduler import Scheduler

    completed = set()
    failed = set()
    tasks = (
        create_task('foo'),
        create_task('bar', ('foo',)),
        create_task('baz', ('bar',)),
        create_task('bell', ('bar',)),
        create_task('lorem', ()),
        create_task('ipsum', ('lorem',)),
        create_task('node'),
        create_task('failed', ('fake',)),
    )

    with Scheduler(tasks, completed=completed, failed=failed) as scheduler:
        assert_equals(completed, frozenset())
        assert_equals(failed, frozenset(('failed',)))

        scheduler.start_task('foo')
        scheduler.end_task('foo')

        assert_equals(completed, frozenset(('foo',)))
        assert_equals(failed, frozenset(('failed',)))

        scheduler.start_task('lorem')
        scheduler.end_task('lorem', False)

        assert_equals(completed, frozenset(('foo',)))
        assert_equals(failed, frozenset(('failed', 'lorem', 'ipsum')))

        scheduler.start_task('bar')

    assert_equals(completed, frozenset(('foo',)))
    assert_equals(
        failed,
        frozenset(('failed', 'lorem', 'ipsum', 'bar', 'baz', 'bell', 'node'))
    )
