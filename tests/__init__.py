# encoding: UTF-8
"""
Tests for the arbiter module.
"""
from nose.tools import assert_equals, assert_raises


def test_empty():
    """
    Test the creation of an empty Arbiter.
    """
    from arbiter import Arbiter

    arbiter = Arbiter()

    assert_equals(arbiter.tasks, frozenset())
    assert_equals(arbiter.blocked, {})
    assert_equals(arbiter.runnable, frozenset())
    assert_equals(arbiter.running, frozenset())
    assert_equals(arbiter.completed, frozenset())
    assert_equals(arbiter.failed, frozenset())


def test_add_task():
    """
    Test the add_task method.
    """
    from arbiter import Arbiter

    arbiter = Arbiter()

    # add a task with no dependencies
    arbiter.add_task('foo')

    assert_equals(arbiter.tasks, {'foo'})
    assert_equals(arbiter.blocked, {})
    assert_equals(arbiter.runnable, {'foo'})
    assert_equals(arbiter.running, frozenset())
    assert_equals(arbiter.completed, frozenset())
    assert_equals(arbiter.failed, frozenset())

    # attempt to add a task with no name
    with assert_raises(ValueError):
        arbiter.add_task(None)

    # attempt to re-add the task
    with assert_raises(ValueError):
        arbiter.add_task('foo')

    # attempt to redefine the task
    with assert_raises(ValueError):
        arbiter.add_task('foo', {'fu'})

    # sanity check that none of the previous broke anything
    assert_equals(arbiter.tasks, {'foo'})
    assert_equals(arbiter.blocked, {})
    assert_equals(arbiter.runnable, {'foo'})
    assert_equals(arbiter.running, frozenset())
    assert_equals(arbiter.completed, frozenset())
    assert_equals(arbiter.failed, frozenset())

    # add a task with a dependency
    arbiter.add_task('bar', {'foo'})

    assert_equals(arbiter.tasks, {'foo', 'bar'})
    assert_equals(arbiter.blocked, {'bar': {'foo'}})
    assert_equals(arbiter.runnable, {'foo'})
    assert_equals(arbiter.running, frozenset())
    assert_equals(arbiter.completed, frozenset())
    assert_equals(arbiter.failed, frozenset())

    # add a task with not-yet-existent dependency
    arbiter.add_task('bravo', {'alpha'})

    assert_equals(arbiter.tasks, {'foo', 'bar', 'bravo'})
    assert_equals(arbiter.blocked, {'bar': {'foo'}, 'bravo': {'alpha'}})
    assert_equals(arbiter.runnable, {'foo'})
    assert_equals(arbiter.running, frozenset())
    assert_equals(arbiter.completed, frozenset())
    assert_equals(arbiter.failed, frozenset())

    # add a task with multiple dependencies
    arbiter.add_task(
        'battle',
        (task for task in ('knowing', 'red lasers', 'blue lasers'))
    )

    assert_equals(arbiter.tasks, {'foo', 'bar', 'bravo', 'battle'})
    assert_equals(
        arbiter.blocked,
        {
            'bar': {'foo'},
            'bravo': {'alpha'},
            'battle': {'knowing', 'red lasers', 'blue lasers'},
        }
    )
    assert_equals(arbiter.runnable, {'foo'})
    assert_equals(arbiter.running, frozenset())
    assert_equals(arbiter.completed, frozenset())
    assert_equals(arbiter.failed, frozenset())

    # How does add_task handle running/completed/failed
    arbiter.add_task('running')
    arbiter.start_task('running')

    arbiter.add_task('completed')
    arbiter.start_task('completed')
    arbiter.end_task('completed')

    arbiter.add_task('failed')
    arbiter.start_task('failed')
    arbiter.end_task('failed', success=False)

    # sanity check that the above methods aren't broken
    assert_equals(
        arbiter.tasks,
        {'foo', 'bar', 'bravo', 'battle', 'running', 'completed', 'failed'}
    )
    assert_equals(
        arbiter.blocked,
        {
            'bar': {'foo'},
            'bravo': {'alpha'},
            'battle': {'knowing', 'red lasers', 'blue lasers'},
        }
    )
    assert_equals(arbiter.runnable, {'foo'})
    assert_equals(arbiter.running, {'running'})
    assert_equals(arbiter.completed, {'completed'})
    assert_equals(arbiter.failed, {'failed'})

    # tasks dependent on failed task should also fail
    arbiter.add_task('should-fail', {'running', 'completed', 'failed'})

    assert_equals(
        arbiter.tasks,
        {'foo', 'bar', 'bravo', 'battle', 'running', 'completed', 'failed',
         'should-fail'}
    )
    assert_equals(
        arbiter.blocked,
        {
            'bar': {'foo'},
            'bravo': {'alpha'},
            'battle': {'knowing', 'red lasers', 'blue lasers'},
        }
    )
    assert_equals(arbiter.runnable, {'foo'})
    assert_equals(arbiter.running, {'running'})
    assert_equals(arbiter.completed, {'completed'})
    assert_equals(arbiter.failed, {'failed', 'should-fail'})

    # tasks shouldn't be blocked on completed tasks
    arbiter.add_task('partial-block', {'running', 'completed'})
    arbiter.add_task('should-be-runnable', {'completed'})

    assert_equals(
        arbiter.tasks,
        {'foo', 'bar', 'bravo', 'battle', 'running', 'completed', 'failed',
         'should-fail', 'partial-block', 'should-be-runnable'}
    )
    assert_equals(
        arbiter.blocked,
        {
            'bar': {'foo'},
            'bravo': {'alpha'},
            'battle': {'knowing', 'red lasers', 'blue lasers'},
            'partial-block': {'running'},
        }
    )
    assert_equals(arbiter.runnable, {'foo', 'should-be-runnable'})
    assert_equals(arbiter.running, {'running'})
    assert_equals(arbiter.completed, {'completed'})
    assert_equals(arbiter.failed, {'failed', 'should-fail'})

    # failures should cascade after the fact
    arbiter.add_task('alpha', {'failed'})

    assert_equals(
        arbiter.tasks,
        {'foo', 'bar', 'bravo', 'battle', 'running', 'completed', 'failed',
         'should-fail', 'partial-block', 'should-be-runnable', 'alpha'}
    )
    assert_equals(
        arbiter.blocked,
        {
            'bar': {'foo'},
            'battle': {'knowing', 'red lasers', 'blue lasers'},
            'partial-block': {'running'},
        }
    )
    assert_equals(arbiter.runnable, {'foo', 'should-be-runnable'})
    assert_equals(arbiter.running, {'running'})
    assert_equals(arbiter.completed, {'completed'})
    assert_equals(arbiter.failed, {'failed', 'should-fail', 'alpha', 'bravo'})

    # add_tasks should be called on any tasks added at init
    pre_loaded = Arbiter(
        tasks={
            'foo': set(),
            'bar': {'foo'},
            'alpha': None,
            'dangerdoom': (task for task in ('mf doom', 'danger mouse')),
        }
    )

    assert_equals(pre_loaded.tasks, {'foo', 'bar', 'alpha', 'dangerdoom'})
    assert_equals(
        pre_loaded.blocked,
        {
            'bar': {'foo'},
            'dangerdoom': {'mf doom', 'danger mouse'},
        }
    )
    assert_equals(pre_loaded.runnable, {'foo', 'alpha'})
    assert_equals(pre_loaded.running, frozenset())
    assert_equals(pre_loaded.completed, frozenset())
    assert_equals(pre_loaded.failed, frozenset())

    # Unicode for good measure
    arbiter = Arbiter()

    arbiter.add_task(u'👶')
    arbiter.add_task(u'👧', dependencies={u'👶'})
    arbiter.add_task(u'👩', dependencies={u'👧'})

    assert_equals(arbiter.tasks, {u'👶', u'👧', u'👩'})
    assert_equals(
        arbiter.blocked,
        {
            u'👧': {u'👶'},
            u'👩': {u'👧'},
        }
    )
    assert_equals(arbiter.runnable, {u'👶'})
    assert_equals(arbiter.running, frozenset())
    assert_equals(arbiter.completed, frozenset())
    assert_equals(arbiter.failed, frozenset())


def test_remove_dead_nodes():
    """
    Test the remove_dead_tasks method.
    """
    from arbiter import Arbiter

    # Make sure an empty set of tasks doesn't disrupt anything
    arbiter = Arbiter()

    arbiter.remove_dead_tasks()
    arbiter.remove_dead_tasks(keep_orphans=True)

    assert_equals(arbiter.tasks, frozenset())
    assert_equals(arbiter.blocked, {})
    assert_equals(arbiter.runnable, frozenset())
    assert_equals(arbiter.running, frozenset())
    assert_equals(arbiter.completed, frozenset())
    assert_equals(arbiter.failed, frozenset())

    # Cycles and children thereof
    arbiter = Arbiter(
        tasks={
            'ouroboros': {'ouroboros'},
            'snakelet': {'ouroboros'},
            'bert': {'ernie'},
            'ernie': {'bert'},
            'a': {'b'},
            'b': {'c'},
            'c': {'a'},
            'beta': {'a'},
            'charlie': {'beta'},
            'safe': None,
            'safer': {'safe'},
        }
    )

    arbiter.remove_dead_tasks()

    assert_equals(
        arbiter.tasks,
        {
            'ouroboros',
            'snakelet',
            'bert',
            'ernie',
            'a',
            'b',
            'c',
            'beta',
            'charlie',
            'safe',
            'safer',
        }
    )
    assert_equals(arbiter.blocked, {'safer': {'safe'}})
    assert_equals(arbiter.runnable, {'safe'})
    assert_equals(arbiter.running, frozenset())
    assert_equals(arbiter.completed, frozenset())
    assert_equals(
        arbiter.failed,
        {
            'ouroboros',
            'snakelet',
            'bert',
            'ernie',
            'a',
            'b',
            'c',
            'beta',
            'charlie',
        }
    )

    # Orphans
    arbiter = Arbiter(
        tasks={
            'brawlers': {'orphans'},
            'bawlers': {'orphans'},
            'bastards': {'orphans'},
            'glitter': {'bastards'},
            'doom': {'glitter'},
        }
    )

    # keep
    arbiter.remove_dead_tasks(keep_orphans=True)

    assert_equals(
        arbiter.tasks,
        {
            'brawlers',
            'bawlers',
            'bastards',
            'glitter',
            'doom',
        }
    )
    assert_equals(
        arbiter.blocked,
        {
            'brawlers': {'orphans'},
            'bawlers': {'orphans'},
            'bastards': {'orphans'},
            'glitter': {'bastards'},
            'doom': {'glitter'},
        }
    )
    assert_equals(arbiter.runnable, frozenset())
    assert_equals(arbiter.running, frozenset())
    assert_equals(arbiter.completed, frozenset())
    assert_equals(arbiter.failed, frozenset())

    # remove
    arbiter.remove_dead_tasks(keep_orphans=False)

    assert_equals(
        arbiter.tasks,
        {
            'brawlers',
            'bawlers',
            'bastards',
            'glitter',
            'doom',
        }
    )
    assert_equals(arbiter.blocked, {})
    assert_equals(arbiter.runnable, frozenset())
    assert_equals(arbiter.running, frozenset())
    assert_equals(arbiter.completed, frozenset())
    assert_equals(
        arbiter.failed,
        {
            'brawlers',
            'bawlers',
            'bastards',
            'glitter',
            'doom',
        }
    )

    # make sure that keeping orphans doesn't stop cycle detections
    arbiter = Arbiter(
        tasks={
            'ouroboros': {'ouroboros'},
            'quetzalcoatl': {'ouroboros', 'feathers'},
            'father': {u'holy 👻'},
            u'holy 👻': {'son'},
            'son': {'father', 'mary'},
        }
    )

    arbiter.remove_dead_tasks(keep_orphans=True)
    assert_equals(
        arbiter.tasks,
        {
            'ouroboros',
            'quetzalcoatl',
            'father',
            u'holy 👻',
            'son',
        }
    )
    assert_equals(arbiter.blocked, {})
    assert_equals(arbiter.runnable, frozenset())
    assert_equals(arbiter.running, frozenset())
    assert_equals(arbiter.completed, frozenset())
    assert_equals(
        arbiter.failed,
        {
            'ouroboros',
            'quetzalcoatl',
            'father',
            u'holy 👻',
            'son',
        }
    )


def test_start_task():
    """
    Test the start_task method.
    """
    from arbiter import Arbiter

    arbiter = Arbiter()

    # No tasks
    assert_equals(arbiter.start_task(), None)

    assert_equals(arbiter.tasks, frozenset())
    assert_equals(arbiter.blocked, {})
    assert_equals(arbiter.runnable, frozenset())
    assert_equals(arbiter.running, frozenset())
    assert_equals(arbiter.completed, frozenset())
    assert_equals(arbiter.failed, frozenset())

    # make sure only runnable tasks can be started
    arbiter.add_task('runnable')
    arbiter.add_task('blocked', dependencies={'runnable'})

    assert_equals(arbiter.start_task(), 'runnable')
    assert_equals(arbiter.start_task(), None)

    assert_equals(arbiter.tasks, {'runnable', 'blocked'})
    assert_equals(arbiter.blocked, {'blocked': {'runnable'}})
    assert_equals(arbiter.runnable, frozenset())
    assert_equals(arbiter.running, {'runnable'})
    assert_equals(arbiter.completed, frozenset())
    assert_equals(arbiter.failed, frozenset())

    # starting tasks by name
    with assert_raises(ValueError):
        arbiter.start_task('non-existent')

    with assert_raises(ValueError):
        arbiter.start_task('blocked')

    with assert_raises(ValueError):
        arbiter.start_task('runnable')

    assert_equals(arbiter.tasks, {'runnable', 'blocked'})
    assert_equals(arbiter.blocked, {'blocked': {'runnable'}})
    assert_equals(arbiter.runnable, frozenset())
    assert_equals(arbiter.running, {'runnable'})
    assert_equals(arbiter.completed, frozenset())
    assert_equals(arbiter.failed, frozenset())

    arbiter.add_task('alpha')
    arbiter.add_task('bravo')

    assert_equals(arbiter.start_task('alpha'), 'alpha')

    assert_equals(arbiter.tasks, {'runnable', 'blocked', 'alpha', 'bravo'})
    assert_equals(arbiter.blocked, {'blocked': {'runnable'}})
    assert_equals(arbiter.runnable, {'bravo'})
    assert_equals(arbiter.running, {'runnable', 'alpha'})
    assert_equals(arbiter.completed, frozenset())
    assert_equals(arbiter.failed, frozenset())


def test_end_task():
    """
    Test the end_task method.
    """
    from arbiter import Arbiter

    arbiter = Arbiter(
        tasks={
            'blocked': {'runnable'},
            'runnable': None,
            'running': None,
        }
    )
    arbiter.start_task('running')

    # can't end a non-existent task
    with assert_raises(ValueError):
        arbiter.end_task('non-existent')

    # ... or one that isn't running
    with assert_raises(ValueError):
        arbiter.end_task('runnable')

    with assert_raises(ValueError):
        arbiter.end_task('blocked')

    assert_equals(arbiter.tasks, {'blocked', 'runnable', 'running'})
    assert_equals(arbiter.blocked, {'blocked': {'runnable'}})
    assert_equals(arbiter.runnable, {'runnable'})
    assert_equals(arbiter.running, {'running'})
    assert_equals(arbiter.completed, frozenset())
    assert_equals(arbiter.failed, frozenset())

    # ending a running task
    arbiter.end_task('running')

    assert_equals(arbiter.tasks, {'blocked', 'runnable', 'running'})
    assert_equals(arbiter.blocked, {'blocked': {'runnable'}})
    assert_equals(arbiter.runnable, {'runnable'})
    assert_equals(arbiter.running, frozenset())
    assert_equals(arbiter.completed, {'running'})
    assert_equals(arbiter.failed, frozenset())

    # completing a task should update blocked
    arbiter.add_task('still blocked', {'runnable', 'something else'})
    arbiter.start_task('runnable')
    arbiter.end_task('runnable')

    assert_equals(
        arbiter.tasks,
        {'blocked', 'runnable', 'running', 'still blocked'}
    )
    assert_equals(arbiter.blocked, {'still blocked': {'something else'}})
    assert_equals(arbiter.runnable, {'blocked'})
    assert_equals(arbiter.running, frozenset())
    assert_equals(arbiter.completed, {'running', 'runnable'})
    assert_equals(arbiter.failed, frozenset())

    # make sure failed tasks cascade
    arbiter = Arbiter(
        tasks={
            'foo': None,
            'bar': {'foo'},
            'baz': {'bar'},
            'qux': {'baz'},
            'lorem': None,
            'ipsum': {'lorem'},
        }
    )

    arbiter.start_task('foo')
    arbiter.end_task('foo', success=False)

    assert_equals(
        arbiter.tasks,
        {'foo', 'bar', 'baz', 'qux', 'lorem', 'ipsum'}
    )
    assert_equals(arbiter.blocked, {'ipsum': {'lorem'}})
    assert_equals(arbiter.runnable, {'lorem'})
    assert_equals(arbiter.running, frozenset())
    assert_equals(arbiter.completed, frozenset())
    assert_equals(arbiter.failed, {'foo', 'bar', 'baz', 'qux'})


def test_stop():
    """
    Test the stop method
    """
    from arbiter import Arbiter

    arbiter = Arbiter(
        tasks={
            'blocked': {'runnable'},
            'runnable': None,
            'running': None,
            'completed': None,
            'failed': None,
        }
    )
    arbiter.start_task('running')
    arbiter.start_task('completed')
    arbiter.end_task('completed')
    arbiter.start_task('failed')
    arbiter.end_task('failed', False)

    assert_equals(
        arbiter.stop(),
        (
            {'completed'},
            {'failed', 'running', 'runnable', 'blocked'}
        )
    )

    assert_equals(
        arbiter.tasks,
        {'blocked', 'runnable', 'running', 'completed', 'failed'}
    )
    assert_equals(arbiter.blocked, {})
    assert_equals(arbiter.runnable, frozenset())
    assert_equals(arbiter.running, frozenset())
    assert_equals(arbiter.completed, {'completed'})
    assert_equals(
        arbiter.failed,
        {'failed', 'running', 'runnable', 'blocked'}
    )


def test_context():
    """
    Test the context manager
    """
    from arbiter import Arbiter

    arbiter = Arbiter(
        tasks={
            'blocked': {'runnable'},
            'runnable': None,
            'running': None,
            'completed': None,
            'failed': None,
        }
    )

    with arbiter as another_name:
        another_name.start_task('running')
        another_name.start_task('completed')
        another_name.end_task('completed')
        another_name.start_task('failed')
        another_name.end_task('failed', False)

    assert_equals(
        arbiter.tasks,
        {'blocked', 'runnable', 'running', 'completed', 'failed'}
    )
    assert_equals(arbiter.blocked, {})
    assert_equals(arbiter.runnable, frozenset())
    assert_equals(arbiter.running, frozenset())
    assert_equals(arbiter.completed, {'completed'})
    assert_equals(
        arbiter.failed,
        {'failed', 'running', 'runnable', 'blocked'}
    )

    # make sure errors aren't suppressed
    arbiter = Arbiter(
        tasks={
            'foo': None,
        }
    )

    with assert_raises(ImportError):
        with arbiter:
            raise ImportError

    assert_equals(arbiter.tasks, {'foo'})
    assert_equals(arbiter.blocked, {})
    assert_equals(arbiter.runnable, frozenset())
    assert_equals(arbiter.running, frozenset())
    assert_equals(arbiter.completed, frozenset())
    assert_equals(arbiter.failed, {'foo'})
