"""
Tests for the runner submodule.
"""
from concurrent.futures import Future

from nose.tools import assert_false, assert_equal, assert_raises


def test_when_ready():
    """
    Test the when_ready wrapper function.
    """
    from arbiter.runner import when_ready

    def function(should_raise=False, exception=Exception, **kwargs):
        """
        A function for testing when_ready
        """
        if should_raise:
            raise exception('foo')

        return kwargs

    # No dependencies
    wrapped = when_ready(function, [])

    assert_equal(wrapped.__doc__, function.__doc__)
    assert_equal(
        wrapped(False, foo='bar', lorem='ipsum'),
        {'foo': 'bar', 'lorem': 'ipsum'}
    )

    # make sure exceptions don't fall through
    assert_false(wrapped(True, foo='bar', lorem='ipsum'))

    # make sure this isn't true for KeyboardInterrupt
    with assert_raises(KeyboardInterrupt):
        wrapped(True, KeyboardInterrupt)

    with assert_raises(SystemExit):
        wrapped(True, SystemExit)

    # Failed dependency
    futures = []
    for result in (True, True, True, False, True):
        future = Future()
        future.set_result(result)
        futures.append(future)

    wrapped = when_ready(function, futures)

    assert_equal(wrapped.__doc__, function.__doc__)
    assert_false(wrapped(False, foo='bar', lorem='ipsum'))
    assert_false(wrapped(True, foo='bar', lorem='ipsum'))
    assert_false(wrapped(True, KeyboardInterrupt))
    assert_false(wrapped(True, SystemExit))

    # All successful dependencies
    futures = []
    for result in (True, True, True, True):
        future = Future()
        future.set_result(result)
        futures.append(future)

    wrapped = when_ready(function, futures)

    assert_equal(wrapped.__doc__, function.__doc__)
    assert_equal(
        wrapped(False, foo='bar', lorem='ipsum'),
        {'foo': 'bar', 'lorem': 'ipsum'}
    )
    assert_false(wrapped(True, foo='bar', lorem='ipsum'))

    with assert_raises(KeyboardInterrupt):
        wrapped(True, KeyboardInterrupt)

    with assert_raises(SystemExit):
        wrapped(True, SystemExit)
