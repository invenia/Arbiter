"""
Tests for the task submodule.
"""
from datetime import timedelta

from nose.tools import assert_equal, assert_true, assert_false, assert_raises


def test_create():
    """
    Test the create function.
    """
    from arbiter.task import create, Task

    function = lambda x: True

    # No dependencies
    assert_equal(
        create('foo', function),
        Task('foo', function, frozenset())
    )

    # Dependencies
    actual = create('foo', function, {'bar', 'baz'})
    assert_equal(actual, Task('foo', function, frozenset(('bar', 'baz'))))
    assert_equal(type(actual.dependencies), frozenset)

    # passing an iterable
    actual = create('foo', function, (name for name in ('bar', 'baz')))
    assert_equal(actual, Task('foo', function, frozenset(('bar', 'baz'))))
    assert_equal(type(actual.dependencies), frozenset)

    # test retrying
    response = [False, False, True]
    task = create('foo', lambda: response.pop(), retries=2, delay=timedelta())
    assert_true(task.function())


def test_retry():
    """
    Test the retry decorator.
    """
    from arbiter.task import retry

    def retrier(retries, responses, delay=timedelta()):
        """
        A function for testing the retry decorator.
        """
        @retry(retries, delay)
        def function():
            """
            Function that is wrapped by retry
            """
            response = responses.pop(0)

            if isinstance(response, BaseException):
                raise response

            return response

        return function()

    # error on invalid retry count
    with assert_raises(TypeError):
        retrier(1.1, [True])

    # no need to retry
    assert_true(retrier(1, [True, False]))

    # no retries allowed
    assert_false(retrier(0, [False, True]))

    # not enough retries
    assert_false(retrier(2, [False, False, False, True]))

    # success on retires
    assert_true(retrier(2, [False, False, True]))

    # don't raise if retries remaining
    assert_false(retrier(2, [Exception(), TypeError(), False]))
    assert_true(retrier(2, [Exception(), False, True]))

    # raise if no retries remain
    with assert_raises(ImportError):
        retrier(2, [TypeError(), AttributeError(), ImportError()])

    # make sure Falsy/Truthy, not False/True only
    assert_equal(retrier(3, [[], timedelta(), None, [1, 2, 3]]), [1, 2, 3])
    assert_equal(retrier(2, [[], 1]), 1)

    # Don't let retry catch kill exceptions
    with assert_raises(KeyboardInterrupt):
        retrier(1, [KeyboardInterrupt(), True])

    with assert_raises(SystemExit):
        retrier(1, [SystemExit(), True])


def test_retry_delay():
    """
    Tests for the delay functionality in the retry wrapper.
    """
    from arbiter.task import retry

    from arbiter import task

    call_stack = []

    def mock_sleep(seconds):
        """
        Mock version of sleep to ensure retry is sleeping the righ
        lengths, and the right period of time.
        """
        call_stack.append(('sleep', seconds))

    task.sleep = mock_sleep

    def retrier(retries, responses, delay=None):
        """
        A function for testing the retry decorator.
        """
        if delay is None:
            kwargs = {}
        else:
            kwargs = {'delay': delay}

        @retry(retries, **kwargs)
        def function():
            """
            Function that is wrapped by retry
            """
            response = responses.pop(0)

            call_stack.append(('call', response))

            if isinstance(response, Exception):
                raise response

            return response

        return function()

    # default settings
    assert_true(retrier(0, [True]))
    assert_equal(call_stack, [('call', True)])

    call_stack = []
    assert_true(retrier(1, [True]))
    assert_equal(call_stack, [('call', True)])

    call_stack = []
    assert_true(retrier(1, [False, True]))
    assert_equal(call_stack, [('call', False), ('sleep', 0), ('call', True)])

    call_stack = []
    assert_false(retrier(1, [False, False]))
    assert_equal(call_stack, [('call', False), ('sleep', 0), ('call', False)])

    # delays
    call_stack = []
    assert_false(retrier(1, [False, False], delay=timedelta(seconds=55)))
    assert_equal(call_stack, [('call', False), ('sleep', 55), ('call', False)])

    # invalid delays
    with assert_raises(AttributeError):
        retrier(1, [False, False], delay=5)

    with assert_raises(TypeError):
        retrier(1, [False, False], delay=timedelta(seconds=-1))

    # duck typing?
    class Delay(object):
        """
        Test that retry is duck typing on delay
        """
        def __init__(self, delay):
            self.delay = delay

        def total_seconds(self):
            """
            All that's really required
            """
            return self.delay

    call_stack = []
    assert_false(retrier(1, [False, False], delay=Delay(10)))
    assert_equal(call_stack, [('call', False), ('sleep', 10), ('call', False)])
