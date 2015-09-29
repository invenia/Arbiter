from nose.tools import assert_equals, assert_raises, assert_false, assert_true

from arbiter.utils import RetryCondition, retry_loop, retry


def retry_on_value_error(exc):
    """
    Function that returns True (retries) on a
    ValueError with the message 'do_retry'.

    Args:
        exc (Exception): The exception object to check against.

    Returns:
        (bool): Whether this should trigger a retry.
    """
    if isinstance(exc, ValueError) and exc.args[0] == 'do_retry':
        return True
    else:
        return False


def retry_on_do_retry(value):
    """
    Function that returns True (retries) on the
    value 'do_retry'.

    Args:
        value: The value to check against.

    Returns:
        (bool): Whether this should trigger a retry.
    """
    if value == 'do_retry':
        return True
    else:
        return False


def test_retry_conditions():
    """
    Test retry conditions.
    """
    assert_raises(ValueError, RetryCondition, retry_on_value_error, kind='Val')

    val_err_cond = RetryCondition(retry_on_value_error)

    assert_false(val_err_cond.on_value('do_retry'))
    assert_false(val_err_cond.on_exception(Exception('foo')))
    assert_true(val_err_cond.on_exception(ValueError('do_retry')))

    val_cond = RetryCondition(retry_on_do_retry, kind='value')
    assert_false(val_cond.on_exception(ValueError('do_retry')))
    assert_false(val_cond.on_value('blah'))
    assert_true(val_cond.on_value('do_retry'))


def test_retry_loop():
    """
    Test retry loop.

    NOTES: a value runs is used for checking how many times
    myfunc has been run. In order for this to be edited both inside
    and outside myfunc we make it global.
    """
    global runs
    conditions = [
        RetryCondition(retry_on_value_error),
        RetryCondition(retry_on_do_retry, kind='value')
    ]
    runs = 0

    def myfunc_err(fail_num=2):
        """
        Update runs. If runs is less than the fail_num
        then raise the ValueError and trigger the retry conditions.
        """
        global runs

        runs = runs + 1
        if runs < fail_num:
            raise ValueError('do_retry')

    def myfunc_val(fail_num=2):
        """
        Update runs. If runs is less than the fail_num
        then return 'do_retry' and trigger the retry conditions.
        """
        global runs

        runs = runs + 1
        if runs < fail_num:
            return 'do_retry'
        else:
            return True

    assert_raises(TypeError, retry_loop, 5.4, 0, [], myfunc_err)
    assert_raises(TypeError, retry_loop, 5, -1, [], myfunc_err)

    retry_loop(5, 0, conditions, myfunc_err)
    assert_equals(runs, 2)

    runs = 0
    retry_loop(5, 0, conditions, myfunc_val)
    assert_equals(runs, 2)

    runs = 0
    assert_raises(ValueError, retry_loop, 0, 0, [], myfunc_err)

    runs = 0
    assert_raises(ValueError, retry_loop, 0, 0, conditions, myfunc_err)

    runs = 0
    assert_raises(ValueError, retry_loop, 0, 0, conditions, myfunc_val)


def test_retry_decorator():
    """
    Test the retry decorator.
    """
    global runs
    conditions = [RetryCondition(retry_on_do_retry, kind='value')]
    runs = 0

    @retry(retries=5, conditions=conditions)
    def myfunc(fail_num=2):
        """
        Update runs. If runs is less than the fail_num
        then return 'do_retry' and trigger the retry conditions.
        """
        global runs

        runs = runs + 1
        if runs < fail_num:
            return 'do_retry'
        else:
            return True

    myfunc()
    assert_equals(runs, 2)


# NOTE: The retry handler is currently tested in test_task.py as it is
# already pretty integrated into tasks.
