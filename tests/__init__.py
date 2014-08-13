"""
Tests for the arbiter module
"""
from nose.tools import assert_equal


def test_paths():
    """
    Test that functions are avaialable from the base module.
    """
    import arbiter

    from arbiter.task import create_task
    assert_equal(arbiter.create_task, create_task)
