"""
Custom Exceptions used by arbiter.
"""
from concurrent.futures import TimeoutError


class DependencyException(Exception):
    """
    A base class for exceptions related to task dependencies.
    """


class FailedDependencyError(DependencyException):
    """
    Raised when a task is unable to run due to a dependency for the task
    having failed.
    """


class UnsatisfiedDependencyError(DependencyException):
    """
    Raised when a task is unable to run due to a dependency for the task
    having not run.

    A task with an unsatisfied dependency is:
      * a task dependent on a task that was not passed to run_tasks.
      * a task has a dependency cycle.
          * (e.g., A depends on B and B depends on A).
      * a task has a dependency on a task with an unsatisfied
        dependency.
    """


class UncancelledTaskError(TimeoutError):
    """
    Raised when a TimeoutError occurs, but a task is already running, so
    it can't be cancelled.
    """
    def __init__(self, future):
        super(UncancelledTaskError, self).__init__()

        self.future = future
