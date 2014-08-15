"""
Custom Exceptions used by arbiter.
"""
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
