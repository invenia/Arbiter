"""
Synchronous task runner
"""
from arbiter.base import task_loop, TaskResult


__all__ = ('run_tasks',)


def run_tasks(tasks):
    """
    Run an iterable of tasks.

    tasks: The iterable of tasks
    """
    return task_loop(tasks, execute)


def execute(function, name):
    """
    Execute a task, returning a TaskResult
    """
    try:
        return TaskResult(name, True, None, function())
    except Exception as exc:
        return TaskResult(name, False, exc, None)
