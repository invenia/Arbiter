"""
The base task runner.
"""
from collections import namedtuple

from arbiter.scheduler import Scheduler


Results = namedtuple('Results', ('completed', 'failed'))
TaskResult = namedtuple('TaskResult', ('name', 'successful'))


def task_loop(tasks, execute, wait=None):
    """
    The inner task loop for a task runner.

    execute: A function that runs a task. It should take a task as its
        sole argument, and may optionally return a TaskResult.
    wait: (optional, None) A function to run whenever there aren't any
        runnable tasks (but there are still tasks listed as running).
        If given, this function should take no arguments, and should
        return an iterable of TaskResults.
    """
    with Scheduler(tasks) as scheduler:
        while not scheduler.is_finished():
            task = scheduler.start_task()

            while task is not None:
                result = execute(task)

                # result exists iff execute is synchroous
                if result:
                    scheduler.end_task(result.name, result.successful)

                task = scheduler.start_task()

            if wait:
                for result in wait():
                    scheduler.end_task(result.name, result.successful)

        return Results(scheduler.completed, scheduler.failed)
