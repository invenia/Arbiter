"""
The base task runner.
"""
from collections import namedtuple
from time import sleep

from arbiter.scheduler import Scheduler
from arbiter.task import create_task
from arbiter.utils import timedelta_to_seconds

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
    completed = set()
    failed = set()

    with Scheduler(tasks, completed=completed, failed=failed) as scheduler:
        while not scheduler.is_finished():
            task = scheduler.start_task()

            while task is not None:
                result = execute(task)

                # result exists iff execute is synchroous
                if result:
                    if not result.successful and task.retries > 0:
                        newtask = create_task(
                            task.name,
                            task.function,
                            task.dependencies,
                            task.retries - 1,
                            timedelta_to_seconds(task.delay),
                        )

                        scheduler.remove_task(task.name)
                        scheduler.add_task(newtask)
                    else:
                        scheduler.end_task(result.name, result.successful)

                task = scheduler.start_task()

            if wait:
                for result in wait():
                    scheduler.end_task(result.name, result.successful)
            elif scheduler.min_delay > 0:
                sleep(scheduler.min_delay)

    return Results(completed, failed)
