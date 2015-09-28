"""
The base task runner.
"""
from collections import namedtuple
from functools import partial
from arbiter.scheduler import Scheduler
from arbiter.task import Task, TaskStore


Results = namedtuple('Results', ('completed', 'failed', 'exceptions'))
TaskResult = namedtuple(
    'TaskResult',
    ('name', 'successful', 'exception', 'data')
)


def task_loop(tasks, execute, wait=None, store=TaskStore()):
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
    exceptions = []

    def collect(task):
        args = []
        kwargs = {}

        for arg in task.args:
            if isinstance(arg, Task):
                args.append(store.get(arg.name))
            else:
                args.append(arg)

        for key in task.kwargs:
            if isinstance(task.kwargs[key], Task):
                kwargs[key] = store.get(task.kwargs[key].name)
            else:
                kwargs[key] = task.kwargs[key]

        return args, kwargs

    def complete(scheduler, result):
        store.put(result.name, result.data)
        scheduler.end_task(result.name, result.successful)
        if result.exception:
            exceptions.append(result.exception)

    with Scheduler(tasks, completed=completed, failed=failed) as scheduler:
        while not scheduler.is_finished():
            task = scheduler.start_task()

            while task is not None:
                # Collect any dependent results
                args, kwargs = collect(task)
                func = partial(task.function, *args, **kwargs)
                if task.handler:
                    func = partial(task.handler, func)

                result = execute(func, task.name)

                # result exists iff execute is synchroous
                if result:
                    complete(scheduler, result)

                task = scheduler.start_task()

            if wait:
                for result in wait():
                    complete(scheduler, result)

    # TODO: if in debug mode print out all failed tasks?
    return Results(completed, failed, exceptions)
