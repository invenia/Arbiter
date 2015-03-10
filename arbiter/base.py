"""
Actual task runners using Arbiter for dependency-solving
"""
from collections import namedtuple

from arbiter.scheduler import Scheduler


Result = namedtuple('Result', ('completed', 'failed'))


class Arbiter(object):
    """
    A serial task-runner that handles dependencies
    """
    def __init__(self, tasks):
        self._tasks = {}
        self.scheduler = Scheduler()

        for task in tasks:
            self.scheduler.add_task(task)
            self._tasks[task.name] = task

    def run(self):
        """
        Run the Arbiter
        """
        return self.event_loop()

    def event_loop(self):
        """
        The loop in which tests actually run.
        """
        with self.scheduler as scheduler:
            while not scheduler.is_finished():
                task = scheduler.start_task()

                while task is not None:
                    self.execute(task)

                    task = scheduler.start_task()

                self.wait()

        return Result(
            completed=self.scheduler.completed,
            failed=self.scheduler.failed
        )

    def execute(self, task):
        """
        Execute a task.

        task: The task to execute
        """
        self.scheduler.end_task(task.name, task.function())

    def wait(self):
        """
        Wait until tasks complete
        """
        pass
