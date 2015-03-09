"""
Actual task runners using Arbiter for dependency-solving
"""
from collections import namedtuple

from arbiter.scheduler import Scheduler


Task = namedtuple(
    'Task',
    ('name', 'function', 'dependencies'),
)
Result = namedtuple('Result', ('completed', 'failed'))


class Arbiter(object):
    """
    A serial task-runner that handles dependencies
    """
    def __init__(self, tasks):
        self._tasks = {}
        self.scheduler = Scheduler()

        for task in tasks:
            self.scheduler.add_task(task.name, task.dependencies)
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
                name = scheduler.start_task()

                while name is not None:
                    self.execute(self._tasks[name])

                    name = scheduler.start_task()

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


# @six.add_metaclass(ABCMeta)
# class BaseRunner(object):
#     """
#     The base task runner.
#     """
#     def __init__(self, retries=0, delay=None):
#         if delay is None:
#             delay = timedelta()

#         self._default_retries = retries
#         self._default_delay = delay

#         self._tasks = {}

#     def add_task(self, name, function, dependencies=None,
#                  retries=None, delay=None):
#         """
#         Add a task to the runner.

#         name: The name of the task.
#         function: The actual task. The function should take no
#             arguments, and should return a False-y value if it fails
#         dependencies: (optional, None) The names of any tasks this task
#             depends on.
#         retries: (optional, None) The number of retries that should be
#             attempted should the task fail. If not given, the runner's
#             default number of retries should be used.
#         delay: (optional, None) A timedelta representing the delay
#             between a task failing, and a retry occurring. If not given,
#             the runner's default delay will be used.
#         """
#         if dependencies is None:
#             dependencies = ()

#         if retries is None:
#             retries = self._default_retries

#         if delay is None:
#             delay = self._default_delay

#         self._tasks[name] = Task(function, dependencies, retries, delay)

#     def get_task(self, name):
#         """
#         Get a task.

#         name: The name of the task.
#         """
#         return self._tasks.get(name)

#     def run_tasks(self):
#         """
#         Run the
#         """


# class Runner(object):
#     """
#     Base task runner. Runs tasks one-at-a-time, using Arbiter to resolve
#     dependencies
#     """
#     def __init__(self):
#         self._arbiter = Arbiter()
