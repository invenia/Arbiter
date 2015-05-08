"""
Task creation/generation
"""
from collections import namedtuple
from datetime import datetime, timedelta

Task = namedtuple(
    'Task',
    ('name', 'function', 'dependencies', 'retries', 'delay', 'timestamp'),
)


def create_task(name, function, dependencies=(), retries=0, delay=0):
    """
    Create a task object

    name: The name of the task.
    function: The actual task function. It should take no arguments,
        and return a False-y value if it fails.
    dependencies: (optional, ()) Any dependencies that this task relies
        on.
    """
    return Task(
        name,
        function,
        frozenset(dependencies),
        retries,
        delay=timedelta(seconds=delay),
        timestamp=datetime.now()
    )
