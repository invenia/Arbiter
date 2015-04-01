"""
Task creation/generation
"""
from collections import namedtuple


Task = namedtuple(
    'Task',
    ('name', 'function', 'dependencies'),
)


def create_task(name, function, dependencies=()):
    """
    Create a task object

    name: The name of the task.
    function: The actual task function. It should take no arguments,
        and return a False-y value if it fails.
    dependencies: (optional, ()) Any dependencies that this task relies
        on.
    """
    return Task(name, function, frozenset(dependencies))
