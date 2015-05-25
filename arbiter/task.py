"""
Task creation/generation
"""
from collections import namedtuple


Task = namedtuple(
    'Task',
    ('name', 'function', 'dependencies', 'expected'),
)


def create_task(name, function, dependencies=(), expected=True):
    """
    Create a task object

    name: The name of the task.
    function: The actual task function. It should take no arguments,
        and return a False-y value if it fails.
    dependencies: (optional, ()) Any dependencies that this task relies
        on.
    expected: (optional, True) The expected return value of a task if it
        succeeds.
    """
    return Task(name, function, frozenset(dependencies), expected)
