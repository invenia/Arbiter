from datetime import timedelta


def test_tasks_with_handler():
    from arbiter.task import create_task
    from arbiter.sync import run_tasks
    from arbiter.utils import retry_handler

    foo = create_task(
        len,
        [1, 2, 3],
        handler=retry_handler(
            retries=5,
            delay=timedelta(),
            conditions=[]
        ),
    )

    run_tasks((foo,))
