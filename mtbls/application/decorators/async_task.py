import logging

from mtbls.application.context.async_task_registry import (
    ASYNC_TASK_APP_NAME,
    ASYNC_TASK_QUEUE,
    ASYNC_TASK_REGISTRY,
)
from mtbls.domain.shared.async_task.async_task_description import AsyncTaskDescription

logger = logging.getLogger(__name__)


def async_task(
    app_name: ASYNC_TASK_APP_NAME = "default", queue: ASYNC_TASK_QUEUE = "common"
):
    def inner(task_method):
        task_name = task_method.__module__ + "." + task_method.__name__

        def wrapper(**kwargs):
            return task_method(**kwargs)

        executor = AsyncTaskDescription(wrapper, task_name=task_name, queue=queue)
        if app_name not in ASYNC_TASK_REGISTRY:
            ASYNC_TASK_REGISTRY[app_name] = {}
        ASYNC_TASK_REGISTRY[app_name][task_name] = executor
        logger.info(
            "Task for application '%s', '%s' queue: %s", app_name, queue, task_name
        )
        return executor

    return inner
