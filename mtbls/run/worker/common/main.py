import asyncio
import logging
from logging.config import dictConfig
from typing import Any, Sequence, Union

from celery.signals import setup_logging
from dependency_injector.wiring import Provide, inject

import mtbls
from mtbls.application.services.interfaces.async_task.utils import (
    get_async_task_registry,
)
from mtbls.infrastructure.pub_sub.celery.celery_impl import CeleryAsyncTaskService
from mtbls.infrastructure.pub_sub.connection.redis import RedisConnectionProvider
from mtbls.run.config_utils import (
    get_application_config_files,
    set_application_configuration,
)
from mtbls.run.module_utils import load_modules
from mtbls.run.rest_api.submission import initialization
from mtbls.run.subscribe import find_async_task_modules, find_injectable_modules
from mtbls.run.worker.common.containers import Ws3WorkerApplicationContainer

logger = logging.getLogger(__name__)


@setup_logging.connect()
@inject
def config_loggers(
    *args,
    config: dict[str, Any] = Provide["config.run.common_worker.logging"],
    **kwargs,
):
    dictConfig(config)


def update_container(
    config_file_path: str,
    secrets_file_path: str,
    app_name="default",
    queue_names: Union[None, Sequence[str]] = None,
    container: Union[None, Ws3WorkerApplicationContainer] = None,
) -> Ws3WorkerApplicationContainer:
    global logger  # noqa: PLW0603
    queue_names = queue_names if queue_names else ["common"]

    success = set_application_configuration(
        container, config_file_path, secrets_file_path
    )
    if not success:
        raise Exception("Configuration update task failed.")
    container.init_resources()

    module_config = container.module_config()
    modules = find_async_task_modules(app_name=app_name, queue_names=queue_names)
    async_task_modules = load_modules(modules, module_config)
    modules = find_injectable_modules()
    injectable_modules = load_modules(modules, module_config)

    container.wire(packages=[mtbls.__name__])

    container.wire(modules=[initialization.__name__])
    container.wire(modules=[__name__, *async_task_modules, *injectable_modules])
    logger.info(
        "Registered modules contain async tasks. %s",
        [x.__name__ for x in async_task_modules],
    )
    logger.info(
        "Registered modules contain dependency injections. %s",
        [x.__name__ for x in injectable_modules],
    )
    return container


def get_worker_app(container: Ws3WorkerApplicationContainer):
    async_task_registry = get_async_task_registry()

    rc = container.gateways.config.cache.redis.connection()
    redis_connection_provider = RedisConnectionProvider(rc)
    manager = CeleryAsyncTaskService(
        broker=redis_connection_provider,
        backend=redis_connection_provider,
        default_queue="common",
        queue_names=["common"],
        async_task_registry=async_task_registry,
    )

    return manager.app


def get_celery_worker_app(config_file_path: None | str, secrets_file_path: None | str):
    container = Ws3WorkerApplicationContainer()
    update_container(
        config_file_path=config_file_path,
        secrets_file_path=secrets_file_path,
        container=container,
        app_name="default",
        queue_names=["common"],
    )
    asyncio.run(
        initialization.init_application(
            test_async_task_service=False, test_policy_service=True
        )
    )
    return get_worker_app(container)


def main():
    config_file_path, secrets_file_path = get_application_config_files()

    app = get_celery_worker_app(
        config_file_path=config_file_path, secrets_file_path=secrets_file_path
    )
    app.start(
        argv=[
            "worker",
            "-Q",
            "common",
            "--concurrency=1",
            "--loglevel=INFO",
        ]
    )


if __name__ == "__main__":
    main()
