import asyncio
import logging
from logging.config import dictConfig
from typing import Any, Sequence, Union

from celery.signals import setup_logging, worker_process_init, worker_process_shutdown
from dependency_injector.wiring import Provide, inject

import mtbls
from mtbls.application.remote_tasks import get_worker_loop, set_worker_loop
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


_container: None | Ws3WorkerApplicationContainer = None
_secrets_file_path: None | str = None
_config_file_path: None | str = None


async def update_container(
    config_file_path: str,
    secrets_file_path: str,
    app_name="default",
    queue_names: Union[None, Sequence[str]] = None,
    container: Union[None, Ws3WorkerApplicationContainer] = None,
) -> Ws3WorkerApplicationContainer:
    success = set_application_configuration(
        container, config_file_path, secrets_file_path
    )
    if not success:
        raise Exception("Configuration update task failed.")
    container.init_resources()
    queue_names = queue_names if queue_names else ["common"]
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
    await initialization.init_application(
        test_async_task_service=False, test_policy_service=True
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


@worker_process_shutdown.connect
def on_worker_shutdown(**kwargs):
    global _container
    _loop = get_worker_loop()
    if _container and _loop:
        _loop.run_until_complete(_container.shutdown_resources())
        _loop.close()


@worker_process_init.connect
def on_worker_init(**kwargs):
    global _container, _config_file_path, _secrets_file_path

    _loop = asyncio.new_event_loop()
    set_worker_loop(_loop)
    _container = Ws3WorkerApplicationContainer()

    coroutine = update_container(
        config_file_path=_config_file_path,
        secrets_file_path=_secrets_file_path,
        container=_container,
        app_name="default",
        queue_names=["common"],
    )

    asyncio.set_event_loop(_loop)
    _loop.run_until_complete(coroutine)


if __name__ == "__main__":
    container: Ws3WorkerApplicationContainer = Ws3WorkerApplicationContainer()
    loop = asyncio.new_event_loop()
    set_worker_loop(loop)

    _config_file_path, _secrets_file_path = get_application_config_files()
    success = set_application_configuration(
        container, _config_file_path, _secrets_file_path
    )
    if not success:
        raise Exception("Configuration update task failed.")
    asyncio.set_event_loop(loop)
    loop.run_until_complete(
        update_container(
            config_file_path=_config_file_path,
            secrets_file_path=_secrets_file_path,
            container=container,
            app_name="default",
            queue_names=["common"],
        )
    )
    container.wire(modules=[__name__])
    celery_app = get_worker_app(container)

    celery_app.start(
        argv=["worker", "-Q", "common", "--concurrency=1", "--loglevel=INFO"]
    )
