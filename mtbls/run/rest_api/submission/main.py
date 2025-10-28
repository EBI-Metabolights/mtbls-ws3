import logging
import uuid
from contextlib import asynccontextmanager
from typing import Union

import uvicorn
from asgi_correlation_id import CorrelationIdMiddleware
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from starlette.middleware.authentication import AuthenticationMiddleware

import mtbls
from mtbls.application.context.request_tracker import get_request_tracker
from mtbls.presentation.rest_api.core import core_router
from mtbls.presentation.rest_api.core.auth_backend import AuthBackend
from mtbls.presentation.rest_api.core.authorization_middleware import (
    AuthorizationMiddleware,
)
from mtbls.presentation.rest_api.core.exception import exception_handler
from mtbls.presentation.rest_api.core.models import ApiServerConfiguration
from mtbls.presentation.rest_api.shared.router_utils import add_routers
from mtbls.run.config_utils import (
    get_application_config_files,
    set_application_configuration,
)
from mtbls.run.module_utils import load_modules
from mtbls.run.rest_api.submission import initialization
from mtbls.run.rest_api.submission.containers import Ws3ApplicationContainer
from mtbls.run.subscribe import find_async_task_modules, find_injectable_modules

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(fast_api: FastAPI):
    logger.info(
        "Application initialization. %s",
        "Debug mode is enabled." if fast_api.debug else "Debug mode is disabled.",
    )
    await initialization.init_application()
    logger.info("Application is initialized.")
    yield


def update_container(
    config_file_path: str,
    secrets_file_path: str,
    app_name: str = "default",
    queue_names: Union[None, list[str]] = None,
    container: Union[None, Ws3ApplicationContainer] = None,
) -> Ws3ApplicationContainer:
    if not queue_names:
        queue_names = ["common"]
    global logger  # noqa: PLW0603

    if not container:
        raise TypeError("Init container is not defined")

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

    async_module_names = {x.__name__ for x in async_task_modules}
    injectable_module_names = {x.__name__ for x in injectable_modules}
    wirable_module_name = list(async_module_names.union(injectable_module_names))

    container.wire(packages=[mtbls.__name__])
    container.wire(
        modules=[
            __name__,
            initialization.__name__,
            *wirable_module_name,
        ]
    )
    logger.info(
        "Registered modules contain async tasks. %s",
        [x.__name__ for x in async_task_modules],
    )
    logger.info(
        "Registered modules contain dependency injections. %s",
        [x.__name__ for x in injectable_modules],
    )
    return container


def create_app(
    config_file_path: str,
    secrets_file_path: str,
    app_name="default",
    queue_names: Union[None, list[str]] = None,
    db_connection_pool_size=3,
    container=None,
):
    if not queue_names:
        queue_names = ["common"]
    container = update_container(
        config_file_path=config_file_path,
        secrets_file_path=secrets_file_path,
        app_name=app_name,
        queue_names=queue_names,
        container=container,
    )
    container.gateways.runtime_config.db_pool_size.override(db_connection_pool_size)
    server_config: ApiServerConfiguration = container.api_server_config()
    version: str = mtbls.__version__

    server_info = server_config.server_info.model_dump()
    swagger_ui_oauth2_redirect_url = "/api/oauth2-redirect"
    app = FastAPI(
        lifespan=lifespan,
        openapi_url="/openapi.json",
        docs_url=None,
        redoc_url=None,
        version=version,
        **server_info,
        swagger_ui_oauth2_redirect_url=swagger_ui_oauth2_redirect_url,
    )
    app.mount("/resources", StaticFiles(directory="resources"), name="resources")
    app.add_exception_handler(Exception, exception_handler)
    app.include_router(core_router.router)
    for group in server_config.api_groups:
        if group.enabled:
            for router_path in group.router_paths:
                logger.debug("Search routers within %s", router_path)
                add_routers(application=app, root_path=router_path)

    app.add_middleware(
        AuthorizationMiddleware,
        authorization_service=container.services.authorization_service(),
        request_tracker=get_request_tracker(),
        authorized_endpoints=container.config.run.submission.authorized_endpoints(),
    )
    auth_backend = AuthBackend(
        authentication_service=container.services.authentication_service(),
        user_read_repository=container.repositories.user_read_repository(),
    )
    app.add_middleware(AuthenticationMiddleware, backend=auth_backend)
    app.add_middleware(
        CorrelationIdMiddleware,
        header_name="X-Request-ID",
        generator=lambda: str(uuid.uuid4()),
    )

    if server_config.cors.origins:
        origin_regex = "|".join(server_config.cors.origins)
        app.add_middleware(
            CORSMiddleware,
            allow_origin_regex=f"({origin_regex})",
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
            expose_headers=["X-Request-ID"],
            # expose_headers="*",
        )
    return app, container


def get_app(
    config_file_path: None | str,
    secrets_file_path: None | str,
    container: Union[None, Ws3ApplicationContainer] = None,
    app_name="default",
    queue_names: Union[None, list[str]] = None,
    db_connection_pool_size=3,
):
    if not container:
        container = Ws3ApplicationContainer()
    fast_app, _ = create_app(
        config_file_path=config_file_path,
        secrets_file_path=secrets_file_path,
        app_name=app_name,
        queue_names=queue_names,
        db_connection_pool_size=db_connection_pool_size,
        container=container,
    )
    return fast_app


if __name__ == "__main__":
    app_container: Ws3ApplicationContainer = Ws3ApplicationContainer()
    config_file_path, secrets_file_path = get_application_config_files()
    fast_app = get_app(
        config_file_path=config_file_path,
        secrets_file_path=secrets_file_path,
        container=app_container,
    )
    server_configuration: ApiServerConfiguration = app_container.api_server_config()
    config = server_configuration.server_info
    log_config = app_container.config.run.submission.logging()

    try:
        uvicorn.run(
            fast_app,
            host="0.0.0.0",
            port=server_configuration.port,
            root_path=config.root_path,
            log_config=log_config,
            forwarded_allow_ips="*",
        )
    except Exception as ex:
        raise ex
    finally:
        app_container.shutdown_resources()
