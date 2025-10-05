import asyncio
import copy
import json
from pathlib import Path
from typing import Any, Generator

import pytest
import yaml
from fastapi.testclient import TestClient
from metabolights_utils.models.metabolights.model import MetabolightsStudyModel

from local.policy_service.mock_policy_service import MockPolicyService
from local.sqlite.init_db import create_test_sqlite_db
from mtbls.infrastructure.auth.standalone.standalone_authentication_config import (
    StandaloneAuthenticationConfiguration,
)
from mtbls.infrastructure.auth.standalone.standalone_authentication_service import (
    AuthenticationServiceImpl,
)
from mtbls.infrastructure.caching.in_memory.in_memory_cache import InMemoryCacheImpl
from mtbls.infrastructure.persistence.db.sqlite.config import SQLiteDatabaseConnection
from mtbls.infrastructure.persistence.db.sqlite.db_client_impl import (
    SQLiteDatabaseClientImpl,
)
from mtbls.infrastructure.pub_sub.threading.thread_manager_impl import (
    ThreadingAsyncTaskService,
)
from mtbls.infrastructure.system_health_check_service.standalone.standalone_system_health_check_config import (
    StandaloneSystemHealthCheckConfiguration,
)
from mtbls.infrastructure.system_health_check_service.standalone.standalone_system_health_check_service import (
    StandaloneSystemHealthCheckService,
)
from mtbls.run.rest_api.submission.containers import Ws3ApplicationContainer
from mtbls.run.rest_api.submission.main import create_app


@pytest.fixture(scope="session")
def submission_api_config() -> dict[str, Any]:
    file_path = Path("local/configs/submission/submission_base_config.yaml")
    with file_path.open("r") as f:
        return yaml.safe_load(f)


@pytest.fixture(scope="session")
def local_env_container(
    submission_api_config,
) -> Generator[Any, Any, Ws3ApplicationContainer]:
    container = Ws3ApplicationContainer()

    # Override config
    container.config.override(submission_api_config)
    connection_json = container.config.gateways.database.sqlite.connection()

    db_connection = SQLiteDatabaseConnection.model_validate(connection_json)
    container.gateways.database_client.override(SQLiteDatabaseClientImpl(db_connection))

    scheme = db_connection.url_scheme
    db_file_path = Path(db_connection.file_path)
    asyncio.run(
        create_test_sqlite_db(
            db_file_path,
            Path("local/sqlite/initial_data.sql"),
            scheme=scheme,
        )
    )
    yield container

    db_file_path.unlink(missing_ok=True)


@pytest.fixture(scope="module")
def submission_api_container(local_env_container) -> Ws3ApplicationContainer:
    container = local_env_container
    standalone_heath_check_config_str = (
        container.config.services.system_health_check.standalone()
    )
    health_check_config = StandaloneSystemHealthCheckConfiguration.model_validate(
        standalone_heath_check_config_str
    )
    container.services.system_health_check_service.override(
        StandaloneSystemHealthCheckService(config=health_check_config)
    )
    # Override Cache
    container.services.cache_service.override(InMemoryCacheImpl())
    # Override Authentication service
    standalone_auth_config_str = container.config.services.authentication.standalone()
    standalone_auth_config = StandaloneAuthenticationConfiguration.model_validate(
        standalone_auth_config_str
    )
    container.services.authentication_service.override(
        AuthenticationServiceImpl(
            config=standalone_auth_config,
            cache_service=container.services.cache_service(),
            user_read_repository=container.repositories.user_read_repository(),
        )
    )
    # container.repositories.study_file_repository.override(
    #     FileSystemStudyFileRepository()
    # )
    container.services.policy_service.override(MockPolicyService())

    return container


@pytest.fixture(scope="module")
def submission_api_client(submission_api_container):
    app, _ = create_app(container=submission_api_container, db_connection_pool_size=3)
    # Override async task service
    async_task_registry = submission_api_container.core.async_task_registry()
    submission_api_container.services.async_task_service.override(
        ThreadingAsyncTaskService(
            app_name="default",
            queue_names=["common", "validation", "datamover", "compute"],
            async_task_registry=async_task_registry,
        )
    )

    return TestClient(
        app=app,
        base_url="http://wwwdev.ebi.ac.uk",
        raise_server_exceptions=False,
    )


@pytest.fixture(scope="session")
def templates_json() -> dict[str, Any]:
    with Path("tests/data/json/templates.json").open("r") as f:
        return json.load(f)


@pytest.fixture(scope="session")
def control_lists_json() -> dict[str, Any]:
    with Path("tests/data/json/control_lists.json").open("r") as f:
        return json.load(f)


@pytest.fixture(scope="session")
def metabolights_model_json() -> dict[str, Any]:
    with Path("tests/data/json/model_MTBLS1.json").open("r") as f:
        return json.load(f)


@pytest.fixture(scope="session")
def metabolights_model_base_MTBLS1_json() -> dict[str, Any]:
    with Path("tests/data/json/model_MTBLS1_base.json").open("r") as f:
        return json.load(f)


@pytest.fixture(scope="session")
def ms_metabolights_model_json() -> dict[str, Any]:
    with Path("tests/data/json/model_MTBLS5195.json").open("r") as f:
        return json.load(f)


@pytest.fixture(scope="session")
def ms_metabolights_model_base_json() -> dict[str, Any]:
    with Path("tests/data/json/model_MTBLS5195_base.json").open("r") as f:
        return json.load(f)


@pytest.fixture(scope="function")
def metabolights_model_base_MTBLS1(
    metabolights_model_base_MTBLS1_json: dict[str, Any],
) -> dict[str, Any]:
    return MetabolightsStudyModel.model_validate(metabolights_model_base_MTBLS1_json)


@pytest.fixture(scope="function")
def ms_metabolights_model_base_01(
    ms_metabolights_model_base_json: dict[str, Any],
) -> dict[str, Any]:
    return MetabolightsStudyModel.model_validate(ms_metabolights_model_base_json)


@pytest.fixture(scope="function")
def ms_metabolights_model(ms_metabolights_model_json: dict[str, Any]) -> dict[str, Any]:
    return MetabolightsStudyModel.model_validate(ms_metabolights_model_json)


@pytest.fixture(scope="function")
def metabolights_model(metabolights_model_json: dict[str, Any]) -> dict[str, Any]:
    return MetabolightsStudyModel.model_validate(metabolights_model_json)


@pytest.fixture(scope="function")
def control_lists(control_lists_json: dict[str, Any]) -> dict[str, Any]:
    return copy.deepcopy(control_lists_json)


@pytest.fixture(scope="function")
def templates(templates_json: dict[str, Any]) -> dict[str, Any]:
    return copy.deepcopy(templates_json)
