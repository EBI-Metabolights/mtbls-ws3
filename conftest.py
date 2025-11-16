import asyncio
import json
from pathlib import Path
from typing import Any, Generator
from unittest.mock import Mock

import pytest
from fastapi.testclient import TestClient
from metabolights_utils.models.metabolights.model import MetabolightsStudyModel

from mtbls.application.services.interfaces.http_client import HttpClient
from mtbls.domain.entities.validation.validation_configuration import (
    FileTemplates,
    ValidationControls,
)
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
from mtbls.infrastructure.system_health_check_service.standalone.standalone_system_health_check_config import (  # noqa E501
    StandaloneSystemHealthCheckConfiguration,
)
from mtbls.infrastructure.system_health_check_service.standalone.standalone_system_health_check_service import (  # noqa E501
    StandaloneSystemHealthCheckService,
)
from mtbls.run.config_utils import set_application_configuration
from mtbls.run.rest_api.submission.containers import Ws3ApplicationContainer
from mtbls.run.rest_api.submission.main import create_app
from tests.data.sqlite.init_db import create_test_sqlite_db
from tests.mtbls.mocks.policy_service.mock_policy_service import MockPolicyService


@pytest.fixture(scope="session")
def local_config_file() -> Generator[Any, Any, str]:
    return "tests/data/config/mtbls-base-config.yaml"


@pytest.fixture(scope="session")
def local_secrets_file() -> Generator[Any, Any, str]:
    return "tests/data/config/mtbls-base-config-secrets.yaml"


@pytest.fixture(scope="session")
def local_env_container() -> Generator[Any, Any, Ws3ApplicationContainer]:
    container = Ws3ApplicationContainer()
    set_application_configuration(
        container,
        config_file_path="tests/data/config/mtbls-base-config.yaml",
        secrets_file_path="tests/data/config/mtbls-base-config-secrets.yaml",
    )
    connection_json = container.config.gateways.database.sqlite.connection()

    db_connection = SQLiteDatabaseConnection.model_validate(connection_json)
    container.gateways.database_client.override(SQLiteDatabaseClientImpl(db_connection))

    scheme = db_connection.url_scheme
    db_file_path = Path(db_connection.file_path)
    asyncio.run(
        create_test_sqlite_db(
            db_file_path,
            Path("tests/data/sqlite/initial_data.sql"),
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
    container.gateways.http_client.override(Mock(spec=HttpClient))

    container.services.system_health_check_service.override(
        StandaloneSystemHealthCheckService(
            config=health_check_config, http_client=container.gateways.http_client
        )
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
def submission_api_client(
    submission_api_container, local_config_file, local_secrets_file
):
    app, _ = create_app(
        config_file_path=local_config_file,
        secrets_file_path=local_secrets_file,
        container=submission_api_container,
        db_connection_pool_size=3,
    )
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


@pytest.fixture(scope="module")
def public_api_client(submission_api_container, local_config_file, local_secrets_file):
    app, _ = create_app(
        config_file_path=local_config_file,
        secrets_file_path=local_secrets_file,
        container=submission_api_container,
        db_connection_pool_size=3,
    )
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
        return json.load(f)["result"]


@pytest.fixture(scope="session")
def control_lists_json() -> dict[str, Any]:
    with Path("tests/data/json/control_lists.json").open("r") as f:
        return json.load(f)["result"]


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
def control_lists(control_lists_json: dict[str, Any]) -> ValidationControls:
    return ValidationControls.model_validate(control_lists_json, by_alias=True)


@pytest.fixture(scope="function")
def templates(templates_json: dict[str, Any]) -> FileTemplates:
    return FileTemplates.model_validate(templates_json, by_alias=True)
