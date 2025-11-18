import asyncio
from pathlib import Path

import uvicorn

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
from mtbls.presentation.rest_api.core.models import ApiServerConfiguration
from mtbls.run.config_utils import (
    set_application_configuration,
)
from mtbls.run.rest_api.submission.containers import Ws3ApplicationContainer
from mtbls.run.rest_api.submission.main import get_app
from tests.data.sqlite.init_db import create_test_sqlite_db
from tests.mtbls.mocks.policy_service.mock_policy_service import MockPolicyService

if __name__ == "__main__":
    container = Ws3ApplicationContainer()
    config_file_path = "mtbls-ws-config.yaml"
    secrets_file_path = ".secrets/ws3-secrets.yaml"
    set_application_configuration(
        container,
        config_file_path=config_file_path,
        secrets_file_path=secrets_file_path,
    )

    # Override database client to use sqlite for testing
    connection_json = container.config.gateways.database.sqlite.connection()
    db_connection = SQLiteDatabaseConnection.model_validate(connection_json)
    container.gateways.database_client.override(SQLiteDatabaseClientImpl(db_connection))
    scheme = db_connection.url_scheme
    db_file_path = Path(db_connection.file_path)

    # mongodb_connection_json = container.config.gateways.database.mongodb.connection()
    # mongodb_connection = MongoDbConnection.model_validate(mongodb_connection_json)
    # container.repositories.investigation_object_repository.override(
    #     MongoDbInvestigationObjectRepository(mongodb_connection)
    # )
    # container.repositories.isa_table_object_repository.override(
    #     MongoDbIsaTableObjectRepository(mongodb_connection)
    # )
    # container.repositories.isa_table_row_object_repository.override(
    #     MongoDbIsaTableRowObjectRepository(mongodb_connection)
    # )
    # container.repositories.file_registry.override(
    #     MongoDbStudyFileRepository(
    #         connection=mongodb_connection,
    #     )
    # )

    # container.services.study_metadata_service_factory.override(
    #     MongoDbStudyMetadataServiceFactory(
    #         study_read_repository=container.repositories.study_read_repository(),
    #         file_registry=container.repositories.file_registry(),
    #         investigation_object_repository=container.repositories.investigation_object_repository(),  # noqa: E501
    #         isa_table_object_repository=container.repositories.isa_table_object_repository(),  # noqa: E501
    #         isa_table_row_object_repository=container.repositories.isa_table_row_object_repository(),  # noqa: E501
    #     user_read_repository=container.repositories.user_read_repository

    #     )
    # )

    # Override system health check service
    standalone_health_check_config_str = (
        container.config.services.system_health_check.standalone()
    )
    health_check_config = StandaloneSystemHealthCheckConfiguration.model_validate(
        standalone_health_check_config_str
    )
    container.services.system_health_check_service.override(
        StandaloneSystemHealthCheckService(
            config=health_check_config, http_client=container.gateways.http_client
        )
    )
    # Override Cache and use in-memory cache for testing
    container.services.cache_service.override(InMemoryCacheImpl())
    # Override Authentication service
    standalone_auth_config_str = container.config.services.authentication.standalone()
    standalone_auth_config = StandaloneAuthenticationConfiguration.model_validate(
        standalone_auth_config_str
    )

    # Override Authentication service and use standalone authentication for testing
    container.services.authentication_service.override(
        AuthenticationServiceImpl(
            config=standalone_auth_config,
            cache_service=container.services.cache_service(),
            user_read_repository=container.repositories.user_read_repository(),
        )
    )

    # Override Policy service and use mock policy service for testing
    container.services.policy_service.override(MockPolicyService())
    fast_app = get_app(
        config_file_path=config_file_path,
        secrets_file_path=secrets_file_path,
        container=container,
    )

    container.services.async_task_service.override(
        ThreadingAsyncTaskService(
            app_name="default",
            queue_names=["common", "validation", "datamover", "compute", ""],
            async_task_registry=container.core.async_task_registry(),
        )
    )

    # Create the test sqlite database
    asyncio.run(
        create_test_sqlite_db(
            db_file_path,
            Path("tests/data/sqlite/initial_data.sql"),
            scheme=scheme,
        )
    )
    server_configuration: ApiServerConfiguration = container.api_server_config()
    config = server_configuration.server_info
    log_config = container.config.run.submission.logging()
    uvicorn.run(
        fast_app,
        host="0.0.0.0",
        port=server_configuration.port,
        root_path=config.root_path,
        log_config=log_config,
    )
    container.shutdown_resources()
