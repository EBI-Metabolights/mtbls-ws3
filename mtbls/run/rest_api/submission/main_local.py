import os
from pathlib import Path

import uvicorn
import yaml

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
from mtbls.presentation.rest_api.core.models import ApiServerConfiguration
from mtbls.run.config_utils import (
    CONFIG_FILE_ENVIRONMENT_VARIABLE_NAME,
    DEFAULT_CONFIG_FILE_PATH,
    DEFAULT_SECRETS_FILE_PATH,
    SECRET_FILE_ENVIRONMENT_VARIABLE_NAME,
)
from mtbls.run.rest_api.submission.containers import Ws3ApplicationContainer
from mtbls.run.rest_api.submission.main import get_app

# TODO: FIX IT
if __name__ == "__main__":
    container = Ws3ApplicationContainer()
    with Path("tests/data/config/mtbls-base-config.yaml").open("r") as f:
        config = yaml.safe_load(f)
    with Path("tests/data/config/.secrets.yaml").open("r") as f:
        config_secrets = yaml.safe_load(f)
    # Override config
    container.config.override(config)
    container.secrets.override(config_secrets)
    # Override Cache
    container.services.cache_service.override(InMemoryCacheImpl())
    # Override Database gateway
    connection_json = container.config.gateways.database.sqlite.connection()
    db_connection = SQLiteDatabaseConnection.model_validate(connection_json)
    container.gateways.database_client.override(SQLiteDatabaseClientImpl(db_connection))

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
    config_file_path = os.environ.get(
        CONFIG_FILE_ENVIRONMENT_VARIABLE_NAME, DEFAULT_CONFIG_FILE_PATH
    )
    secrets_file_path = os.environ.get(
        SECRET_FILE_ENVIRONMENT_VARIABLE_NAME, DEFAULT_SECRETS_FILE_PATH
    )
    fast_app = get_app(
        config_file_path=config_file_path,
        secrets_file_path=secrets_file_path,
        initial_container=container,
    )
    container.services.async_task_service.override(
        ThreadingAsyncTaskService(
            app_name="default",
            queue_names=["common", "validation", "datamover", "compute", ""],
            async_task_registry=container.core.async_task_registry(),
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
