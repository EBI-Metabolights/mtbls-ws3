import pytest

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
from mtbls.run.config_utils import set_application_configuration
from mtbls.run.rest_api.submission.containers import Ws3ApplicationContainer


@pytest.fixture(scope="module")
def submission_api_container() -> Ws3ApplicationContainer:
    container = Ws3ApplicationContainer()
    # Override config
    set_application_configuration(
        container,
        config_file_path="tests/data/config/mtbls-base-config.yaml",
        secrets_file_path="tests/data/config/mtbls-base-config-secrets.yaml",
    )
    # Override Cache
    container.services.cache_service.override(InMemoryCacheImpl())
    # Override Database gateway
    connection_json = container.config.gateways.database.sqlite.connection()
    db_connection = SQLiteDatabaseConnection.model_validate(connection_json)
    container.gateways.database_client.override(SQLiteDatabaseClientImpl(db_connection))
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

    return container
