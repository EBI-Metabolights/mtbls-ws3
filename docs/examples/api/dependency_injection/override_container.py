from unittest.mock import Mock

import pytest

from mtbls.application.services.interfaces.http_client import HttpClient
from mtbls.infrastructure.auth.standalone.standalone_authentication_config import (
    StandaloneAuthenticationConfiguration,
)
from mtbls.infrastructure.auth.standalone.standalone_authentication_service import (
    AuthenticationServiceImpl,
)
from mtbls.infrastructure.caching.in_memory.in_memory_cache import InMemoryCacheImpl
from mtbls.infrastructure.system_health_check_service.standalone.standalone_system_health_check_config import (  # noqa E501
    StandaloneSystemHealthCheckConfiguration,
)
from mtbls.infrastructure.system_health_check_service.standalone.standalone_system_health_check_service import (  # noqa E501
    StandaloneSystemHealthCheckService,
)
from mtbls.run.rest_api.submission.containers import Ws3ApplicationContainer
from tests.mtbls.mocks.policy_service.mock_policy_service import MockPolicyService


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

    container.services.policy_service.override(MockPolicyService())

    return container
