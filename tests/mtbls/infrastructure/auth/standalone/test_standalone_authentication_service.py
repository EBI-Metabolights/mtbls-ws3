from unittest.mock import AsyncMock

import pytest

from mtbls.application.services.interfaces.cache_service import CacheService
from mtbls.application.services.interfaces.repositories.user.user_read_repository import (  # noqa: E501
    UserReadRepository,
)
from mtbls.infrastructure.auth.standalone.standalone_authentication_config import (
    StandaloneAuthenticationConfiguration,
)
from mtbls.infrastructure.auth.standalone.standalone_authentication_service import (
    AuthenticationServiceImpl,
)


@pytest.fixture
def user_read_repository() -> UserReadRepository:
    service = AsyncMock(spec=UserReadRepository)
    return service


@pytest.fixture
def cache_service() -> CacheService:
    service: CacheService = AsyncMock(spec=CacheService)
    return service


@pytest.fixture
def config() -> StandaloneAuthenticationConfiguration:
    return StandaloneAuthenticationConfiguration(
        application_secret_key="test",
        access_token_hash_algorithm="HS256",
        access_token_expires_delta_in_minutes=24 * 60,
        revocation_management_enabled=False,
        revoked_access_token_prefix="revoked_jwt_token",
    )


@pytest.fixture
def authentication_service(
    config: StandaloneAuthenticationConfiguration,
    cache_service: CacheService,
    user_read_repository: UserReadRepository,
) -> AuthenticationServiceImpl:
    return AuthenticationServiceImpl(
        config=config,
        cache_service=cache_service,
        user_read_repository=user_read_repository,
    )


@pytest.mark.asyncio
async def test_update_single_column_01(
    authentication_service: AuthenticationServiceImpl,
):
    result = await authentication_service.get_password_sha1_hash("test")
    assert result
