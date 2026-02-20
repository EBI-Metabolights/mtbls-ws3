from unittest.mock import AsyncMock

import pytest

from mtbls.application.services.interfaces.cache_service import CacheService
from mtbls.application.services.interfaces.http_client import HttpClient
from mtbls.domain.entities.validation.validation_configuration import (
    BaseOntologyValidation,
    OntologyValidationType,
)
from mtbls.infrastructure.ontology_search.ols.ols_configuration import OlsConfiguration
from mtbls.infrastructure.ontology_search.ols.ols_search_service import (
    OlsOntologySearchService,
)


@pytest.fixture
def ols_config() -> OlsConfiguration:
    return OlsConfiguration()


@pytest.fixture
def cache_service() -> CacheService:
    service: CacheService = AsyncMock(spec=CacheService)
    return service


@pytest.fixture
def http_client() -> HttpClient:
    http_client: HttpClient = AsyncMock(spec=HttpClient)
    return http_client


@pytest.fixture
def ols_mock_search_service(
    ols_config: OlsConfiguration,
    cache_service: CacheService,
    http_client: HttpClient,
) -> OlsOntologySearchService:
    return OlsOntologySearchService(
        config=ols_config, cache_service=cache_service, http_client=http_client
    )


@pytest.fixture
def ols_search_service(
    ols_config: OlsConfiguration,
    cache_service: CacheService,
    http_client: HttpClient,
) -> OlsOntologySearchService:
    return OlsOntologySearchService(
        config=ols_config, cache_service=cache_service, http_client=http_client
    )


@pytest.mark.asyncio
async def test_search_01(
    ols_mock_search_service: OlsOntologySearchService,
):
    result = await ols_mock_search_service.search(
        "liter",
        rule=BaseOntologyValidation(
            rule_name="Test 1",
            field_name="test 1",
            validation_type=OntologyValidationType.SELECTED_ONTOLOGY,
            ontologies=None,
        ),
    )
    assert not result.success
