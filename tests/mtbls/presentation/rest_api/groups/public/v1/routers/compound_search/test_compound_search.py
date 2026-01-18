from typing import Any
from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from mtbls.domain.entities.search.index_search import IndexSearchResult
from mtbls.presentation.rest_api.core.responses import APIResponse


def test_compound_search_with_basic_query(
    public_api_client: TestClient, submission_api_container
):
    url = "/public/v2/public-compound-index/search"
    query = {
        "query": "aspirin",
        "page": {"current": 1, "size": 25},
        "sort": {"field": "_score", "direction": "desc"},
        "filters": [],
        "facets": {},
    }

    mock_search_result = IndexSearchResult(
        results=[{"id": "MTBLC123", "name": "aspirin"}],
        totalResults=1,
        facets={},
        requestId="req-123",
    )
    mock_gateway = AsyncMock()
    mock_gateway.search.return_value = mock_search_result
    submission_api_container.gateways.elasticsearch_compound_gateway.override(mock_gateway)

    response = public_api_client.post(url, json=query)

    assert response.status_code == 200
    result = APIResponse[IndexSearchResult].model_validate(response.json())
    assert result.content.totalResults == 1
    assert result.content.results[0]["id"] == "MTBLC123"


def test_get_compound_index_mapping(
    public_api_client: TestClient, submission_api_container
):
    url = "/public/v2/public-compound-index/mapping"
    mock_mapping = {"public-compound-index": {"mappings": {"properties": {}}}}

    mock_gateway = AsyncMock()
    mock_gateway.get_index_mapping.return_value = mock_mapping
    submission_api_container.gateways.elasticsearch_compound_gateway.override(mock_gateway)

    response = public_api_client.get(url)

    assert response.status_code == 200
    result = APIResponse[Any].model_validate(response.json())
    assert result.content == mock_mapping


def test_compound_search_with_study_ids_filter(
    public_api_client: TestClient, submission_api_container
):
    """Test that study_ids parameter is accepted and passed to the gateway."""
    url = "/public/v2/public-compound-index/search"
    query = {
        "query": "aspirin",
        "page": {"current": 1, "size": 25},
        "sort": {"field": "_score", "direction": "desc"},
        "filters": [],
        "facets": {},
        "study_ids": ["MTBLS1", "MTBLS2"],
    }

    mock_search_result = IndexSearchResult(
        results=[
            {"id": "MTBLC123", "name": "aspirin", "studyIds": ["MTBLS1"]},
            {"id": "MTBLC456", "name": "acetylsalicylic acid", "studyIds": ["MTBLS2"]},
        ],
        totalResults=2,
        facets={},
        requestId="req-456",
    )
    mock_gateway = AsyncMock()
    mock_gateway.search.return_value = mock_search_result
    submission_api_container.gateways.elasticsearch_compound_gateway.override(mock_gateway)

    response = public_api_client.post(url, json=query)

    assert response.status_code == 200
    result = APIResponse[IndexSearchResult].model_validate(response.json())
    assert result.content.totalResults == 2
    # Verify the gateway was called with study_ids
    call_args = mock_gateway.search.call_args
    assert call_args.kwargs["query"].study_ids == ["MTBLS1", "MTBLS2"]


def test_compound_search_with_study_id_pattern_in_query(
    public_api_client: TestClient, submission_api_container
):
    """Test that a query matching MTBLS pattern is accepted."""
    url = "/public/v2/public-compound-index/search"
    query = {
        "query": "MTBLS123",
        "page": {"current": 1, "size": 25},
        "filters": [],
    }

    mock_search_result = IndexSearchResult(
        results=[{"id": "MTBLC789", "name": "glucose", "studyIds": ["MTBLS123"]}],
        totalResults=1,
        facets={},
        requestId="req-789",
    )
    mock_gateway = AsyncMock()
    mock_gateway.search.return_value = mock_search_result
    submission_api_container.gateways.elasticsearch_compound_gateway.override(mock_gateway)

    response = public_api_client.post(url, json=query)

    assert response.status_code == 200
    result = APIResponse[IndexSearchResult].model_validate(response.json())
    assert result.content.totalResults == 1
