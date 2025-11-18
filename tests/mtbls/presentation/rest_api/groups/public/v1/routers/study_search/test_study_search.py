
from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from mtbls.domain.entities.search.study.index_search import IndexSearchResult
from mtbls.presentation.rest_api.core.responses import APIResponse


def test_study_search_with_basic_query(
    public_api_client: TestClient, submission_api_container
):
    url = "/public/v2/public-study-index/search"
    query = {
        "query": "bean",
        "page": {"current": 1, "size": 25},
        "sort": {"field": "_score", "direction": "desc"},
        "filters": [],
        "facets": {},
    }

    mock_search_result = IndexSearchResult(
        results=[{"studyId": "MTBLS123", "title": "Beans study"}],
        totalResults=1,
        facets={},
        requestId="req-123",
    )
    mock_gateway = AsyncMock()
    mock_gateway.search.return_value = mock_search_result
    submission_api_container.gateways.elasticsearch_study_gateway.override(mock_gateway)

    response = public_api_client.post(url, json=query)

    assert response.status_code == 200
    result = APIResponse[IndexSearchResult].model_validate(response.json())
    assert result.content.totalResults == 1
    assert result.content.results[0]["studyId"] == "MTBLS123"
