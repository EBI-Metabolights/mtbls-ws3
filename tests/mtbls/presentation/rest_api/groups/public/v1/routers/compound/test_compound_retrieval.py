from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from mtbls.domain.entities.compound import Compound
from mtbls.presentation.rest_api.core.responses import APIResponse
from mtbls.presentation.rest_api.groups.public.v1.routers.compound.compound import (
    BATCH_SIZE_LIMIT,
    BatchCompoundResult,
)


def test_get_compound_by_id(public_api_client: TestClient, submission_api_container):
    url = "/public/v2/compound/MTBLC123"
    mock_compound = Compound(
        id="MTBLC123",
        name="aspirin",
        inchiKey="ABC",
    )
    mock_repo = AsyncMock()
    mock_repo.get_compound_by_id.return_value = mock_compound
    submission_api_container.repositories.compound_read_repository.override(mock_repo)

    response = public_api_client.get(url)

    assert response.status_code == 200
    result = APIResponse[Compound].model_validate(response.json())
    assert result.content.id == "MTBLC123"
    assert result.content.name == "aspirin"


class TestBatchCompoundRetrieval:
    """Tests for batch compound retrieval endpoint."""

    def test_batch_retrieval_returns_all_found_compounds(
        self, public_api_client: TestClient, submission_api_container
    ):
        url = "/public/v2/compound/batch"
        mock_compounds = [
            Compound(id="MTBLC1", name="aspirin", inchiKey="ABC"),
            Compound(id="MTBLC2", name="ibuprofen", inchiKey="DEF"),
        ]
        mock_repo = AsyncMock()
        mock_repo.get_compounds_by_ids.return_value = (mock_compounds, [])
        submission_api_container.repositories.compound_read_repository.override(
            mock_repo
        )

        response = public_api_client.post(
            url, json={"compound_ids": ["MTBLC1", "MTBLC2"]}
        )

        assert response.status_code == 200
        result = APIResponse[BatchCompoundResult].model_validate(response.json())
        assert result.content.total_requested == 2
        assert result.content.total_found == 2
        assert len(result.content.compounds) == 2
        assert result.content.missing_ids == []

    def test_batch_retrieval_returns_missing_ids(
        self, public_api_client: TestClient, submission_api_container
    ):
        url = "/public/v2/compound/batch"
        mock_compounds = [
            Compound(id="MTBLC1", name="aspirin", inchiKey="ABC"),
        ]
        mock_repo = AsyncMock()
        mock_repo.get_compounds_by_ids.return_value = (mock_compounds, ["MTBLC999"])
        submission_api_container.repositories.compound_read_repository.override(
            mock_repo
        )

        response = public_api_client.post(
            url, json={"compound_ids": ["MTBLC1", "MTBLC999"]}
        )

        assert response.status_code == 200
        result = APIResponse[BatchCompoundResult].model_validate(response.json())
        assert result.content.total_requested == 2
        assert result.content.total_found == 1
        assert len(result.content.compounds) == 1
        assert result.content.missing_ids == ["MTBLC999"]

    def test_batch_retrieval_deduplicates_ids(
        self, public_api_client: TestClient, submission_api_container
    ):
        url = "/public/v2/compound/batch"
        mock_compounds = [
            Compound(id="MTBLC1", name="aspirin", inchiKey="ABC"),
        ]
        mock_repo = AsyncMock()
        mock_repo.get_compounds_by_ids.return_value = (mock_compounds, [])
        submission_api_container.repositories.compound_read_repository.override(
            mock_repo
        )

        response = public_api_client.post(
            url, json={"compound_ids": ["MTBLC1", "MTBLC1", "MTBLC1"]}
        )

        assert response.status_code == 200
        result = APIResponse[BatchCompoundResult].model_validate(response.json())
        # Should deduplicate to 1 unique ID
        assert result.content.total_requested == 1
        mock_repo.get_compounds_by_ids.assert_called_once_with(["MTBLC1"])

    def test_batch_retrieval_rejects_empty_list(
        self, public_api_client: TestClient, submission_api_container
    ):
        url = "/public/v2/compound/batch"

        response = public_api_client.post(url, json={"compound_ids": []})

        assert response.status_code == 422  # Validation error

    def test_batch_retrieval_rejects_exceeding_limit(
        self, public_api_client: TestClient, submission_api_container
    ):
        url = "/public/v2/compound/batch"
        # Create a list exceeding the limit
        too_many_ids = [f"MTBLC{i}" for i in range(BATCH_SIZE_LIMIT + 1)]

        response = public_api_client.post(url, json={"compound_ids": too_many_ids})

        assert response.status_code == 422  # Validation error

    def test_batch_retrieval_handles_all_missing(
        self, public_api_client: TestClient, submission_api_container
    ):
        url = "/public/v2/compound/batch"
        mock_repo = AsyncMock()
        mock_repo.get_compounds_by_ids.return_value = ([], ["MTBLC999", "MTBLC888"])
        submission_api_container.repositories.compound_read_repository.override(
            mock_repo
        )

        response = public_api_client.post(
            url, json={"compound_ids": ["MTBLC999", "MTBLC888"]}
        )

        assert response.status_code == 200
        result = APIResponse[BatchCompoundResult].model_validate(response.json())
        assert result.content.total_requested == 2
        assert result.content.total_found == 0
        assert len(result.content.compounds) == 0
        assert set(result.content.missing_ids) == {"MTBLC999", "MTBLC888"}
