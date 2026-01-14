from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from mtbls.domain.entities.compound import Compound
from mtbls.presentation.rest_api.core.responses import APIResponse


def test_get_compound_by_id(
    public_api_client: TestClient, submission_api_container
):
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
