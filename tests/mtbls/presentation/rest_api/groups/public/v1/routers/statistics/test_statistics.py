from fastapi import status
from fastapi.testclient import TestClient

from mtbls.presentation.rest_api.core.responses import (
    APIResponse,
)
from mtbls.presentation.rest_api.groups.public.v1.routers.statistics.schemas import (
    StatisticData,
)


def test_statistics_01(public_api_client: TestClient):
    response = public_api_client.get("/public/v2/mtbls-statistics/general/data")
    assert response.status_code == status.HTTP_200_OK
    json_response = response.json()
    result = APIResponse[StatisticData].model_validate(json_response)
    # check local/sqlite/initial_data.sql for initial values
    assert result
    assert result.content
    assert result.content.title == "Data in MetaboLights"
    assert len(result.content.key_values) == 2
