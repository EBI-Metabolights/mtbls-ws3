from fastapi import status
from fastapi.testclient import TestClient

from mtbls.presentation.rest_api.core.models import Version
from mtbls.presentation.rest_api.core.responses import APIResponse


def test_api(submission_api_client: TestClient):
    response = submission_api_client.get("/api")
    assert response.status_code == status.HTTP_200_OK


def test_root(submission_api_client: TestClient):
    response = submission_api_client.get("/")
    assert response.status_code == status.HTTP_200_OK


def test_version(submission_api_client: TestClient):
    response = submission_api_client.get("/version")
    assert response.status_code == status.HTTP_200_OK
    json_response = response.json()
    result = APIResponse[Version].model_validate(json_response)
    assert result


def test_configuration(submission_api_client: TestClient):
    response = submission_api_client.get("/system/v2/transfer-status")
    assert response.status_code == status.HTTP_200_OK
    json_response = response.json()
    result = APIResponse[Version].model_validate(json_response)
    assert result
