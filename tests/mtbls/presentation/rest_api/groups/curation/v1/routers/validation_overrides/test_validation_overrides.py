import datetime
import json
from pathlib import Path
from typing import Any, Callable, Generator, Union
from unittest.mock import AsyncMock

import pytest
from fastapi import status
from fastapi.testclient import TestClient

from mtbls.application.services.interfaces.async_task.async_task_service import (
    IdGenerator,
)
from mtbls.application.services.interfaces.auth.authentication_service import (
    AuthenticationService,
)
from mtbls.application.services.interfaces.policy_service import PolicyService
from mtbls.domain.entities.study import StudyOutput
from mtbls.domain.entities.user import UserOutput
from mtbls.domain.enums.study_status import StudyStatus
from mtbls.domain.enums.token_type import TokenType
from mtbls.domain.enums.user_role import UserRole
from mtbls.domain.enums.user_status import UserStatus
from mtbls.domain.shared.permission import ResourcePermission
from mtbls.domain.shared.validator.policy import (
    PolicyMessage,
    PolicyResult,
    PolicyResultList,
    ValidationResult,
)
from mtbls.domain.shared.validator.types import PolicyMessageType
from mtbls.presentation.rest_api.core.responses import (
    APIResponse,
    Status,
)
from mtbls.presentation.rest_api.groups.submission.v1.routers.validation_tasks.models import (  # noqa: E501
    GetValidationResponse,
)
from mtbls.run.rest_api.submission.containers import Ws3ApplicationContainer


class MockApiClient:
    def __init__(
        self,
        client: TestClient,
        container: Ws3ApplicationContainer,
    ):
        self.client = client
        self.container = container


class MockApiClientContext:
    def __init__(
        self,
        api_client: MockApiClient,
        study: StudyOutput,
    ):
        self.api_client = api_client
        self.study = study


@pytest.fixture(scope="module")
def mock_submission_api_client(
    submission_api_client, submission_api_container, validation_result_01
) -> Generator[Any, Any, MockApiClient]:
    container = submission_api_container
    policy_service: PolicyService = container.services.policy_service()
    validate_study = policy_service.validate_study

    async def mock_validate(*args, **kwargs):
        return validation_result_01

    policy_service.validate_study = mock_validate
    yield MockApiClient(client=submission_api_client, container=container)
    policy_service.validate_study = validate_study


@pytest.fixture(scope="module")
def jwt_token(mock_submission_api_client: MockApiClient):
    services = mock_submission_api_client.container.services
    auth_service: AuthenticationService = services.authentication_service()
    return_value = "metabolights-help@ebi.ac.uk"
    mock_auth = AsyncMock(return_value=return_value)

    validate_token = auth_service.validate_token
    authenticate_with_token = auth_service.authenticate_with_token
    authenticate_with_password = auth_service.authenticate_with_password

    auth_service.authenticate_with_token = mock_auth
    auth_service.authenticate_with_password = mock_auth

    async def mock_validate_token(token_type: TokenType, token: str):
        return return_value

    auth_service.validate_token = mock_validate_token

    yield "jwt-token"

    auth_service.validate_token = validate_token
    auth_service.authenticate_with_token = authenticate_with_token
    auth_service.authenticate_with_password = authenticate_with_password


MockApiClientCallable = Callable[
    [
        Union[None, UserOutput],
        Union[None, StudyOutput],
        Union[None, ResourcePermission],
        bool,
    ],
    MockApiClient,
]
MockApiClientContextCallable = Callable[[str], MockApiClientContext]


@pytest.fixture(scope="function")
def get_api_client_for_user_and_study(
    mock_submission_api_client: MockApiClient,
) -> MockApiClientCallable:
    def get_updated_client(
        user: Union[None, UserOutput] = None,
        study: Union[None, StudyOutput] = None,
        permissions: Union[None, ResourcePermission] = None,
        is_owner: bool = True,
    ) -> MockApiClientContext:
        if not user:
            user = UserOutput(
                username="test@ebi.ac.uk",
                email="test@ebi.ac.uk",
                role=UserRole.SUBMITTER,
                status=UserStatus.ACTIVE,
                id_=54321,
                first_name="Test",
                last_name="Test Password",
            )
        if not study:
            now = datetime.datetime.now(datetime.timezone.utc)
            study = StudyOutput(
                id_=800001,
                accession_number="MTBLS800001",
                status=StudyStatus.PROVISIONAL,
                obfuscation_code="test1",
                release_date=now,
                submission_date=now,
            )
        if not permissions:
            permissions = ResourcePermission(update=True, read=True, delete=True)
        return MockApiClientContext(api_client=mock_submission_api_client, study=study)

    return get_updated_client


@pytest.fixture
def user_submitter() -> UserOutput:
    return UserOutput(
        username="test@ebi.ac.uk",
        email="test@ebi.ac.uk",
        role=UserRole.SUBMITTER,
        status=UserStatus.ACTIVE,
        id_=54321,
        first_name="Test",
        last_name="Test Password",
    )


@pytest.fixture
def rest_api_submitter_context(
    get_api_client_for_user_and_study: MockApiClientContextCallable,
    user_submitter: UserOutput,
) -> MockApiClientContext:
    def get_context(resource_id: str) -> MockApiClientContext:
        now = datetime.datetime.now(datetime.timezone.utc)
        study = StudyOutput(
            id_=1,
            accession_number=resource_id,
            status=StudyStatus.PROVISIONAL,
            obfuscation_code="test" + resource_id,
            release_date=now,
            submission_date=now,
        )
        permissions = ResourcePermission(update=True, read=True, delete=True)
        context: MockApiClientContext = get_api_client_for_user_and_study(
            user=user_submitter, study=study, permissions=permissions, is_owner=True
        )
        return context

    return get_context


@pytest.fixture(scope="module")
def summary_result_01() -> PolicyResultList:
    with Path("tests/data/json/validation/result_list_MTBLS1.json").open() as f:
        return json.load(f)


@pytest.fixture(scope="module")
def validation_result_01() -> ValidationResult:
    return ValidationResult(
        violations=[
            PolicyMessage(
                identifier="rule_11",
                type=PolicyMessageType.ERROR,
                title="rule title",
                description="rule description",
            )
        ],
        summary=[
            PolicyMessage(
                identifier="summary_rule_11",
                type=PolicyMessageType.INFO,
                title="summary title",
                description="summary description",
            )
        ],
    )


@pytest.fixture
def policy_result_list_01(validation_result_01: ValidationResult) -> PolicyResultList:
    def get_list(resource_id: str):
        return PolicyResultList(
            results=[
                PolicyResult(resource_id=resource_id, messages=validation_result_01)
            ]
        )

    return get_list


class MockIdGenerator(IdGenerator):
    def __init__(self, task_id: str):
        self.task_id = task_id

    def generate_unique_id(self) -> str:
        return self.task_id


class TestGetOverridesV1:
    @pytest.mark.asyncio
    async def test_get_overrides_01(
        self, mock_submission_api_client: MockApiClient, jwt_token: str
    ):
        """
        Return override list
        expected: return empty list
        """
        resource_id = "MTBLS800001"
        api_client = mock_submission_api_client

        client = api_client.client
        headers = {"Authorization": f"Bearer {jwt_token}"}
        response = client.get(
            f"/curation/v2/validation-overrides/{resource_id}", headers=headers
        )
        assert response.status_code == status.HTTP_200_OK
        json_response = response.json()
        result = APIResponse[GetValidationResponse].model_validate(json_response)
        assert result.status == Status.SUCCESS
        assert not result.error_message
        assert result.success_message
