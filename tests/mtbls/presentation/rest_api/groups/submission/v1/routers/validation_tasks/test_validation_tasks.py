import datetime
import json
from pathlib import Path
from typing import Any, Callable, Generator, Union
from unittest.mock import AsyncMock, Mock

import pytest
from fastapi import status
from fastapi.testclient import TestClient

from mtbls.application.remote_tasks.common.ping import ping_connection
from mtbls.application.services.interfaces.async_task.async_task_result import (
    AsyncTaskResult,
)
from mtbls.application.services.interfaces.async_task.async_task_service import (
    AsyncTaskService,
    IdGenerator,
)
from mtbls.application.services.interfaces.auth.authentication_service import (
    AuthenticationService,
)
from mtbls.application.services.interfaces.cache_service import CacheService
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
from mtbls.infrastructure.pub_sub.threading.thread_manager_impl import (
    ThreadingAsyncTaskService,
)
from mtbls.presentation.rest_api.core.responses import (
    APIErrorResponse,
    APIResponse,
    Status,
)
from mtbls.presentation.rest_api.groups.submission.v1.routers.validation_tasks.models import (  # noqa: E501
    GetValidationResponse,
    StartValidationResponse,
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

    async def mock_validate_token(
        token_type: TokenType, token: str, username: str = None
    ):
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


class TestGetValidationsV1:
    @pytest.mark.asyncio
    async def test_validation_not_valid_task_01(
        self, mock_submission_api_client: MockApiClient, jwt_token: str
    ):
        """
        There is no running task
        expected: 404 not found error
        """
        resource_id = "MTBLS800001"
        api_client = mock_submission_api_client
        task_id = "test-task"
        services = api_client.container.services
        async_task_service: AsyncTaskService = services.async_task_service()

        executor = await async_task_service.get_async_task(
            ping_connection,
            resource_id=resource_id,
            apply_modifiers=False,
            id_generator=MockIdGenerator(task_id=task_id),
        )
        await executor.start()
        client = api_client.client
        headers = {"Authorization": f"Bearer {jwt_token}"}
        response = client.get(
            f"/submissions/v2/validations/{resource_id}/{task_id}", headers=headers
        )
        assert response.status_code == status.HTTP_404_NOT_FOUND
        json_response = response.json()
        result = APIResponse[GetValidationResponse].model_validate(json_response)
        assert result.status == Status.ERROR
        assert result.error_message

    def test_validation_not_valid_task_02(
        self, mock_submission_api_client: MockApiClient, jwt_token: str
    ):
        """
        There is an invalid task id input and there is no running task
        expected: 404 not found error
        """
        resource_id = "MTBLS800001"
        task_id = "123456-987650-54321-x"

        headers = {"Authorization": f"Bearer {jwt_token}"}
        response = mock_submission_api_client.client.get(
            f"/submissions/v2/validations/{resource_id}/{task_id}", headers=headers
        )
        assert response.status_code == status.HTTP_404_NOT_FOUND
        json_response = response.json()
        result = APIResponse[GetValidationResponse].model_validate(json_response)
        assert result.status == Status.ERROR
        assert result.error_message

    @pytest.mark.asyncio
    async def test_validation_running_task_01(
        self, mock_submission_api_client: MockApiClient, jwt_token: str
    ):
        """
        There is a running task. Return its status
        expected: task status without content
        """
        resource_id = "MTBLS800001"
        task_id = "123456-987650-54321-x"
        key = f"validation_task:current:{resource_id}"
        services = mock_submission_api_client.container.services
        cache_service: CacheService = services.cache_service()
        await cache_service.set_value(key, value=task_id, expiration_time_in_seconds=60)
        async_task_service: ThreadingAsyncTaskService = services.async_task_service()

        task_result = Mock(AsyncTaskResult)
        task_result.get_id.return_value = task_id
        task_result.get_status.return_value = "RUNNING"
        task_result.is_ready.return_value = False
        task_result.is_successful.return_value = None
        async_task_service.async_task_results_dict[task_id] = task_result

        headers = {"Authorization": f"Bearer {jwt_token}"}
        response = mock_submission_api_client.client.get(
            f"/submissions/v2/validations/{resource_id}/{task_id}", headers=headers
        )
        await cache_service.delete_key(key)
        del async_task_service.async_task_results_dict[task_id]

        assert response.status_code == status.HTTP_200_OK
        json_response = response.json()
        result = APIResponse[GetValidationResponse].model_validate(json_response)
        assert result.status == Status.SUCCESS
        assert not result.error_message
        assert result.content
        assert not result.content.task_result
        assert not result.content.task.ready
        assert result.content.task.task_status == "RUNNING"
        assert result.content.task.task_id == task_id

    @pytest.mark.asyncio
    async def test_validation_completed_task_with_error_01(
        self, mock_submission_api_client: MockApiClient, jwt_token: str
    ):
        """
        There is a task but task is failed.
        expected: task status without content
        """
        resource_id = "MTBLS800001"
        task_id = "123456-987650-54321-x"
        key = f"validation_task:current:{resource_id}"
        services = mock_submission_api_client.container.services
        cache_service: CacheService = services.cache_service()
        await cache_service.set_value(key, value=task_id, expiration_time_in_seconds=60)
        async_task_service: ThreadingAsyncTaskService = services.async_task_service()

        task_result = Mock(AsyncTaskResult)
        task_result.get_id.return_value = task_id
        task_result.get_status.return_value = "FAILED"
        task_result.is_ready.return_value = True
        task_result.is_successful.return_value = False
        async_task_service.async_task_results_dict[task_id] = task_result

        headers = {"Authorization": f"Bearer {jwt_token}"}
        response = mock_submission_api_client.client.get(
            f"/submissions/v2/validations/{resource_id}/{task_id}",
            headers=headers,
        )
        await cache_service.delete_key(key)
        del async_task_service.async_task_results_dict[task_id]

        assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
        json_response = response.json()
        result = APIResponse[GetValidationResponse].model_validate(json_response)
        assert result.status == Status.ERROR
        assert task_id in result.error_message
        assert resource_id in result.error_message
        assert not result.content

    @pytest.mark.asyncio
    async def test_validation_completed_task_with_error_02(
        self, mock_submission_api_client: MockApiClient, jwt_token: str
    ):
        """
        There is a task id input and it is failed.
        expected: task status without content
        """
        resource_id = "MTBLS800001"
        task_id = "123456-987650-54321-x"
        key = f"validation_task:current:{resource_id}"
        services = mock_submission_api_client.container.services
        cache_service: CacheService = services.cache_service()
        await cache_service.set_value(key, value=task_id, expiration_time_in_seconds=60)
        async_task_service: ThreadingAsyncTaskService = services.async_task_service()

        task_result = Mock(AsyncTaskResult)
        task_result.get_id.return_value = task_id
        task_result.get_status.return_value = "FAILED"
        task_result.is_ready.return_value = True
        task_result.is_successful.return_value = False
        async_task_service.async_task_results_dict[task_id] = task_result

        headers = {"Authorization": f"Bearer {jwt_token}"}
        response = mock_submission_api_client.client.get(
            f"/submissions/v2/validations/{resource_id}/{task_id}",
            headers=headers,
        )
        await cache_service.delete_key(key)
        del async_task_service.async_task_results_dict[task_id]

        assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
        json_response = response.json()
        result = APIResponse[GetValidationResponse].model_validate(json_response)
        assert result.status == Status.ERROR
        assert task_id in result.error_message
        assert resource_id in result.error_message
        assert not result.content

    @pytest.mark.asyncio
    async def test_validation_completed_task_success_01(
        self,
        mock_submission_api_client: MockApiClient,
        jwt_token: str,
        policy_result_list_01: PolicyResultList,
    ):
        """There is no task id input and there is a task completed.
        expected: task status and content
        """
        task_id = "123456-987650-54321-success-01"
        target = "tests/data/storages/rw/studies/internal-files/MTBLS800001/validation-history"  # noqa: E501
        self.remove_task_results(task_id, target)
        try:
            resource_id = "MTBLS800001"
            key = f"validation_task:current:{resource_id}"
            policy_result_list = policy_result_list_01(resource_id)
            services = mock_submission_api_client.container.services
            cache_service: CacheService = services.cache_service()
            await cache_service.set_value(
                key, value=task_id, expiration_time_in_seconds=60
            )

            async_task_service: ThreadingAsyncTaskService = (
                services.async_task_service()
            )
            task_result = Mock(AsyncTaskResult)
            task_result.get_id.return_value = task_id
            task_result.get_status.return_value = "SUCCESS"
            task_result.is_ready.return_value = True
            task_result.is_successful.return_value = True
            task_result.get.return_value = policy_result_list
            async_task_service.async_task_results_dict[task_id] = task_result

            headers = {"Authorization": f"Bearer {jwt_token}"}
            response = mock_submission_api_client.client.get(
                f"/submissions/v2/validations/{resource_id}/{task_id}", headers=headers
            )
            await cache_service.delete_key(key)
            del async_task_service.async_task_results_dict[task_id]

            assert response.status_code == status.HTTP_200_OK
            json_response = response.json()
            result = APIResponse[GetValidationResponse].model_validate(json_response)
            assert result.status == Status.SUCCESS
            assert not result.error_message
            assert result.content
            assert result.content.task
            assert result.content.task_result
        finally:
            self.remove_task_results(task_id, target)

    def remove_task_results(
        self, task_id: str, target: str, fail_if_not_exist: bool = False
    ):
        target_path = Path(target)
        found = False
        for file in target_path.iterdir():
            if task_id in file.name:
                file.unlink()
                found = True
        if not found and fail_if_not_exist:
            raise ValueError(task_id, target, "File not found")

    @pytest.mark.asyncio
    async def test_validation_completed_task_success_02(
        self,
        mock_submission_api_client: MockApiClient,
        jwt_token: str,
    ):
        """
        There is a task id input and there is a task in history repository.
        expected: task status and content
        """
        resource_id = "MTBLS800001"
        task_id = "840e1fea-b0e1-44a0-8a9a-968923e4d7fa"

        headers = {"Authorization": f"Bearer {jwt_token}"}
        response = mock_submission_api_client.client.get(
            f"/submissions/v2/validations/{resource_id}/{task_id}", headers=headers
        )

        assert response.status_code == status.HTTP_200_OK
        json_response = response.json()
        result = APIResponse[GetValidationResponse].model_validate(json_response)
        assert result.status == Status.SUCCESS
        assert not result.error_message
        assert result.content
        assert result.content.task
        assert result.content.task_result

    @pytest.mark.asyncio
    async def test_validation_completed_task_success_03(
        self,
        mock_submission_api_client: MockApiClient,
        jwt_token: str,
        policy_result_list_01: PolicyResultList,
    ):
        """
        There is a task id input.
        It is not in history but there is a completed task.
        expected: task status and content
        """
        resource_id = "MTBLS800001"
        key = f"validation_task:current:{resource_id}"

        task_id = "83599aa5-7130-48ad-95e9-36e4ac51405c-234"
        target = "tests/data/storages/rw/studies/internal-files/MTBLS800001/validation-history"  # noqa: E501
        services = mock_submission_api_client.container.services
        cache_service: CacheService = services.cache_service()
        await cache_service.set_value(key, value=task_id, expiration_time_in_seconds=60)
        self.remove_task_results(task_id, target)
        try:
            policy_result_list = policy_result_list_01(resource_id)
            services = mock_submission_api_client.container.services

            async_task_service: ThreadingAsyncTaskService = (
                services.async_task_service()
            )
            task_result = Mock(AsyncTaskResult)
            task_result.get_id.return_value = task_id
            task_result.get_status.return_value = "SUCCESS"
            task_result.is_ready.return_value = True
            task_result.is_successful.return_value = True
            task_result.get.return_value = policy_result_list
            async_task_service.async_task_results_dict[task_id] = task_result

            headers = {"Authorization": f"Bearer {jwt_token}"}
            response = mock_submission_api_client.client.get(
                f"/submissions/v2/validations/{resource_id}/{task_id}", headers=headers
            )
            del async_task_service.async_task_results_dict[task_id]

            assert response.status_code == status.HTTP_200_OK
            json_response = response.json()
            result = APIResponse[GetValidationResponse].model_validate(json_response)
            assert result.status == Status.SUCCESS
            assert not result.error_message
            assert result.content
            assert result.content.task
            assert result.content.task_result
        finally:
            self.remove_task_results(task_id, target, fail_if_not_exist=True)

    @pytest.mark.asyncio
    async def test_validation_completed_task_success_04(
        self,
        mock_submission_api_client: MockApiClient,
        jwt_token: str,
        policy_result_list_01: PolicyResultList,
    ):
        """
        There is a task id input.
        It is not in history but there is a completed task.
        expected: task status and content
        """
        resource_id = "MTBLS800001"
        key = f"validation_task:current:{resource_id}"

        task_id = "83599aa5-7130-48ad-95e9-36e4ac51405c-234"
        target = "tests/data/storages/rw/studies/internal-files/MTBLS800001/validation-history"  # noqa: E501
        services = mock_submission_api_client.container.services
        cache_service: CacheService = services.cache_service()
        await cache_service.set_value(key, value=task_id, expiration_time_in_seconds=60)
        self.remove_task_results(task_id, target)
        try:
            policy_result_list = policy_result_list_01(resource_id)
            services = mock_submission_api_client.container.services

            async_task_service: ThreadingAsyncTaskService = (
                services.async_task_service()
            )
            task_result = Mock(AsyncTaskResult)
            task_result.get_id.return_value = task_id
            task_result.get_status.return_value = "SUCCESS"
            task_result.is_ready.return_value = True
            task_result.is_successful.return_value = True
            task_result.get.return_value = policy_result_list
            async_task_service.async_task_results_dict[task_id] = task_result

            headers = {"Authorization": f"Bearer {jwt_token}", "Task-Id": task_id}
            response = mock_submission_api_client.client.get(
                f"/submissions/v2/validations/{resource_id}/{task_id}", headers=headers
            )
            del async_task_service.async_task_results_dict[task_id]

            assert response.status_code == status.HTTP_200_OK
            json_response = response.json()
            result = APIResponse[GetValidationResponse].model_validate(json_response)
            assert result.status == Status.SUCCESS
            assert not result.error_message
            assert result.content
            assert result.content.task
            assert result.content.task_result
        finally:
            self.remove_task_results(task_id, target, fail_if_not_exist=True)


class TestPostValidationsV1:
    @pytest.mark.asyncio
    async def test_start_task_successfully_01(
        self,
        mock_submission_api_client: MockApiClient,
        jwt_token: str,
    ):
        """
        There is no task running.

        Expected: Start task successfully
        """
        resource_id = "MTBLS800001"

        client = mock_submission_api_client.client
        headers = {"Authorization": f"Bearer {jwt_token}"}
        params = {
            "run_metadata_modifiers": False,
            "override_previous_task_results": False,
        }

        response = client.post(
            f"/submissions/v2/validations/{resource_id}", headers=headers, params=params
        )
        assert response.status_code == status.HTTP_200_OK
        json_response = response.json()
        result = APIResponse[StartValidationResponse].model_validate(json_response)
        assert result.content.task
        assert result.content.task.task_id

    @pytest.mark.asyncio
    async def test_start_task_successfully_02(
        self,
        mock_submission_api_client: MockApiClient,
        jwt_token: str,
    ):
        """
        There is a task in cache and it started after 10 secs.

        Expected: Delete previous task and start a new task
        """
        resource_id = "MTBLS800001"
        key = f"validation_task:current:{resource_id}"
        services = mock_submission_api_client.container.services
        cache_service: CacheService = services.cache_service()
        await cache_service.set_value(key, value="xyz", expiration_time_in_seconds=580)
        client = mock_submission_api_client.client
        headers = {"Authorization": f"Bearer {jwt_token}"}
        params = {
            "run_metadata_modifiers": False,
            "override_previous_task_results": False,
        }
        response = client.post(
            f"/submissions/v2/validations/{resource_id}", headers=headers, params=params
        )
        assert response.status_code == status.HTTP_200_OK
        json_response = response.json()
        result = APIResponse[StartValidationResponse].model_validate(json_response)
        assert result.content.task
        assert result.content.task.task_id

    @pytest.mark.asyncio
    async def test_start_task_failed_01(
        self,
        mock_submission_api_client: MockApiClient,
        jwt_token: str,
    ):
        """
        There is a task in cache but it is created 5 seconds before.

        Expected: Raise exception AsyncTaskAlreadyStartedError
        """
        resource_id = "MTBLS800001"
        key = f"validation_task:current:{resource_id}"
        task_id = "xyz"
        services = mock_submission_api_client.container.services
        cache_service: CacheService = services.cache_service()
        await cache_service.set_value(
            key, value=task_id, expiration_time_in_seconds=600
        )
        async_task_service: ThreadingAsyncTaskService = services.async_task_service()

        task_result = Mock(AsyncTaskResult)
        task_result.get_id.return_value = task_id
        task_result.get_status.return_value = "RUNNING"
        task_result.is_ready.return_value = False
        task_result.is_successful.return_value = None
        async_task_service.async_task_results_dict[task_id] = task_result
        client = mock_submission_api_client.client
        headers = {"Authorization": f"Bearer {jwt_token}"}
        params = {
            "run_metadata_modifiers": False,
            "override_previous_task_results": False,
        }
        response = client.post(
            f"/submissions/v2/validations/{resource_id}", headers=headers, params=params
        )
        await cache_service.delete_key(key)
        del async_task_service.async_task_results_dict[task_id]
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        json_response = response.json()
        result = APIErrorResponse.model_validate(json_response)
        assert "AsyncTaskAlreadyStartedError" in result.error_message
