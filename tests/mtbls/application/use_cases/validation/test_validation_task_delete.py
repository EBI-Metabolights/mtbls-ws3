from unittest.mock import Mock

import pytest
from metabolights_utils.models.metabolights.model import MetabolightsStudyModel

from mtbls.application.services.interfaces.async_task.async_task_result import (
    AsyncTaskResult,
)
from mtbls.application.services.interfaces.async_task.async_task_service import (
    AsyncTaskService,
)
from mtbls.application.services.interfaces.cache_service import CacheService
from mtbls.application.use_cases.validation.validation_task import (
    delete_validation_task,
)
from mtbls.domain.shared.async_task.async_task_summary import AsyncTaskStatus


class TestDeleteValidationTask:
    @pytest.mark.asyncio
    async def test_delete_validation_task_01(
        self,
        ms_metabolights_model: MetabolightsStudyModel,
        cache_service: CacheService,
        async_task_service: AsyncTaskService,
    ):
        """_summary_
        Case:
            There is no task. Delete task and its result just in case.
        Expected result:
            return False
        """

        task_id = "initial-task-id"
        cache_service.get_value.return_value = ""

        result: AsyncTaskResult = Mock(spec=AsyncTaskResult)
        result.get_id.return_value = task_id
        result.is_successful.return_value = False
        result.get_status.return_value = "running"
        result.is_ready.return_value = False

        async_task_service.get_async_task_result.return_value = result

        resource_id = ms_metabolights_model.investigation.studies[0].identifier
        status: AsyncTaskStatus = await delete_validation_task(
            resource_id=resource_id,
            task_id=task_id,
            cache_service=cache_service,
            async_task_service=async_task_service,
        )
        cache_service.get_value.assert_called_once()
        cache_service.delete_key.assert_called_once()
        result.revoke.assert_called_once()
        assert not status

    @pytest.mark.asyncio
    async def test_delete_validation_task_02(
        self,
        ms_metabolights_model: MetabolightsStudyModel,
        cache_service: CacheService,
        async_task_service: AsyncTaskService,
    ):
        """_summary_
        Case:
            There is a task running. Delete task and its result.
        Expected result:
            return True
        """

        task_id = "initial-task-id"
        cache_service.get_value.return_value = task_id

        result: AsyncTaskResult = Mock(spec=AsyncTaskResult)
        result.get_id.return_value = task_id
        result.is_successful.return_value = False
        result.get_status.return_value = "running"
        result.is_ready.return_value = False

        async_task_service.get_async_task_result.return_value = result

        resource_id = ms_metabolights_model.investigation.studies[0].identifier
        status: AsyncTaskStatus = await delete_validation_task(
            resource_id=resource_id,
            task_id=task_id,
            cache_service=cache_service,
            async_task_service=async_task_service,
        )
        cache_service.get_value.assert_called_once()
        cache_service.delete_key.assert_called_once()
        result.revoke.assert_called_once()
        assert status

    @pytest.mark.asyncio
    async def test_delete_validation_task_03(
        self,
        ms_metabolights_model: MetabolightsStudyModel,
        cache_service: CacheService,
        async_task_service: AsyncTaskService,
    ):
        """_summary_
        Case:
            There is a task running but it is diffent then current task id.
        Expected result:
            Do not delete it and return False
        """

        task_id = "initial-task-id"
        cache_service.get_value.return_value = task_id

        result: AsyncTaskResult = Mock(spec=AsyncTaskResult)
        result.get_id.return_value = task_id + "2"
        result.is_successful.return_value = False
        result.get_status.return_value = "running"
        result.is_ready.return_value = False

        async_task_service.get_async_task_result.return_value = result

        resource_id = ms_metabolights_model.investigation.studies[0].identifier
        status: AsyncTaskStatus = await delete_validation_task(
            resource_id=resource_id,
            task_id=task_id,
            cache_service=cache_service,
            async_task_service=async_task_service,
        )
        cache_service.get_value.assert_called_once()
        cache_service.delete_key.assert_called_once()
        result.revoke.assert_not_called()
        assert not status

    @pytest.mark.asyncio
    async def test_delete_validation_task_04(
        self,
        ms_metabolights_model: MetabolightsStudyModel,
        cache_service: CacheService,
        async_task_service: AsyncTaskService,
    ):
        """_summary_
        Case:
            There is a task running but revoke failed.
        Expected result:
            add warning messaage and return True
        """

        task_id = "initial-task-id"
        cache_service.get_value.return_value = task_id

        result: AsyncTaskResult = Mock(spec=AsyncTaskResult)
        result.get_id.return_value = task_id
        result.is_successful.return_value = False
        result.get_status.return_value = "running"
        result.is_ready.return_value = False
        result.revoke.side_effect = Exception("test exception")

        async_task_service.get_async_task_result.return_value = result

        resource_id = ms_metabolights_model.investigation.studies[0].identifier
        status: AsyncTaskStatus = await delete_validation_task(
            resource_id=resource_id,
            task_id=task_id,
            cache_service=cache_service,
            async_task_service=async_task_service,
        )
        cache_service.get_value.assert_called_once()
        cache_service.delete_key.assert_called_once()
        result.revoke.assert_called_once()

        assert status

    @pytest.mark.asyncio
    async def test_delete_validation_task_05(
        self,
        ms_metabolights_model: MetabolightsStudyModel,
        cache_service: CacheService,
        async_task_service: AsyncTaskService,
    ):
        """_summary_
        Case:
            Failed to fetch task.
        Expected result:
            add warning messaage and return True
        """

        task_id = "initial-task-id"
        cache_service.get_value.return_value = task_id

        async_task_service.get_async_task_result.side_effect = Exception("error")

        resource_id = ms_metabolights_model.investigation.studies[0].identifier
        status: AsyncTaskStatus = await delete_validation_task(
            resource_id=resource_id,
            task_id=task_id,
            cache_service=cache_service,
            async_task_service=async_task_service,
        )
        cache_service.get_value.assert_called_once()
        cache_service.delete_key.assert_called_once()

        assert status
