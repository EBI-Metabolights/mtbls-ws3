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
from mtbls.application.services.interfaces.validation_override_service import (
    ValidationOverrideService,
)
from mtbls.application.services.interfaces.validation_report_service import (
    ValidationReportService,
)
from mtbls.application.use_cases.validation.validation_task import (
    get_study_validation_result,
)
from mtbls.domain.entities.validation_override import ValidationOverrideList
from mtbls.domain.exceptions.async_task import (
    AsyncTaskCheckStatusFailure,
    AsyncTaskNotFoundError,
    AsyncTaskRemoteFailure,
)
from mtbls.domain.exceptions.repository import StudyObjectNotFoundError
from mtbls.domain.shared.async_task.async_task_summary import AsyncTaskSummary
from mtbls.domain.shared.validator.policy import (
    PolicyResult,
    PolicyResultList,
    PolicySummaryResult,
)


class TestGetStudyValidationResult:
    @pytest.mark.asyncio
    async def test_get_study_validation_result_01(
        self,
        ms_metabolights_model: MetabolightsStudyModel,
        cache_service: CacheService,
        async_task_service: AsyncTaskService,
        validation_report_service: ValidationReportService,
        validation_override_service: ValidationOverrideService,
    ):
        """_summary_
        Case:
            Task id is defined and there is a result on repository.
        Expected result:
            return result from repository
        """

        task_id = "initial-task-id"

        summary_result = PolicySummaryResult()
        validation_report_service.load_validation_report_by_task_id.return_value = (
            summary_result
        )
        resource_id = ms_metabolights_model.investigation.studies[0].identifier
        result: AsyncTaskSummary = await get_study_validation_result(
            resource_id=resource_id,
            task_id=task_id,
            cache_service=cache_service,
            async_task_service=async_task_service,
            validation_report_service=validation_report_service,
            validation_override_service=validation_override_service,
        )
        validation_report_service.load_validation_report_by_task_id.assert_called_once()
        cache_service.get_value.assert_not_called()
        validation_override_service.assert_not_called()
        async_task_service.get_async_task_result.assert_not_called()

        assert result.task_result == summary_result

    @pytest.mark.asyncio
    async def test_get_study_validation_result_02(
        self,
        ms_metabolights_model: MetabolightsStudyModel,
        cache_service: CacheService,
        async_task_service: AsyncTaskService,
        validation_report_service: ValidationReportService,
        validation_override_service: ValidationOverrideService,
    ):
        """_summary_
        Case:
            Task id is not defined. Find last task id and fetch result from repository
        Expected result:
            result from repository
        """

        task_id = "initial-task-id"
        cache_service.get_value.return_value = task_id

        summary_result = PolicySummaryResult()
        validation_report_service.load_validation_report_by_task_id.return_value = (
            summary_result
        )

        resource_id = ms_metabolights_model.investigation.studies[0].identifier
        result: AsyncTaskSummary = await get_study_validation_result(
            resource_id=resource_id,
            task_id=None,
            cache_service=cache_service,
            async_task_service=async_task_service,
            validation_report_service=validation_report_service,
            validation_override_service=validation_override_service,
        )
        validation_report_service.load_validation_report_by_task_id.assert_called_once()
        cache_service.get_value.assert_called()
        validation_override_service.assert_not_called()
        async_task_service.get_async_task_result.assert_not_called()
        assert result.task_result == summary_result

    @pytest.mark.asyncio
    async def test_get_study_validation_result_03(
        self,
        ms_metabolights_model: MetabolightsStudyModel,
        cache_service: CacheService,
        async_task_service: AsyncTaskService,
        validation_report_service: ValidationReportService,
        validation_override_service: ValidationOverrideService,
    ):
        """_summary_
        Case:
            Task id is not defined. There is no running task
        Expected result:
            raise AsyncTaskNotFoundError exception
        """

        cache_service.get_value.return_value = ""

        summary_result = PolicySummaryResult()
        validation_report_service.load_validation_report_by_task_id.return_value = (
            summary_result
        )
        resource_id = ms_metabolights_model.investigation.studies[0].identifier
        with pytest.raises(AsyncTaskNotFoundError):
            await get_study_validation_result(
                resource_id=resource_id,
                task_id=None,
                cache_service=cache_service,
                async_task_service=async_task_service,
                validation_report_service=validation_report_service,
                validation_override_service=validation_override_service,
            )
        cache_service.get_value.assert_called()
        validation_report_service.load_validation_report_by_task_id.assert_not_called()
        validation_override_service.assert_not_called()
        async_task_service.get_async_task_result.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_study_validation_result_04(
        self,
        ms_metabolights_model: MetabolightsStudyModel,
        cache_service: CacheService,
        async_task_service: AsyncTaskService,
        validation_report_service: ValidationReportService,
        validation_override_service: ValidationOverrideService,
    ):
        """_summary_
        Case:
            Task id is not defined. There is a running task. It is not completed.
        Expected result:
            Fetch task status, content is None
        """

        task_id = "initial-task-id"
        cache_service.get_value.return_value = task_id
        resource_id = ms_metabolights_model.investigation.studies[0].identifier

        validation_report_service.load_validation_report_by_task_id.side_effect = (
            StudyObjectNotFoundError(resource_id, "", "")
        )

        result: AsyncTaskResult = Mock(spec=AsyncTaskResult)
        result.get_id.return_value = task_id
        result.is_successful.return_value = False
        result.get_status.return_value = "running"
        result.is_ready.return_value = False
        async_task_service.get_async_task_result.return_value = result

        result: AsyncTaskSummary = await get_study_validation_result(
            resource_id=resource_id,
            task_id=None,
            cache_service=cache_service,
            async_task_service=async_task_service,
            validation_report_service=validation_report_service,
            validation_override_service=validation_override_service,
        )
        cache_service.get_value.assert_called()
        validation_report_service.load_validation_report_by_task_id.assert_called_once()
        validation_override_service.assert_not_called()
        async_task_service.get_async_task_result.assert_called()
        assert result.task.task_id == task_id
        assert result.task_result is None

    @pytest.mark.asyncio
    async def test_get_study_validation_result_05(
        self,
        ms_metabolights_model: MetabolightsStudyModel,
        cache_service: CacheService,
        async_task_service: AsyncTaskService,
        validation_report_service: ValidationReportService,
        validation_override_service: ValidationOverrideService,
    ):
        """_summary_
        Case:
            Task id is not defined. There is a running task. It is completed successfully.
        Expected result:
            Fetch task status, content has validation result
        """

        task_id = "initial-task-id"
        cache_service.get_value.return_value = task_id
        resource_id = ms_metabolights_model.investigation.studies[0].identifier

        validation_report_service.load_validation_report_by_task_id.side_effect = (
            StudyObjectNotFoundError(resource_id, "", "")
        )

        policy_result = PolicyResultList(
            results=[PolicyResult(resource_id=resource_id)]
        )

        result: AsyncTaskResult = Mock(spec=AsyncTaskResult)
        result.get_id.return_value = task_id
        result.is_successful.return_value = True
        result.get_status.return_value = "success"
        result.is_ready.return_value = True
        result.get.return_value = policy_result
        async_task_service.get_async_task_result.return_value = result
        validation_override_service.get_validation_overrides.return_value = (
            ValidationOverrideList(resource_id=resource_id)
        )
        summary_result: AsyncTaskSummary = await get_study_validation_result(
            resource_id=resource_id,
            task_id=None,
            cache_service=cache_service,
            async_task_service=async_task_service,
            validation_report_service=validation_report_service,
            validation_override_service=validation_override_service,
        )
        cache_service.get_value.assert_called()
        validation_report_service.load_validation_report_by_task_id.assert_called_once()
        validation_report_service.save_validation_report.assert_called()
        validation_override_service.get_validation_overrides.assert_called()
        async_task_service.get_async_task_result.assert_called()
        result.get.assert_called()
        assert summary_result.task.task_id == task_id
        assert summary_result.task_result is not None

    @pytest.mark.asyncio
    async def test_get_study_validation_result_06(
        self,
        ms_metabolights_model: MetabolightsStudyModel,
        cache_service: CacheService,
        async_task_service: AsyncTaskService,
        validation_report_service: ValidationReportService,
        validation_override_service: ValidationOverrideService,
    ):
        """_summary_
        Case:
            Task id is not defined. There is a running task. It is completed with failure.
        Expected result:
            return AsyncTaskRemoteFailure
        """

        task_id = "initial-task-id"
        cache_service.get_value.return_value = task_id
        resource_id = ms_metabolights_model.investigation.studies[0].identifier

        validation_report_service.load_validation_report_by_task_id.side_effect = (
            StudyObjectNotFoundError(resource_id, "", "")
        )

        result: AsyncTaskResult = Mock(spec=AsyncTaskResult)
        result.get_id.return_value = task_id
        result.is_successful.return_value = False
        result.get_status.return_value = "success"
        result.is_ready.return_value = True
        result.get.side_effect = Exception("Error")
        async_task_service.get_async_task_result.return_value = result
        validation_override_service.get_validation_overrides.return_value = (
            ValidationOverrideList(resource_id=resource_id)
        )
        with pytest.raises(AsyncTaskRemoteFailure):
            await get_study_validation_result(
                resource_id=resource_id,
                task_id=None,
                cache_service=cache_service,
                async_task_service=async_task_service,
                validation_report_service=validation_report_service,
                validation_override_service=validation_override_service,
            )
        cache_service.get_value.assert_called()
        validation_report_service.load_validation_report_by_task_id.assert_called_once()
        validation_report_service.save_validation_report.assert_not_called()
        validation_override_service.get_validation_overrides.assert_not_called()
        async_task_service.get_async_task_result.assert_called()
        result.get.assert_called()

    @pytest.mark.asyncio
    async def test_get_study_validation_result_07(
        self,
        ms_metabolights_model: MetabolightsStudyModel,
        cache_service: CacheService,
        async_task_service: AsyncTaskService,
        validation_report_service: ValidationReportService,
        validation_override_service: ValidationOverrideService,
    ):
        """_summary_
        Case:
            Task id is not defined. There is a running task. It is completed with failure but not throws exception.
        Expected result:
            return AsyncTaskRemoteFailure
        """

        task_id = "initial-task-id"
        cache_service.get_value.return_value = task_id
        resource_id = ms_metabolights_model.investigation.studies[0].identifier

        validation_report_service.load_validation_report_by_task_id.side_effect = (
            StudyObjectNotFoundError(resource_id, "", "")
        )

        result: AsyncTaskResult = Mock(spec=AsyncTaskResult)
        result.get_id.return_value = task_id
        result.is_successful.return_value = False
        result.get_status.return_value = "success"
        result.is_ready.return_value = True
        result.get.return_value = Exception("Error")
        async_task_service.get_async_task_result.return_value = result
        validation_override_service.get_validation_overrides.return_value = (
            ValidationOverrideList(resource_id=resource_id)
        )
        with pytest.raises(AsyncTaskRemoteFailure):
            await get_study_validation_result(
                resource_id=resource_id,
                task_id=None,
                cache_service=cache_service,
                async_task_service=async_task_service,
                validation_report_service=validation_report_service,
                validation_override_service=validation_override_service,
            )
        cache_service.get_value.assert_called()
        validation_report_service.load_validation_report_by_task_id.assert_called_once()
        validation_report_service.save_validation_report.assert_not_called()
        validation_override_service.get_validation_overrides.assert_not_called()
        async_task_service.get_async_task_result.assert_called()
        result.get.assert_called()

    @pytest.mark.asyncio
    async def test_get_study_validation_result_08(
        self,
        ms_metabolights_model: MetabolightsStudyModel,
        cache_service: CacheService,
        async_task_service: AsyncTaskService,
        validation_report_service: ValidationReportService,
        validation_override_service: ValidationOverrideService,
    ):
        """_summary_
        Case:
            Task id is not defined. Task status is not retrieved from remote.
        Expected result:
            return AsyncTaskRemoteFailure
        """

        task_id = "initial-task-id"
        cache_service.get_value.return_value = task_id
        resource_id = ms_metabolights_model.investigation.studies[0].identifier

        validation_report_service.load_validation_report_by_task_id.side_effect = (
            StudyObjectNotFoundError(resource_id, "", "")
        )

        async_task_service.get_async_task_result.side_effect = Exception()

        with pytest.raises(AsyncTaskCheckStatusFailure):
            await get_study_validation_result(
                resource_id=resource_id,
                task_id=None,
                cache_service=cache_service,
                async_task_service=async_task_service,
                validation_report_service=validation_report_service,
                validation_override_service=validation_override_service,
            )
        cache_service.get_value.assert_called()
        validation_report_service.load_validation_report_by_task_id.assert_called_once()
        validation_report_service.save_validation_report.assert_not_called()
        validation_override_service.get_validation_overrides.assert_not_called()
        async_task_service.get_async_task_result.assert_called()
