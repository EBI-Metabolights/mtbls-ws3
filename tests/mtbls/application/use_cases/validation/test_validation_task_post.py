from unittest.mock import Mock

import pytest
from metabolights_utils.models.metabolights.model import MetabolightsStudyModel

from mtbls.application.services.interfaces.async_task.async_task_executor import (
    AsyncTaskExecutor,
)
from mtbls.application.services.interfaces.async_task.async_task_result import (
    AsyncTaskResult,
)
from mtbls.application.services.interfaces.async_task.async_task_service import (
    AsyncTaskService,
)
from mtbls.application.services.interfaces.cache_service import CacheService
from mtbls.application.use_cases.validation.validation_task import (
    start_study_validation_task,
)
from mtbls.domain.exceptions.async_task import (
    AsyncTaskAlreadyStartedError,
    AsyncTaskResultExistsError,
    AsyncTaskStartFailure,
)
from mtbls.domain.shared.async_task.async_task_summary import AsyncTaskStatus


@pytest.mark.asyncio
async def test_start_study_validation_task_01(
    ms_metabolights_model: MetabolightsStudyModel,
    cache_service: CacheService,
    async_task_service: AsyncTaskService,
):
    """_summary_
    Case:
        start new task successfully
    Expected result:
        return task details
    """

    task_id = "initial-task-id"
    cache_service.get_value.return_value = ""

    result: AsyncTaskResult = Mock(spec=AsyncTaskResult)
    executor: AsyncTaskExecutor = Mock(spec=AsyncTaskExecutor)
    result.get_id.return_value = task_id
    result.is_successful.return_value = False
    result.get_status.return_value = "running"
    result.is_ready.return_value = False
    async_task_service.get_async_task.return_value = executor

    executor.start.return_value = result

    async_task_service.get_async_task.return_value = executor
    async_task_service.get_async_task_result.return_value = result

    resource_id = ms_metabolights_model.investigation.studies[0].identifier
    status: AsyncTaskStatus = await start_study_validation_task(
        resource_id=resource_id,
        cache_service=cache_service,
        apply_modifiers=False,
        async_task_service=async_task_service,
        override_ready_task_results=False,
        cache_expiration_in_seconds=60,
    )
    cache_service.get_value.assert_called_once()
    assert status.task_id == task_id
    assert not status.is_successful
    assert not status.ready
    assert status.task_status == "running"


@pytest.mark.asyncio
async def test_start_study_validation_task_02(
    ms_metabolights_model: MetabolightsStudyModel,
    cache_service: CacheService,
    async_task_service: AsyncTaskService,
):
    """_summary_
    Case:
        There is a task that is still running
    Expected result:
        Task will not start.  AsyncTaskAlreadyStartedError
    """

    task_id = "initial-task-id"
    cache_service.get_value.return_value = task_id
    cache_service.get_ttl_in_seconds.return_value = 595
    result: AsyncTaskResult = Mock(spec=AsyncTaskResult)
    result.get_id.return_value = task_id
    result.is_successful.return_value = False
    result.get_status.return_value = "running"
    result.is_ready.return_value = False

    async_task_service.get_async_task_result.return_value = result

    resource_id = ms_metabolights_model.investigation.studies[0].identifier
    with pytest.raises(AsyncTaskAlreadyStartedError) as x:
        await start_study_validation_task(
            resource_id=resource_id,
            cache_service=cache_service,
            apply_modifiers=False,
            async_task_service=async_task_service,
            override_ready_task_results=False,
            cache_expiration_in_seconds=60,
        )
        assert x.value.args[0] == resource_id
        assert x.value.args[1] == task_id
        async_task_service.get_async_task.assert_not_called()
        async_task_service.get_async_task_result.assert_called()


@pytest.mark.asyncio
async def test_start_study_validation_task_03(
    ms_metabolights_model: MetabolightsStudyModel,
    cache_service: CacheService,
    async_task_service: AsyncTaskService,
):
    """_summary_
    Case:
        There is a task that has been competed succesfully. It is not fetched yet.
        override_ready_task_results is False.  Task will not start.
    Expected result:
        AsyncTaskResultExistsError
    """
    # There is a task that has been competed succesfully.
    # override_ready_task_results is False

    task_id = "initial-task-id"
    cache_service.get_value.return_value = task_id

    result: AsyncTaskResult = Mock(spec=AsyncTaskResult)
    result.get_id.return_value = task_id
    result.is_successful.return_value = True
    result.get_status.return_value = "running"
    result.is_ready.return_value = True

    async_task_service.get_async_task_result.return_value = result

    resource_id = ms_metabolights_model.investigation.studies[0].identifier
    with pytest.raises(AsyncTaskResultExistsError) as x:
        await start_study_validation_task(
            resource_id=resource_id,
            cache_service=cache_service,
            apply_modifiers=False,
            async_task_service=async_task_service,
            override_ready_task_results=False,
            cache_expiration_in_seconds=60,
        )
        assert x.value.args[0] == resource_id
        assert x.value.args[1] == task_id
        async_task_service.get_async_task.assert_not_called()
        async_task_service.get_async_task_result.assert_called()


@pytest.mark.asyncio
async def test_start_study_validation_task_04(
    ms_metabolights_model: MetabolightsStudyModel,
    cache_service: CacheService,
    async_task_service: AsyncTaskService,
):
    """_summary_
    Case:
        There is a task that has been failed.
    Expected result:
        New task will start.
    """

    task_id = "initial-task-id"
    cache_service.get_value.return_value = "task_id"
    cache_service.get_ttl_in_seconds.return_value = 400

    init_result: AsyncTaskResult = Mock(spec=AsyncTaskResult)
    init_result.get_id.return_value = task_id
    init_result.is_successful.return_value = False
    init_result.get_status.return_value = "PENDING"
    init_result.is_ready.return_value = False
    executor: AsyncTaskExecutor = Mock(spec=AsyncTaskExecutor)

    executor.start.return_value = init_result

    async_task_service.get_async_task.return_value = executor

    result_init: AsyncTaskResult = Mock(spec=AsyncTaskResult)
    result_init.get_id.return_value = task_id
    result_init.is_successful.return_value = False
    result_init.get_status.return_value = "success"
    result_init.is_ready.return_value = True

    result: AsyncTaskResult = Mock(spec=AsyncTaskResult)
    result.get_id.return_value = task_id + "2"
    result.is_successful.return_value = False
    result.get_status.return_value = "started"
    result.is_ready.return_value = False

    async_task_service.get_async_task_result.side_effect = [
        result_init,
        result,
    ]

    resource_id = ms_metabolights_model.investigation.studies[0].identifier
    status: AsyncTaskStatus = await start_study_validation_task(
        resource_id=resource_id,
        cache_service=cache_service,
        apply_modifiers=False,
        async_task_service=async_task_service,
        override_ready_task_results=False,
        cache_expiration_in_seconds=600,
    )
    executor.start.assert_called_once()
    cache_service.set_value.assert_called_once()
    async_task_service.get_async_task_result.assert_called()
    assert status.task_id == task_id + "2"
    assert not status.is_successful
    assert not status.ready
    assert status.task_status == "started"


@pytest.mark.asyncio
async def test_start_study_validation_task_05(
    ms_metabolights_model: MetabolightsStudyModel,
    cache_service: CacheService,
    async_task_service: AsyncTaskService,
):
    """_summary_
    Case:
        There is a task that has been completed successfully. It is not fetched.
        But override_ready_task_results is True
    Expected result:
        New task will start.
    """

    task_id = "initial-task-id"
    cache_service.get_value.return_value = "task_id"
    cache_service.get_ttl_in_seconds.return_value = 400
    init_result: AsyncTaskResult = Mock(spec=AsyncTaskResult)
    init_result.get_id.return_value = task_id
    init_result.is_successful.return_value = True
    init_result.get_status.return_value = "PENDING"
    init_result.is_ready.return_value = True
    executor: AsyncTaskExecutor = Mock(spec=AsyncTaskExecutor)

    executor.start.return_value = init_result

    async_task_service.get_async_task.return_value = executor

    result_init: AsyncTaskResult = Mock(spec=AsyncTaskResult)
    result_init.get_id.return_value = task_id
    result_init.is_successful.return_value = True
    result_init.get_status.return_value = "success"
    result_init.is_ready.return_value = True

    result: AsyncTaskResult = Mock(spec=AsyncTaskResult)
    result.get_id.return_value = task_id + "2"
    result.is_successful.return_value = False
    result.get_status.return_value = "started"
    result.is_ready.return_value = False

    async_task_service.get_async_task_result.side_effect = [
        result_init,
        result,
    ]

    resource_id = ms_metabolights_model.investigation.studies[0].identifier
    status: AsyncTaskStatus = await start_study_validation_task(
        resource_id=resource_id,
        cache_service=cache_service,
        apply_modifiers=False,
        async_task_service=async_task_service,
        override_ready_task_results=True,
        cache_expiration_in_seconds=600,
    )
    executor.start.assert_called_once()
    cache_service.set_value.assert_called_once()
    async_task_service.get_async_task_result.assert_called()
    assert status.task_id == task_id + "2"
    assert not status.is_successful
    assert not status.ready
    assert status.task_status == "started"


@pytest.mark.asyncio
async def test_start_study_validation_task_06(
    ms_metabolights_model: MetabolightsStudyModel,
    cache_service: CacheService,
    async_task_service: AsyncTaskService,
):
    """_summary_
    Case:
        There is no task running. New task not started.
    Expected result:
        AsyncTaskStartFailure
    """
    # There is a task that has been competed succesfully.
    # override_ready_task_results is False

    task_id = "initial-task-id"
    cache_service.get_value.return_value = ""
    init_result: AsyncTaskResult = Mock(spec=AsyncTaskResult)
    init_result.get_id.return_value = task_id
    init_result.is_successful.return_value = True
    init_result.get_status.return_value = "PENDING"
    init_result.is_ready.return_value = True
    executor: AsyncTaskExecutor = Mock(spec=AsyncTaskExecutor)

    executor.start.return_value = init_result
    async_task_service.get_async_task.return_value = executor
    async_task_service.get_async_task_result.side_effect = Exception()

    resource_id = ms_metabolights_model.investigation.studies[0].identifier
    with pytest.raises(AsyncTaskStartFailure) as x:
        await start_study_validation_task(
            resource_id=resource_id,
            cache_service=cache_service,
            apply_modifiers=False,
            async_task_service=async_task_service,
            override_ready_task_results=False,
            cache_expiration_in_seconds=60,
        )
        assert x.value.args[0] == resource_id
        assert x.value.args[1] == task_id
        async_task_service.get_async_task.assert_called()
        async_task_service.get_async_task_result.assert_called()
        cache_service.set_value.assert_not_called()


@pytest.mark.asyncio
async def test_start_study_validation_task_07(
    ms_metabolights_model: MetabolightsStudyModel,
    cache_service: CacheService,
    async_task_service: AsyncTaskService,
):
    """_summary_
    Case:
        Task id not found on server.
    Expected result:
        New task will start.
    """

    task_id = "initial-task-id"
    cache_service.get_value.return_value = "task_id"

    init_result: AsyncTaskResult = Mock(spec=AsyncTaskResult)
    init_result.get_id.return_value = task_id
    init_result.is_successful.return_value = True
    init_result.get_status.return_value = "PENDING"
    init_result.is_ready.return_value = True
    executor: AsyncTaskExecutor = Mock(spec=AsyncTaskExecutor)

    executor.start.return_value = init_result

    async_task_service.get_async_task.return_value = executor

    result: AsyncTaskResult = Mock(spec=AsyncTaskResult)
    result.get_id.return_value = task_id + "2"
    result.is_successful.return_value = False
    result.get_status.return_value = "started"
    result.is_ready.return_value = False
    async_task_service.get_async_task_result.side_effect = [
        Exception(),
        result,
    ]

    resource_id = ms_metabolights_model.investigation.studies[0].identifier
    status: AsyncTaskStatus = await start_study_validation_task(
        resource_id=resource_id,
        cache_service=cache_service,
        apply_modifiers=False,
        async_task_service=async_task_service,
        override_ready_task_results=True,
        cache_expiration_in_seconds=60,
    )
    executor.start.assert_called_once()
    cache_service.set_value.assert_called()
    async_task_service.get_async_task_result.assert_called()
    assert status.task_id == task_id + "2"
    assert not status.is_successful
    assert not status.ready
    assert status.task_status == "started"
