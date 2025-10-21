import datetime
from logging import getLogger
from typing import Annotated, Union

from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends, Query, status
from fastapi.params import Param
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import Field

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
from mtbls.application.use_cases.validation.validation_reports import (
    get_validation_reports,
)
from mtbls.application.use_cases.validation.validation_task import (
    delete_validation_task,
    get_study_validation_report,
    get_study_validation_result,
    start_study_validation_task,
)
from mtbls.domain.exceptions.async_task import (
    AsyncTaskCheckStatusFailure,
    AsyncTaskNotFoundError,
    AsyncTaskNotReadyError,
    AsyncTaskRemoteFailure,
)
from mtbls.domain.shared.async_task.async_task_summary import AsyncTaskStatus
from mtbls.domain.shared.data_types import ZeroOrPositiveInt
from mtbls.domain.shared.permission import StudyPermissionContext
from mtbls.domain.shared.repository.paginated_output import PaginatedOutput
from mtbls.domain.shared.validation_result_file import ValidationResultFile
from mtbls.domain.shared.validator.types import PolicyMessageType
from mtbls.presentation.rest_api.core.responses import (
    APIErrorResponse,
    APIResponse,
    DeleteTaskResponse,
    Status,
)
from mtbls.presentation.rest_api.groups.auth.v1.routers.dependencies import (
    check_read_permission,
    check_update_permission,
)
from mtbls.presentation.rest_api.groups.submission.v1.routers.validation_tasks.models import (  # noqa: E501
    GetValidationResponse,
    StartValidationResponse,
)
from mtbls.presentation.rest_api.shared.dependencies import get_resource_id, get_task_id

logger = getLogger(__name__)

router = APIRouter(
    tags=["MetaboLights Submission"], prefix="/submissions/v2/validations"
)


@router.get(
    "/{resource_id}",
    summary="Get Study Validation Results History "
    "ordered by validation time descending",
    description="Get Validation Results",
    response_model=APIResponse[PaginatedOutput[ValidationResultFile]],
)
@inject
async def get_validation_history(
    context: Annotated[StudyPermissionContext, Depends(check_read_permission)],
    offset: Annotated[
        Union[None, ZeroOrPositiveInt],
        Query(description="initial item index. 0 is the latest ones"),
    ] = None,
    limit: Annotated[
        Union[None, ZeroOrPositiveInt],
        Query(description="maximum number of items"),
    ] = None,
    validation_report_service: ValidationReportService = Depends(  # noqa: FAST002
        Provide["services.validation_report_service"]
    ),
):
    resource_id = context.study.accession_number

    validation_history = await get_validation_reports(
        resource_id=resource_id,
        offset=offset,
        limit=limit,
        validation_report_service=validation_report_service,
    )
    paginated_data = PaginatedOutput[ValidationResultFile](
        offset=offset if offset else 0,
        size=len(validation_history),
        data=validation_history,
    )
    response = APIResponse[PaginatedOutput[ValidationResultFile]](
        content=paginated_data
    )
    if not validation_history:
        response.success_message = "No validation result in history."
    else:
        response.success_message = (
            f"{len(validation_history)} validation result(s) in history."
        )
    return response


@router.post(
    "/{resource_id}",
    summary="Create Study Validation Task",
    description="Create a task to execute MetaboLights validations",
    response_model=APIResponse[StartValidationResponse],
)
@inject
async def create_validation_task(
    context: Annotated[StudyPermissionContext, Depends(check_update_permission)],
    run_metadata_modifiers: Annotated[
        bool,
        Field(
            title="Run metadata modifier",
            description="Updates metadata files and "
            "fixes common errors in metadata files.",
        ),
    ] = True,
    override_previous_task_results: Annotated[
        bool,
        Field(
            title="Force to delete previous result.",
            description="Deletes previous task results even if it is not read.",
        ),
    ] = True,
    async_task_service: AsyncTaskService = Depends(  # noqa: FAST002
        Provide["services.async_task_service"]
    ),
    cache_service: CacheService = Depends(Provide["services.cache_service"]),  # noqa: FAST002
):
    resource_id = context.study.accession_number
    if run_metadata_modifiers:
        logger.info("Run validation with modifiers...")
    else:
        logger.info("Run validation without modifiers...")

    task_status = await start_study_validation_task(
        resource_id=resource_id,
        apply_modifiers=run_metadata_modifiers,
        override_ready_task_results=override_previous_task_results,
        async_task_service=async_task_service,
        cache_service=cache_service,
        cache_expiration_in_seconds=10 * 60,
    )

    response = APIResponse[StartValidationResponse]()
    response.success_message = (
        f"Validation task {task_status.task_id} is started for {resource_id}."
    )
    logger.info(response.success_message)
    response.content = StartValidationResponse(
        task=AsyncTaskStatus(
            task_id=task_status.task_id,
            task_status="INITIATED",
            message=f"New validation task is started for {resource_id}",
        )
    )
    return response


@router.get(
    "/{resource_id}/{task_id}",
    summary="Check and Get Study Validation Result",
    description="Checks Validation Task and "
    "return MetaboLights validations (if task completed). "
    "If task_id is not defined, returns the last task result (if exists)",
    response_model=APIResponse[GetValidationResponse],
)
@inject
async def get_validation_task_result(  # noqa: PLR0913
    resource_id: Annotated[str, Depends(get_resource_id)],
    task_id: Annotated[Union[None, str], Depends(get_task_id)],
    context: Annotated[StudyPermissionContext, Depends(check_read_permission)],
    async_task_service: AsyncTaskService = Depends(  # noqa: FAST002
        Provide["services.async_task_service"]
    ),
    cache_service: CacheService = Depends(Provide["services.cache_service"]),  # noqa: FAST002
    validation_report_service: ValidationReportService = Depends(  # noqa: FAST002
        Provide["services.validation_report_service"]
    ),
    validation_override_service: ValidationOverrideService = Depends(  # noqa: FAST002
        Provide["services.validation_override_service"]
    ),
):
    resource_id = context.study.accession_number
    try:
        result = await get_study_validation_result(
            resource_id=resource_id,
            task_id=task_id,
            async_task_service=async_task_service,
            cache_service=cache_service,
            validation_report_service=validation_report_service,
            validation_override_service=validation_override_service,
        )
    except AsyncTaskNotFoundError as ex:
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content=APIErrorResponse(
                error_message=f"{type(ex).__name__}: {str(ex)}"
            ).model_dump(),
        )
    except (
        AsyncTaskCheckStatusFailure,
        AsyncTaskRemoteFailure,
    ) as ex:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content=APIErrorResponse(
                error_message=f"{type(ex).__name__}: {str(ex)}"
            ).model_dump(),
        )
    response = APIResponse[GetValidationResponse]()
    response.content = GetValidationResponse.model_validate(
        result, from_attributes=True
    )
    task_id = result.task.task_id
    if result.task.ready:
        if not result.task.is_successful:
            response.status = Status.ERROR
            response.error_message = f"{resource_id} validation task {task_id} failed."
        else:
            response.success_message = (
                f"Result of {resource_id} validation task: {task_id}."
            )
    else:
        response.success_message = (
            f"Status of validation task {task_id} for {resource_id}."
        )
    return response


@router.get(
    "/{resource_id}/{task_id}/report",
    summary="Get Study Validation Report",
    description="Checks Validation Task and return MetaboLights validation report (if task completed). "  # noqa: E501
    "If task_id is not defined, returns the last task result (if exists)",
)
@inject
async def get_validation_task_report(  # noqa: PLR0913
    resource_id: Annotated[str, Depends(get_resource_id)],
    task_id: Annotated[Union[None, str], Depends(get_task_id)],
    context: Annotated[StudyPermissionContext, Depends(check_read_permission)],
    min_violation_level: Annotated[
        Union[PolicyMessageType],
        Param(
            description="Minimum violation message level. WARNING, ERROR, INFO, SUCCESS"
        ),
    ] = PolicyMessageType.SUCCESS,
    include_summary_messages: Annotated[
        bool, Param(description="Include summary messages")
    ] = True,
    include_isa_metadata_updates: Annotated[
        bool, Param(description="Include metadata file updates")
    ] = True,
    include_overrides: Annotated[
        bool, Param(description="Include validation rule overrides")
    ] = True,
    async_task_service: AsyncTaskService = Depends(  # noqa: FAST002
        Provide["services.async_task_service"]
    ),
    cache_service: CacheService = Depends(Provide["services.cache_service"]),  # noqa: FAST002
    validation_report_service: ValidationReportService = Depends(  # noqa: FAST002
        Provide["services.validation_report_service"]
    ),
    validation_override_service: ValidationOverrideService = Depends(  # noqa: FAST002
        Provide["services.validation_override_service"]
    ),
):
    resource_id = context.study.accession_number
    media_type = "text/tab-separated-values"
    try:
        summary, result = await get_study_validation_report(
            resource_id=resource_id,
            task_id=task_id,
            async_task_service=async_task_service,
            cache_service=cache_service,
            validation_report_service=validation_report_service,
            validation_override_service=validation_override_service,
            min_violation_level=min_violation_level,
            include_summary_messages=include_summary_messages,
            include_isa_metadata_updates=include_isa_metadata_updates,
            include_overrides=include_overrides,
            delimiter="\t",
        )
        task_id = summary.task.task_id
        now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
        download_filename = (
            f'attachment; filename="{resource_id}_validation_{now}_{task_id}.tsv"'
        )
        headers = {
            "x-mtbls-file-type": media_type,
            "Content-Disposition": download_filename,
        }
        report_chunk_size_in_bytes = 1024 * 1024 * 1

        def iter_content(data: str):
            for i in range(0, len(data), report_chunk_size_in_bytes):
                yield data[i : (i + report_chunk_size_in_bytes)]

        response = StreamingResponse(content=iter_content(result), headers=headers)
        return response
    except (AsyncTaskNotFoundError, AsyncTaskNotReadyError) as ex:
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content=APIErrorResponse(
                error_message=f"{type(ex).__name__}: {str(ex)}"
            ).model_dump(),
        )
    except (
        AsyncTaskCheckStatusFailure,
        AsyncTaskRemoteFailure,
    ) as ex:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content=APIErrorResponse(
                error_message=f"{type(ex).__name__}: {str(ex)}"
            ).model_dump(),
        )


@router.delete(
    "/{resource_id}/tasks/{task_id}",
    summary="Delete running validation task.",
    description="If there is a task running, terminate the task",
    response_model=APIResponse[DeleteTaskResponse],
)
@inject
async def delete_validation_task_result(
    task_id: Annotated[Union[None, str], Depends(get_task_id)],
    resource_id: Annotated[str, Depends(get_resource_id)],
    context: Annotated[StudyPermissionContext, Depends(check_update_permission)],
    async_task_service: AsyncTaskService = Depends(  # noqa: FAST002
        Provide["services.async_task_service"]
    ),
    cache_service: CacheService = Depends(Provide["services.cache_service"]),  # noqa: FAST002
):
    resource_id = context.study.accession_number
    deleted = await delete_validation_task(
        resource_id=resource_id,
        task_id=task_id,
        async_task_service=async_task_service,
        cache_service=cache_service,
    )
    if deleted:
        message = "Task is terminated and task results are deleted"
    else:
        message = "Task is not found."
    return APIResponse[DeleteTaskResponse](
        content=DeleteTaskResponse(
            deleted=deleted,
            task_id=task_id,
            message=message,
        )
    )
