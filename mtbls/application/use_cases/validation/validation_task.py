import datetime
import io
import logging
from typing import Union

from mtbls.application.remote_tasks.common.run_validation import run_validation
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
from mtbls.application.use_cases.validation.validation_reports import (
    load_validation_report_by_task_id,
    override_and_save_validation_report,
)
from mtbls.domain.exceptions.async_task import (
    AsyncTaskAlreadyStartedError,
    AsyncTaskCheckStatusFailure,
    AsyncTaskNotFoundError,
    AsyncTaskNotReadyError,
    AsyncTaskRemoteFailure,
    AsyncTaskResultExistsError,
    AsyncTaskStartFailure,
)
from mtbls.domain.exceptions.repository import StudyObjectNotFoundError
from mtbls.domain.shared.async_task.async_task_summary import (
    AsyncTaskStatus,
    AsyncTaskSummary,
)
from mtbls.domain.shared.validator.policy import (
    PolicyMessage,
    PolicyResult,
    PolicyResultList,
    PolicySummaryResult,
)
from mtbls.domain.shared.validator.types import PolicyMessageType

logger = logging.getLogger(__name__)


async def start_study_validation_task(  # noqa: PLR0913
    resource_id: str,
    async_task_service: AsyncTaskService,
    cache_service: CacheService,
    apply_modifiers: bool = False,
    override_ready_task_results: bool = False,
    cache_expiration_in_seconds: int = 10 * 60,
) -> AsyncTaskStatus:
    """Start a validation task for the study. Only one study validation task is allowed at the same time.

    Args:
        resource_id (str): a study accession number or submission id
        async_task_service (AsyncTaskService): a service to run a remote task
        cache_service (CacheService): cache service to track current running tasks for studies.
        apply_modifiers (bool, optional): runs modifiers and update metadata files before study validation. Defaults to False.
        override_ready_task_results (bool, optional): deletes the previous task result and starts new one. Defaults to False.
        cache_expiration_in_seconds (int, optional): duration to store the last task id in the cache. Defaults to 10*60 sec.

    Raises:
        AsyncTaskAlreadyStartedError: Raise if there is a task running.
        AsyncTaskResultExistsError: Raise when there is a completed task and its result is not fetched yet.
            It is not thrown if override_ready_task_results is True
        AsyncTaskStartFailure: Raise if start task is failed.

    Returns:
        AsyncTaskStatus: Status of the started task.
    """
    key = f"validation_task:current:{resource_id}"
    task_id = await cache_service.get_value(key)
    if task_id:
        task = None
        try:
            task = await async_task_service.get_async_task_result(task_id)
        except Exception as exc:
            message = (
                f"Task {task_id} is not fetched from executor. Starting new task..."
            )
            logger.warning(message)
            logger.exception(exc)
            await cache_service.delete_key(key)
        if task:
            if not override_ready_task_results and task.is_successful():
                raise AsyncTaskResultExistsError(
                    resource_id,
                    task_id,
                    f"Previous validation task: {task_id}",
                    "Validation result of the previous validation task exists. "
                    "Read the previous task result or delete it to start a new validation task.",
                )
            ttl = await cache_service.get_ttl_in_seconds(key)
            if ttl > (cache_expiration_in_seconds - 10):
                raise AsyncTaskAlreadyStartedError(
                    resource_id,
                    task_id,
                    f"A validation task has been started for {resource_id}. Wait for its result.",
                )
            logger.debug(
                "Validation task for %s is overriding the previous task result %s",
                resource_id,
                task_id,
            )
            await cache_service.delete_key(key)
            task.revoke(terminate=True)

    executor = await async_task_service.get_async_task(
        run_validation,
        resource_id=resource_id,
        apply_modifiers=apply_modifiers,
    )
    result = await executor.start()
    task_id = result.get_id()
    logger.info(
        "Validation task started for %s with task id %s",
        resource_id,
        task_id,
    )

    try:
        updated_task_result = await async_task_service.get_async_task_result(task_id)
    except Exception as ex:
        message = f"Current validation task failed to start: {task_id}"
        logger.error(message)
        await cache_service.delete_key(key)
        raise AsyncTaskStartFailure(resource_id, task_id, message) from ex

    await cache_service.set_value(
        key=key,
        value=updated_task_result.get_id(),
        expiration_time_in_seconds=cache_expiration_in_seconds,
    )
    logger.debug(
        "Cache current validation task '%s' for %s secs.",
        updated_task_result.get_id(),
        cache_expiration_in_seconds,
    )
    if updated_task_result.get_status().upper().startswith("FAIL"):
        logger.error(
            "Current task id:'%s' and its status: %s.",
            updated_task_result.get_id(),
            updated_task_result.get_status(),
        )
    else:
        logger.debug(
            "Current task id:'%s' and its status: %s.",
            updated_task_result.get_id(),
            updated_task_result.get_status(),
        )
    return AsyncTaskStatus(
        task_id=updated_task_result.get_id(),
        task_status=updated_task_result.get_status(),
        is_successful=updated_task_result.is_successful(),
        ready=updated_task_result.is_ready(),
    )


async def get_study_validation_result(  # noqa: PLR0913
    resource_id: str,
    async_task_service: AsyncTaskService,
    cache_service: CacheService,
    validation_report_service: ValidationReportService,
    validation_override_service: ValidationOverrideService,
    task_id: Union[None, str] = None,
) -> AsyncTaskSummary:
    """Get the status and validation result (if it is ready) of the validation task.
    If task_id is not defined, It returns the latest task's status and its status/

    Args:
        resource_id (str): a study accession number or submission id
        task_id (Union[None, str]): task_id of the validation result.
        async_task_service (AsyncTaskService): a service to fetch result of the remote task.
        cache_service (CacheService): cache service that is tracking current running tasks.
        validation_report_service (ValidationReportService): service to fetch or save validation results.
        validation_override_service (ValidationOverrideService): service to fetch validation overrides for the study.
        task_id (Union[None, str], optional): _description_. Defaults to None.

    Raises:
        AsyncTaskNotFoundError: raises if task_id is not defined and there is no a running validation task for the study.
        AsyncTaskCheckStatusFailure: raises async task service raises an error while checking task status.
        AsyncTaskRemoteFailure: raises remote task has ended with failure.

    Returns:
        AsyncTaskSummary: Task status and validation result (if task is completed).
    """

    task_status: Union[AsyncTaskStatus, None] = None
    task: Union[AsyncTaskResult, None] = None
    task_result: Union[PolicySummaryResult, None] = None
    key = f"validation_task:current:{resource_id}"
    cached_checked = False
    if not task_id:
        task_id = await cache_service.get_value(key)
        cached_checked = True
    task_status, task_result = await get_result_from_repository(
        validation_report_service, resource_id, task_id
    )

    if not task_result:
        if not cached_checked:
            cached_task_id = await cache_service.get_value(key)
            if cached_task_id != task_id:
                raise AsyncTaskNotFoundError(
                    resource_id,
                    f"No validation task found for resource_id: {resource_id}",
                )

        try:
            task: AsyncTaskResult = await async_task_service.get_async_task_result(
                task_id=task_id
            )
            if not task:
                raise AsyncTaskNotFoundError(
                    resource_id, task_id, f"Task result is not valid for task {task_id}"
                )
        except Exception as ex:
            logger.debug(
                "Task id %s is not found on study validation history for study %s",
                task_id,
                resource_id,
            )
            raise AsyncTaskCheckStatusFailure(
                resource_id, f"Validation task fetch error: {str(ex)}"
            ) from ex

    if not task_result:
        task_status = AsyncTaskStatus(
            task_id=task.get_id(),
            task_status=task.get_status(),
            ready=task.is_ready(),
            is_successful=task.is_successful() if task.is_ready() else None,
        )
        if task.is_ready():
            try:
                if task.is_successful():
                    report_content = task.get()
                    result_list = PolicyResultList.model_validate(report_content)

                    task_result = await convert_to_summary_result(
                        resource_id, result_list
                    )
                    task_result.task_id = task_id
                    task_result = await override_and_save_validation_report(
                        resource_id=resource_id,
                        task_id=task_id,
                        validation_result=task_result,
                        validation_report_service=validation_report_service,
                        validation_override_service=validation_override_service,
                    )
                else:
                    try:
                        task.get()

                    except Exception as ex:
                        raise AsyncTaskRemoteFailure(
                            resource_id, task_id, f"Remote task failed: {str(ex)}"
                        ) from ex
                    raise AsyncTaskRemoteFailure(
                        resource_id, task_id, "Remote task failed."
                    )
            finally:
                await cache_service.delete_key(key)
                task.revoke()

    return AsyncTaskSummary[PolicySummaryResult](
        task=task_status,
        task_result=task_result,
    )


async def get_study_validation_report(  # noqa: PLR0913
    resource_id: str,
    async_task_service: AsyncTaskService,
    cache_service: CacheService,
    validation_report_service: ValidationReportService,
    validation_override_service: ValidationOverrideService,
    task_id: Union[None, str] = None,
    min_violation_level: Union[None, PolicyMessageType] = None,
    include_summary_messages: bool = True,
    include_isa_metadata_updates: bool = True,
    include_overrides: bool = True,
    delimiter: str = "\t",
) -> tuple[AsyncTaskSummary, str]:
    task_summary: AsyncTaskSummary[
        PolicySummaryResult
    ] = await get_study_validation_result(
        resource_id=resource_id,
        async_task_service=async_task_service,
        cache_service=cache_service,
        validation_report_service=validation_report_service,
        validation_override_service=validation_override_service,
        task_id=task_id,
    )

    if task_summary.task.ready and task_summary.task.is_successful:
        summary_result: PolicySummaryResult = task_summary.task_result
        summary_report = await get_report_content_from_summary_report(
            summary_result=summary_result,
            min_violation_level=min_violation_level,
            include_summary_messages=include_summary_messages,
            include_isa_metadata_updates=include_isa_metadata_updates,
            include_overrides=include_overrides,
            delimiter=delimiter,
        )
        return task_summary, summary_report
    raise AsyncTaskNotReadyError(
        resource_id, "Task is not ready", task_summary.task.task_status
    )


async def get_report_content_from_summary_report(
    summary_result: PolicySummaryResult,
    min_violation_level: Union[None, PolicyMessageType] = None,
    include_summary_messages: bool = True,
    include_isa_metadata_updates: bool = True,
    include_overrides: bool = True,
    delimiter: str = "\t",
) -> str:
    f = io.StringIO()
    row = [
        "MESSAGE",
        "SOURCE FILE",
        "ASSAY TECHNIQUE",
        "SECTION",
        "COLUMN NAME",
        "COLUMN INDEX",
        "RULE ID",
        "TYPE",
        "PRIORITY",
        "RULE TITLE",
        "DESCRIPTION",
        "VIOLATION MESSAGE",
        "MORE VIOLATIONS",
        "TOTAL VIOLATIONS",
        "OVERRIDDEN",
        "OVERRIDE_COMMENT",
    ]
    row_str = delimiter.join(row)
    f.write(f"{row_str}\n")
    if summary_result.messages.violations:
        for messages in (
            summary_result.messages.summary,
            summary_result.messages.violations,
        ):
            # Define technique names for messages
            for message in messages:
                for lists in (
                    summary_result.assay_file_techniques,
                    summary_result.maf_file_techniques,
                ):
                    if message.source_file in lists:
                        message.technique = lists[message.source_file]
                        break

    min_level = min_violation_level.get_level() if min_violation_level else 0

    if include_summary_messages:
        for item in summary_result.messages.summary:
            m: PolicyMessage = item
            if not min_violation_level or m.type.get_level() >= min_level:
                row_str = get_row_string("SUMMARY", item, delimiter=delimiter)
                f.write(f"{row_str}\n")

    for item in summary_result.messages.violations:
        m: PolicyMessage = item
        if not min_violation_level or m.type.get_level() >= min_level:
            row_str = get_row_string("VIOLATION", item, delimiter=delimiter)
            f.write(f"{row_str}\n")

    if include_overrides and summary_result.overrides.validation_overrides:
        f.write("\n\n")
        f.write("OVERRIDDEN VALIDATION RULES\n")
        row = [
            "RULE ID",
            "TITLE",
            "DESCRIPTION",
            "ENABLED",
            "SOURCE FILE",
            "COLUMN NAME",
            "OLD TYPE",
            "NEW TYPE",
            "COMMENT",
        ]
        row_str = delimiter.join(row)
        f.write(f"{row_str}\n")
        summary_result.overrides.validation_overrides.sort(
            key=lambda x: f"{x.rule_id}:{x.source_file}:{str(x.source_column_index)}"
        )
        for item in summary_result.overrides.validation_overrides:
            row = [
                item.rule_id,
                item.title,
                item.description,
                str(item.enabled),
                item.source_file,
                item.source_column_header,
                item.old_type,
                item.new_type,
                item.comment,
            ]
            row_str = delimiter.join(row)
            f.write(f"{row_str}\n")

    if include_isa_metadata_updates and summary_result.metadata_updates:
        if summary_result.metadata_updates:
            f.write("\n\n")
            f.write("METADATA UPDATES\n")
            row = ["SOURCE FILE", "OLD VALUE(s)", "NEW VALUE(s)", "ACTION"]
            row_str = delimiter.join(row)
            f.write(f"{row_str}\n")
            for item in summary_result.metadata_updates:
                row = [item.source, item.old_value, item.new_value, item.action]
                row_str = delimiter.join(row)
                f.write(f"{row_str}\n")
    value = f.getvalue()
    return value


def get_row_string(message_type: str, m: PolicyMessage, delimiter: str = "\t"):
    row = [
        message_type,
        m.source_file,
        m.technique,
        m.section,
        m.source_column_header,
        str(m.source_column_index),
        m.identifier,
        m.type.value,
        m.priority,
        m.title,
        m.description,
        m.violation,
        str(m.has_more_violations),
        str(m.total_violations),
        str(m.overridden),
        m.override_comment,
    ]

    return delimiter.join(row)


async def get_result_from_repository(
    validation_report_service: ValidationReportService,
    resource_id: str,
    task_id: str,
):
    if not task_id:
        raise AsyncTaskNotFoundError(
            resource_id, f"No validation task found for resource_id: {resource_id}"
        )
    task_status: Union[AsyncTaskStatus, None] = None
    task_result: Union[PolicySummaryResult, None] = None
    try:
        # search on history
        task_result = await load_validation_report_by_task_id(
            resource_id=resource_id,
            task_id=task_id,
            validation_report_service=validation_report_service,
        )
        task_status = AsyncTaskStatus(
            task_id=task_id,
            task_status="SUCCESS",
            ready=True,
            is_successful=True,
            message="Read from history",
        )
    except StudyObjectNotFoundError:
        logger.debug(
            "Task id %s is not found on study validation history for study %s. "
            "Checking running tasks...",
            task_id,
            resource_id,
        )

    return task_status, task_result


async def delete_validation_task(
    resource_id: str,
    task_id: Union[None, str],
    async_task_service: AsyncTaskService,
    cache_service: CacheService,
) -> bool:
    """Delete task id from cache and signal remote workers to terminate if.
        If task_id is not defined, the latest study validation task of the study will be deleted.

    Args:
        resource_id (str): a study accession number or submission id
        task_id (Union[None, str]): task_id that will be deleted.
        async_task_service (AsyncTaskService): a service to delete a remote task
        cache_service (CacheService): cache service that is tracking current running tasks.

    Raises:
        AsyncTaskNotFoundError: raises if there is not task with task_id

    Returns:
        bool: Return if task id is deleted from cache and sent signal to terminate current running task.
    """
    result: Union[AsyncTaskResult, None] = None
    key = f"validation_task:current:{resource_id}"
    value = await cache_service.get_value(key)

    task_id_in_cache = value == task_id
    await cache_service.delete_key(key)
    try:
        result: AsyncTaskResult = await async_task_service.get_async_task_result(
            task_id=task_id
        )
        if not result or result.get_id() != task_id:
            raise AsyncTaskNotFoundError(resource_id, task_id)
    except Exception as ex:
        if isinstance(ex, AsyncTaskNotFoundError):
            return False
        message = f"Validation task id {task_id} is not found for resource_id: {resource_id}: {ex}"
        logger.error(message)

    try:
        if result:
            result.revoke(terminate=True)
        else:
            logger.warning(
                "Remote task is not fetched. If task %s is still running, "
                "it may cause resource consumption.",
                task_id,
            )
    except Exception as ex:
        message = f"Revoke failed: validation task id {task_id} for resource_id: {resource_id}: {ex}"
        logger.error(message)

    return task_id_in_cache


async def convert_to_summary_result(resource_id: str, result_list: PolicyResultList):
    results: list[PolicyResult] = result_list.results
    summary_result = PolicySummaryResult()

    if results and results[0] and results[0].metadata_modifier_enabled:
        summary_result.metadata_modifier_enabled = results[0].metadata_modifier_enabled
        summary_result.metadata_updates = results[0].metadata_updates
        summary_result.metadata_updates.sort(key=lambda x: x.source + x.action)
    now = datetime.datetime.now(datetime.timezone.utc).timestamp()
    start_time = now - 1
    completion_time = now
    for result in results:
        policy_result = PolicyResult.model_validate(result, from_attributes=True)
        if policy_result.start_time:
            try:
                result_start_time = start_time
                if isinstance(policy_result.start_time, str):
                    result_start_time = datetime.datetime.fromisoformat(
                        policy_result.start_time
                    ).timestamp()
                elif isinstance(policy_result.start_time, datetime.datetime):
                    result_start_time = policy_result.start_time.timestamp()
                start_time = result_start_time
            except (TypeError, ValueError):
                ...
        if policy_result.completion_time:
            try:
                result_completion_time = completion_time
                if isinstance(policy_result.completion_time, str):
                    result_completion_time = datetime.datetime.fromisoformat(
                        policy_result.completion_time
                    ).timestamp()
                elif isinstance(policy_result.completion_time, datetime.datetime):
                    result_completion_time = policy_result.completion_time.timestamp()
                completion_time = result_completion_time
            except (TypeError, ValueError):
                ...
        summary_result.messages.violations.extend(policy_result.messages.violations)
        summary_result.messages.summary.extend(policy_result.messages.summary)

    summary_result.messages.summary.sort(
        key=lambda x: x.source_file + x.identifier + x.source_column_header
    )
    summary_result.start_time = datetime.datetime.fromtimestamp(start_time)
    summary_result.completion_time = datetime.datetime.fromtimestamp(completion_time)
    summary_result.duration_in_seconds = completion_time - start_time
    summary_result.resource_id = resource_id
    summary_result.assay_file_techniques.update(policy_result.assay_file_techniques)
    summary_result.maf_file_techniques.update(policy_result.maf_file_techniques)
    return summary_result
