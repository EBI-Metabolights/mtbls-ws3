import logging
from typing import Union

from dependency_injector.wiring import Provide, inject

from mtbls.application.decorators.async_task import async_task
from mtbls.application.remote_tasks.common.run_validation import (
    run_validation_task,
    run_validation_task_with_modifiers,
)
from mtbls.application.remote_tasks.common.utils import run_coroutine
from mtbls.application.services.interfaces.async_task.async_task_result import (
    AsyncTaskResult,
)
from mtbls.application.services.interfaces.policy_service import PolicyService
from mtbls.application.services.interfaces.study_metadata_service_factory import (
    StudyMetadataServiceFactory,
)
from mtbls.domain.shared.validator.types import ValidationPhase

logger = logging.getLogger(__name__)


@async_task(queue="common")
@inject
def run_validation(  # noqa: PLR0913
    *,
    resource_id: str,
    apply_modifiers: bool = True,
    phases: Union[ValidationPhase, None, list[str]] = None,
    serialize_result: bool = True,
    study_metadata_service_factory: StudyMetadataServiceFactory = Provide[
        "services.study_metadata_service_factory"
    ],
    policy_service: PolicyService = Provide["services.policy_service"],
    **kwargs,
) -> AsyncTaskResult:
    try:
        modifier_result = None
        if apply_modifiers:
            coroutine = run_validation_task_with_modifiers(
                resource_id,
                study_metadata_service_factory=study_metadata_service_factory,
                policy_service=policy_service,
                phases=phases,
                serialize_result=serialize_result,
            )
        else:
            coroutine = run_validation_task(
                resource_id,
                modifier_result=modifier_result,
                study_metadata_service_factory=study_metadata_service_factory,
                policy_service=policy_service,
                phases=phases,
                serialize_result=serialize_result,
            )
        return run_coroutine(coroutine)

    except Exception as ex:
        logger.error("Validation task execution for %s failed.", resource_id)
        logger.exception(ex)
        raise ex
    finally:
        logger.info("Validation task execution for %s ended.", resource_id)
