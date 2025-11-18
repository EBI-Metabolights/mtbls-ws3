import datetime
import logging
import time
from typing import Any, Dict, Union

from dependency_injector.wiring import Provide, inject
from metabolights_utils.models.metabolights.model import MetabolightsStudyModel

from mtbls.application.decorators.async_task import async_task
from mtbls.application.remote_tasks.common.run_modifier import (
    run_isa_metadata_modifier_task,
)
from mtbls.application.remote_tasks.common.utils import run_coroutine
from mtbls.application.services.interfaces.async_task.async_task_result import (
    AsyncTaskResult,
)
from mtbls.application.services.interfaces.policy_service import PolicyService
from mtbls.application.services.interfaces.study_metadata_service import (
    StudyMetadataService,
)
from mtbls.application.services.interfaces.study_metadata_service_factory import (
    StudyMetadataServiceFactory,
)
from mtbls.domain.shared.modifier import StudyMetadataModifierResult, UpdateLog
from mtbls.domain.shared.validator.policy import PolicyResult, PolicyResultList
from mtbls.domain.shared.validator.types import PolicyMessageType, ValidationPhase

logger = logging.getLogger(__name__)


all_validation_phases = [
    ValidationPhase.PHASE_1,
    ValidationPhase.PHASE_2,
    ValidationPhase.PHASE_3,
    ValidationPhase.PHASE_4,
]

validation_phase_names = {str(x.value) for x in all_validation_phases}


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
            logger.info("Run validation with modifiers")
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


async def run_validation_task_with_modifiers(
    resource_id: str,
    study_metadata_service_factory: StudyMetadataServiceFactory,
    policy_service: PolicyService,
    phases: Union[ValidationPhase, None, list[str]] = None,
    serialize_result: bool = True,
) -> Union[Dict[str, Any], PolicyResultList]:
    try:
        modifier_result = await run_isa_metadata_modifier_task(
            resource_id,
            study_metadata_service_factory=study_metadata_service_factory,
            policy_service=policy_service,
            serialize_result=False,
        )
    except Exception as ex:
        logger.error("Error to modify %s: %s", resource_id, ex)
        logger.exception(ex)
        modifier_result = None

    return await run_validation_task(
        resource_id,
        modifier_result=modifier_result,
        study_metadata_service_factory=study_metadata_service_factory,
        policy_service=policy_service,
        phases=phases,
        serialize_result=serialize_result,
    )


async def run_validation_task(  # noqa: PLR0913
    resource_id: str,
    study_metadata_service_factory: StudyMetadataServiceFactory,
    policy_service: PolicyService,
    modifier_result: Union[dict, StudyMetadataModifierResult] = None,
    phases: Union[ValidationPhase, None, list[str]] = None,
    serialize_result: bool = True,
) -> Union[Dict[str, Any], PolicyResultList]:
    logger.info("Running validation for %s", resource_id)
    metadata_service = await study_metadata_service_factory.create_service(resource_id)
    result_list: PolicyResultList = PolicyResultList()
    if modifier_result and isinstance(modifier_result, dict):
        modifier_result = StudyMetadataModifierResult.model_validate(modifier_result)
    if modifier_result and modifier_result.has_error:
        logger.error(
            "Modifier failed for %s: %s.",
            resource_id,
            modifier_result.error_message or "",
        )
    if isinstance(phases, ValidationPhase):
        phases = [phases]
    elif isinstance(phases, list):
        phases = [
            ValidationPhase(x)
            for x in phases
            if isinstance(x, str) and x in validation_phase_names
        ]
    else:
        phases = all_validation_phases

    logger.debug(
        "Running %s validation for phases %s", resource_id, [str(x) for x in phases]
    )

    if not resource_id or not phases:
        logger.error("Invalid resource id or phases for %s", resource_id)
        raise ValueError(message="Inputs are not valid")

    try:
        logger.debug("Get MetaboLights validation input model.")
        model = await get_input_data(metadata_service, phases)
        logger.debug("Validate using policy service.")
        policy_result = await validate_by_policy_service(
            resource_id, model, modifier_result, policy_service
        )
        policy_result.phases = phases
        result_list.results.append(policy_result)

    except Exception as ex:
        logger.error("Validation task error for %s.", resource_id)
        logger.exception(ex)
        raise ex
    errors_count = sum(
        1
        for x in policy_result.messages.violations
        if x.type == PolicyMessageType.ERROR
    )

    logger.debug("Validation task ended: Validation Errors %s", errors_count)
    if serialize_result:
        return result_list.model_dump(by_alias=True)

    return result_list


async def validate_by_policy_service(
    resource_id: str,
    model: MetabolightsStudyModel,
    modifier_result: StudyMetadataModifierResult,
    policy_service: PolicyService,
) -> PolicyResult:
    policy_result: PolicyResult = PolicyResult()
    policy_result.resource_id = resource_id
    if modifier_result and modifier_result.resource_id:
        policy_result.metadata_modifier_enabled = True
        if modifier_result.error_message:
            policy_result.metadata_updates = [
                UpdateLog(action="Modifier failed. " + modifier_result.error_message)
            ]
        elif modifier_result.logs:
            policy_result.metadata_updates = modifier_result.logs
    start_time = time.time()

    for file in model.assays:
        technique = model.assays[file].assay_technique.name
        policy_result.assay_file_techniques[file] = technique

    for file in model.metabolite_assignments:
        technique = model.metabolite_assignments[file].assay_technique.name
        policy_result.maf_file_techniques[file] = technique

    try:
        messages = await policy_service.validate_study(resource_id, model)
        policy_result.start_time = datetime.datetime.fromtimestamp(
            start_time
        ).isoformat()
        policy_result.completion_time = datetime.datetime.fromtimestamp(
            time.time()
        ).isoformat()
        policy_result.messages = messages
    except Exception as ex:
        logger.error("Invalid OPA response or parse error for %s", resource_id)
        logger.exception(ex)
        raise ex
    return policy_result


async def get_input_data(
    metadata_service: StudyMetadataService,
    phases: list[ValidationPhase],
) -> MetabolightsStudyModel:
    phases.sort(key=lambda x: x.value)
    load_sample_file = False
    load_assay_files = False
    load_maf_files = False
    load_folder_metadata = False
    load_db_metadata = True
    for phase in phases:
        if phase == ValidationPhase.PHASE_2:
            load_sample_file = True
            load_assay_files = True
        elif phase == ValidationPhase.PHASE_3:
            load_sample_file = True
            load_maf_files = True
        elif phase == ValidationPhase.PHASE_4:
            load_folder_metadata = True
    return await metadata_service.load_study_model(
        load_sample_file=load_sample_file,
        load_assay_files=load_assay_files,
        load_maf_files=load_maf_files,
        load_folder_metadata=load_folder_metadata,
        load_db_metadata=load_db_metadata,
    )
