import logging
from typing import Any, Dict

from dependency_injector.wiring import Provide, inject
from metabolights_utils.models.enums import GenericMessageType

from mtbls.application.decorators.async_task import async_task
from mtbls.application.remote_tasks.common.utils import run_coroutine
from mtbls.application.services.interfaces.async_task.async_task_result import (
    AsyncTaskResult,
)
from mtbls.application.services.interfaces.policy_service import PolicyService
from mtbls.application.services.interfaces.study_metadata_service_factory import (
    StudyMetadataServiceFactory,
)
from mtbls.domain.domain_services.modifier.metabolights_study_model_modifier import (
    MetabolightsStudyModelModifier,
)
from mtbls.domain.entities.validation.validation_configuration import (
    FileTemplates,
    ValidationControls,
)
from mtbls.domain.shared.modifier import StudyMetadataModifierResult

logger = logging.getLogger(__name__)


@async_task(queue="common")
@inject
def run_isa_metadata_modifier(
    resource_id: str,
    study_metadata_service_factory: StudyMetadataServiceFactory = Provide[
        "services.study_metadata_service_factory"
    ],
    policy_service: PolicyService = Provide["services.policy_service"],
    **kwargs,
) -> AsyncTaskResult:
    try:
        coroutine = run_isa_metadata_modifier_task(
            resource_id,
            study_metadata_service_factory=study_metadata_service_factory,
            policy_service=policy_service,
        )
        return run_coroutine(coroutine)

    except Exception as ex:
        logger.error("Modifier task execution for %s failed.", resource_id)
        logger.exception(ex)
        raise ex
    finally:
        logger.info("Modifier task execution for %s ended.", resource_id)


async def run_isa_metadata_modifier_task(
    resource_id: str,
    study_metadata_service_factory: StudyMetadataServiceFactory,
    policy_service: PolicyService,
    serialize_result: bool = True,
) -> Dict[str, Any]:
    metadata_service = await study_metadata_service_factory.create_service(resource_id)
    with metadata_service:
        modifier_model = await metadata_service.load_study_model(
            load_sample_file=True,
            load_assay_files=True,
            load_maf_files=True,
            load_folder_metadata=True,
            load_db_metadata=True,
        )

        folder_errors = [
            x
            for x in modifier_model.folder_reader_messages
            if x.type == GenericMessageType.ERROR
        ]
        if folder_errors:
            raise Exception(
                f"Study load error:  {folder_errors[0].short} {folder_errors[0].detail}"
            )
        control_lists: ValidationControls = await policy_service.get_control_lists()
        templates: FileTemplates = await policy_service.get_templates()
        modifier = MetabolightsStudyModelModifier(
            model=modifier_model, templates=templates, control_lists=control_lists
        )

        modifier.modify()

        result = StudyMetadataModifierResult(
            resource_id=resource_id, logs=modifier.update_logs
        )

        if modifier.update_logs:
            logger.info(
                "%s modifier results: %d number of updates.",
                resource_id,
                len(modifier.update_logs),
            )
            logger.info("Create metadata snapshot for %s", resource_id)
            await metadata_service.create_metadata_snapshot(suffix="VALIDATION")
            logger.info("Override %s metadata files", resource_id)
            await metadata_service.save_study_model(modifier_model)
        else:
            logger.info("There is no modification for %s.", resource_id)

        if serialize_result:
            return result.model_dump(by_alias=True)
        return result
