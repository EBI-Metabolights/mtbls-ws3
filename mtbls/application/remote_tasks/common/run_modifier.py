import logging
from typing import Any, Dict

from dependency_injector.wiring import Provide, inject
from metabolights_utils.models.enums import GenericMessageType
from metabolights_utils.models.parser.enums import ParserMessageType

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
from mtbls.domain.shared.validator.run_configuration import (
    ValidationRunConfiguration,
)

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
    validation_run_configuration: None | ValidationRunConfiguration = None,
) -> Dict[str, Any]:
    metadata_service = await study_metadata_service_factory.create_service(resource_id)
    load_maf_files = True
    if not validation_run_configuration:
        validation_run_configuration = ValidationRunConfiguration()
    if validation_run_configuration.skip_result_file_modification:
        load_maf_files = False
    with metadata_service:
        modifier_model = await metadata_service.load_study_model(
            load_sample_file=True,
            load_assay_files=True,
            load_maf_files=load_maf_files,
            load_folder_metadata=True,
            load_db_metadata=True,
        )
        result = StudyMetadataModifierResult(resource_id=resource_id)
        folder_errors = [
            x
            for x in modifier_model.folder_reader_messages
            if x.type == GenericMessageType.ERROR
        ]
        if folder_errors:
            result.error_message = (
                "Study folder load error:  "
                f"{folder_errors[0].short} {folder_errors[0].detail}"
            )
        parse_errors = []
        for _, messages in modifier_model.parser_messages.items():
            parse_errors.extend(
                [x for x in messages if x.type in (ParserMessageType.CRITICAL,)]
            )
        if parse_errors:
            result.error_message = f"Study file parse errors:  {parse_errors}"

        control_lists: ValidationControls = await policy_service.get_control_lists()
        templates: FileTemplates = await policy_service.get_templates()
        config_load_failure = not control_lists or not templates
        if config_load_failure:
            result.error_message = "Control lists or templates are not fetched"

        if parse_errors or folder_errors or config_load_failure:
            result.has_error = True
            if serialize_result:
                return result.model_dump(by_alias=True)
            return result

        modifier = MetabolightsStudyModelModifier(
            model=modifier_model,
            templates=templates,
            control_lists=control_lists,
            config=validation_run_configuration,
        )
        try:
            result.logs = modifier.modify()
        except Exception as ex:
            result.logs = modifier.update_logs
            result.has_error = True
            result.error_message = str(ex)

        if not result.has_error:
            if modifier.update_logs:
                logger.info(
                    "%s modifier results: %d number of updates.",
                    resource_id,
                    len(modifier.update_logs),
                )
                logger.info("Create metadata snapshot for %s", resource_id)
                await metadata_service.create_metadata_snapshot(suffix="VALIDATION")
                logger.info("Override %s metadata files", resource_id)
                save_result_files = (
                    not validation_run_configuration.skip_result_file_modification
                )
                await metadata_service.save_study_model(
                    modifier_model, save_result_files=save_result_files
                )
            else:
                logger.debug("There is no modification for %s.", resource_id)
        else:
            logger.info(
                "Modification error for %s: %s", resource_id, result.error_message
            )

        if serialize_result:
            return result.model_dump(by_alias=True)
        return result
