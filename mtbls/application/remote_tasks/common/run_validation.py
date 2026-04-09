import asyncio
import datetime
import json
import logging
import pathlib
import re
import shutil
import subprocess
import time
import uuid
from pathlib import Path
from typing import Any, Dict, OrderedDict, Union

from cachetools import TTLCache
from cachetools_async import cached
from dependency_injector.wiring import Provide, inject
from metabolights_utils.models.metabolights.model import MetabolightsStudyModel
from mhd_model.convertors.announcement.convertor import create_announcement_file
from mhd_model.model.v0_1.announcement.validation.validator import (
    MhdAnnouncementFileValidator,
)
from mhd_model.model.v0_1.dataset.validation.validator import validate_mhd_file
from mtbls2mhd.config import Mtbls2MhdConfiguration
from mtbls2mhd.convertor_factory import Mtbls2MhdConvertorFactory

from mtbls.application.decorators.async_task import async_task
from mtbls.application.remote_tasks.common.run_modifier import (
    run_isa_metadata_modifier_task,
)
from mtbls.application.remote_tasks.common.utils import run_coroutine
from mtbls.application.services.interfaces.async_task.async_task_result import (
    AsyncTaskResult,
)
from mtbls.application.services.interfaces.ontology_search_service import (
    OntologySearchService,
)
from mtbls.application.services.interfaces.policy_service import PolicyService
from mtbls.application.services.interfaces.repositories.file_object.file_object_write_repository import (  # noqa: E501
    FileObjectWriteRepository,
)
from mtbls.application.services.interfaces.study_metadata_service import (
    StudyMetadataService,
)
from mtbls.application.services.interfaces.study_metadata_service_factory import (
    StudyMetadataServiceFactory,
)
from mtbls.domain.entities.study_file import StudyDataFileOutput
from mtbls.domain.entities.validation.validation_configuration import (
    BaseOntologyValidation,
    FieldValueValidation,
    MetadataFileType,
    OntologyValidationType,
    StudyCategoryStr,
    ValidationControls,
)
from mtbls.domain.enums.study_category import StudyCategory
from mtbls.domain.shared.mhd_configuration import MhdConfiguration
from mtbls.domain.shared.modifier import StudyMetadataModifierResult, UpdateLog
from mtbls.domain.shared.validator.policy import (
    PolicyMessage,
    PolicyResult,
    PolicyResultList,
)
from mtbls.domain.shared.validator.run_configuration import (
    DbConfiguration,
    ValidationRunConfiguration,
)
from mtbls.domain.shared.validator.types import PolicyMessageType, ValidationPhase

logger = logging.getLogger(__name__)


async def create_validation_run_configuration(
    resource_id: str,
    metadata_files_object_repository: FileObjectWriteRepository,
    mhd_config: MhdConfiguration,
    private_metadata_files_root_path: str,
    db_connection: dict,
    temp_folder: Union[None, str] = None,
    apply_modifiers: bool = True,
    ignore_cv_term_validation: None | bool = None,
):
    if not temp_folder:
        temp_folder_path = pathlib.Path(f"/tmp/validation/{uuid.uuid4()}").resolve()
    else:
        temp_folder_path = pathlib.Path(temp_folder).resolve()
    try:
        repo = metadata_files_object_repository
        files: list[StudyDataFileOutput] = await repo.list(resource_id)
        result_files = [f for f in files if re.match(r"m_.+\.tsv$", f.basename)]
        local_result_files = []

        for result_file in result_files:
            local_result_file_path = (
                temp_folder_path
                / pathlib.Path(resource_id)
                / pathlib.Path(result_file.object_key)
            )
            local_result_files.append(local_result_file_path)
            local_result_file_path.parent.mkdir(parents=True, exist_ok=True)

            await repo.download(
                resource_id,
                result_file.object_key,
                target_path=str(local_result_file_path),
            )
        file_lines: dict[str, int] = calculate_file_lines(local_result_files)
        total_result_file_lines = 0
        if file_lines:
            total_result_file_lines = sum([x for x in file_lines.values()])
        validation_run_configuration = ValidationRunConfiguration(
            apply_modifiers=apply_modifiers,
            mhd_configuration=mhd_config,
            metadata_files_root_path=private_metadata_files_root_path,
            db_connection=DbConfiguration.model_validate(db_connection),
            ignore_cv_term_validation=ignore_cv_term_validation,
        )
        if total_result_file_lines > 4000:
            logger.warning(
                "Validation result MAF file lines exceed the limit: %d > 4000. "
                "MAF file content validation PHASE3 will be skipped.",
                total_result_file_lines,
            )
            validation_run_configuration.skip_result_file_modification = True
            validation_run_configuration.validation_phases = [
                x for x in ValidationPhase
            ]
            validation_run_configuration.assignmet_sheet_limit = 100
        return validation_run_configuration
    except Exception as ex:
        logger.error(
            "Creating validation configuration for %s failed: %s", resource_id, ex
        )
        logger.exception(ex)
        return ValidationRunConfiguration(
            apply_modifiers=apply_modifiers,
            mhd_configuration=mhd_config,
            metadata_files_root_path=private_metadata_files_root_path,
            db_connection=DbConfiguration.model_validate(db_connection),
        )
    finally:
        if temp_folder_path and temp_folder_path.exists():
            try:
                shutil.rmtree(temp_folder_path)
            except Exception as ex:
                logger.error(
                    "Temporary folder %s removal failed: %s",
                    str(temp_folder_path),
                    ex,
                )


def calculate_file_lines(file_paths: list[pathlib.Path]) -> dict[str, int]:
    if not file_paths:
        return {}
    files = [f for f in file_paths if f.is_file()]
    result = subprocess.check_output(["wc", "-l"] + files)
    return {
        line.split(maxsplit=1)[1]: int(line.split(maxsplit=1)[0])
        for line in result.decode().strip().split("\n")
        if line and line.strip() and line.split(maxsplit=1)[1] != "total"
    }


@async_task(queue="common")
@inject
def run_validation(  # noqa: PLR0913
    *,
    resource_id: str,
    apply_modifiers: bool = True,
    serialize_result: bool = True,
    ignore_cv_term_validation: None | bool = None,
    study_metadata_service_factory: StudyMetadataServiceFactory = Provide[
        "services.study_metadata_service_factory"
    ],
    policy_service: PolicyService = Provide["services.policy_service"],
    ontology_search_service: OntologySearchService = Provide[
        "services.ontology_search_service"
    ],
    temp_folder: Union[None, str] = None,
    metadata_files_object_repository: FileObjectWriteRepository = Provide[
        "repositories.metadata_files_object_repository"
    ],
    internal_files_object_repository: FileObjectWriteRepository = Provide[
        "repositories.internal_files_object_repository"
    ],
    mhd_config: MhdConfiguration = Provide["mhd_configuration"],
    private_metadata_files_root_path: str = Provide[
        "config.repositories.study_folders.mounted_paths.private_metadata_files_root_path"
    ],
    db_connection: dict = Provide["config.gateways.database.postgresql.connection"],
    **kwargs,
) -> AsyncTaskResult:
    validation_run_configuration = asyncio.run(
        create_validation_run_configuration(
            resource_id=resource_id,
            temp_folder=temp_folder,
            apply_modifiers=apply_modifiers,
            metadata_files_object_repository=metadata_files_object_repository,
            mhd_config=mhd_config,
            private_metadata_files_root_path=private_metadata_files_root_path,
            db_connection=db_connection,
        )
    )
    try:
        modifier_result = None
        if apply_modifiers:
            logger.info("Run validation with modifiers")
            coroutine = run_validation_task_with_modifiers(
                resource_id,
                study_metadata_service_factory=study_metadata_service_factory,
                internal_files_object_repository=internal_files_object_repository,
                policy_service=policy_service,
                serialize_result=serialize_result,
                ontology_search_service=ontology_search_service,
                validation_run_configuration=validation_run_configuration,
            )
        else:
            coroutine = run_validation_task(
                resource_id,
                modifier_result=modifier_result,
                study_metadata_service_factory=study_metadata_service_factory,
                internal_files_object_repository=internal_files_object_repository,
                policy_service=policy_service,
                serialize_result=serialize_result,
                ontology_search_service=ontology_search_service,
                validation_run_configuration=validation_run_configuration,
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
    internal_files_object_repository: FileObjectWriteRepository,
    policy_service: PolicyService,
    serialize_result: bool = True,
    ontology_search_service: None | OntologySearchService = None,
    validation_run_configuration: None | ValidationRunConfiguration = None,
) -> Union[Dict[str, Any], PolicyResultList]:
    try:
        modifier_result = await run_isa_metadata_modifier_task(
            resource_id,
            study_metadata_service_factory=study_metadata_service_factory,
            policy_service=policy_service,
            serialize_result=False,
            validation_run_configuration=validation_run_configuration,
        )
    except Exception as ex:
        logger.error("Error to modify %s: %s", resource_id, ex)
        logger.exception(ex)
        modifier_result = None

    return await run_validation_task(
        resource_id,
        modifier_result=modifier_result,
        study_metadata_service_factory=study_metadata_service_factory,
        internal_files_object_repository=internal_files_object_repository,
        policy_service=policy_service,
        serialize_result=serialize_result,
        ontology_search_service=ontology_search_service,
        validation_run_configuration=validation_run_configuration,
    )


async def run_validation_task(  # noqa: PLR0913
    resource_id: str,
    study_metadata_service_factory: StudyMetadataServiceFactory,
    internal_files_object_repository: FileObjectWriteRepository,
    policy_service: PolicyService,
    modifier_result: Union[None, dict, StudyMetadataModifierResult] = None,
    serialize_result: bool = True,
    ontology_search_service: None | OntologySearchService = None,
    validation_run_configuration: None | ValidationRunConfiguration = None,
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
    if not validation_run_configuration:
        validation_run_configuration = ValidationRunConfiguration()
    phases = validation_run_configuration.validation_phases
    logger.debug(
        "Running %s validation for phases %s", resource_id, [str(x) for x in phases]
    )

    if not resource_id or not phases:
        logger.error("Invalid resource id or phases for %s", resource_id)
        raise ValueError(message="Inputs are not valid")

    try:
        logger.debug("Get MetaboLights validation input model.")
        model = await get_input_data(
            metadata_service,
            phases,
            assignment_sheet_limit=validation_run_configuration.assignmet_sheet_limit,
        )
        logger.debug("Validate using policy service.")
        policy_result = await validate_by_policy_service(
            resource_id,
            model,
            modifier_result,
            policy_service,
            ontology_search_service,
        )
        errors = [
            x
            for x in policy_result.messages.violations
            if x.type == PolicyMessageType.ERROR
        ]
        try:
            await process_mhd_study(
                policy_result,
                resource_id,
                model,
                policy_service,
                internal_files_object_repository,
                validation_run_configuration=validation_run_configuration,
            )
        except Exception as ex:
            logger.exception(ex)
            logger.error("Failed to convert and validate MHD study.")
            if not errors:
                policy_result.messages.violations.append(
                    PolicyMessage(
                        type=PolicyMessageType.ERROR,
                        section="general",
                        source_file="input",
                        priority="CRITICAL",
                        identifier="rule___500_100_001_01",
                        title="MetabolomicsHub model validation error",
                        description="Current study does not comply with "
                        "MetabolomicsHub requirements now. "
                        "contact MetaboLights team for help.",
                        violation="Study MHD model validation failed.",
                        values=[str(ex)],
                    )
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
    modifier_result: None | StudyMetadataModifierResult,
    policy_service: PolicyService,
    ontology_search_service: None | OntologySearchService = None,
) -> PolicyResult:
    policy_result: PolicyResult = PolicyResult()
    policy_result.resource_id = resource_id
    if modifier_result and modifier_result.resource_id:
        policy_result.metadata_modifier_enabled = True
        if modifier_result.error_message:
            policy_result.metadata_updates = [
                UpdateLog(
                    action="Modifier failed. " + modifier_result.error_message,
                    source="",
                    old_value="",
                    new_value="",
                )
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
    await post_process_validation_messages(
        model, policy_result, policy_service, ontology_search_service
    )
    return policy_result


async def process_mhd_study(
    policy_result: PolicyResult,
    resource_id: str,
    model: MetabolightsStudyModel,
    policy_service: PolicyService,
    internal_files_object_repository: FileObjectWriteRepository,
    validation_run_configuration: ValidationRunConfiguration,
):
    templates = await policy_service.get_templates()
    template_version = model.study_db_metadata.template_version
    category = model.study_db_metadata.study_category
    category_label = category.name.lower().replace("_", "-")

    category_str = StudyCategoryStr(category_label)
    version_settings = templates.configuration.versions.get(template_version)

    if category_str in version_settings.active_mhd_profiles:
        mhd_file_path = None
        announcement_file_path = None
        mhd_model_version = model.study_db_metadata.mhd_model_version
        mhd_accession = model.study_db_metadata.reserved_mhd_accession
        profile_settings = version_settings.active_mhd_profiles.get(category_str)

        if mhd_model_version in profile_settings.active_versions:
            profile_info = templates.configuration.mhd_profiles.get(
                profile_settings.profile_name, {}
            ).get(mhd_model_version)
            if profile_info:
                schema_uri = profile_info.file_schema
                profile_uri = profile_info.mhd_file_profile
                config = validation_run_configuration
                mtbls2mhd_config = Mtbls2MhdConfiguration(
                    database_name=config.db_connection.database,
                    database_user=config.db_connection.user,
                    database_user_password=config.db_connection.password,
                    database_host=config.db_connection.host,
                    database_host_port=config.db_connection.port,
                    mtbls_studies_root_path=config.metadata_files_root_path,
                    selected_schema_uri=schema_uri,
                    selected_profile_uri=profile_uri,
                    public_http_base_url=config.mhd_configuration.public_study_base_url,
                    public_ftp_base_url=config.mhd_configuration.public_ftp_base_url,
                    study_http_base_url=config.mhd_configuration.study_http_base_url,
                    default_dataset_licence_url=model.study_db_metadata.dataset_license_url,
                )
                (
                    mhd_file_path,
                    announcement_file_path,
                    mhd_validation_file_path,
                ) = await validate_mhd_study(
                    policy_result,
                    resource_id,
                    mhd_accession,
                    schema_uri,
                    profile_uri,
                    mhd_filename=f"{resource_id}.mhd.json",
                    annoucement_filename=f"{resource_id}.announcement.json",
                    config=mtbls2mhd_config,
                )
            else:
                logger.error(
                    "MHD version %s is not supported for %s",
                    mhd_model_version,
                    resource_id,
                )
        else:
            logger.error(
                "MHD version %s is not supported for %s", mhd_model_version, resource_id
            )
        for x in await internal_files_object_repository.list(resource_id, "DATA_FILES"):
            object_key = x.object_key or ""
            if (
                object_key.endswith(".mhd.json")
                or object_key.endswith(".announcement.json")
                or object_key.endswith(".mhd.validation.json")
            ):
                await internal_files_object_repository.delete_object(
                    resource_id, object_key
                )

        if mhd_file_path and Path(mhd_file_path).exists():
            await internal_files_object_repository.put_object(
                resource_id,
                f"DATA_FILES/{resource_id}.mhd.json",
                f"file://{mhd_file_path}",
                override=True,
            )
        if announcement_file_path and Path(announcement_file_path).exists():
            await internal_files_object_repository.put_object(
                resource_id,
                f"DATA_FILES/{resource_id}.announcement.json",
                f"file://{announcement_file_path}",
                override=True,
            )
        if mhd_validation_file_path and Path(mhd_validation_file_path).exists():
            await internal_files_object_repository.put_object(
                resource_id,
                f"DATA_FILES/{resource_id}.mhd.validation.json",
                f"file://{mhd_validation_file_path}",
                override=True,
            )
    return True


async def validate_mhd_study(
    policy_result: PolicyResult,
    resource_id: str,
    mhd_accession: None | str,
    schema_uri: str,
    profile_uri: str,
    mhd_output_root_path: None | Path = None,
    mhd_filename: None | str = None,
    annoucement_filename: None | str = None,
    config: None | Mtbls2MhdConfiguration = None,
) -> str:
    factory = Mtbls2MhdConvertorFactory()
    current_errors = [
        x
        for x in policy_result.messages.violations
        if x.type == PolicyMessageType.ERROR
    ]
    convertor = factory.get_convertor(
        target_mhd_model_schema_uri=schema_uri, target_mhd_model_profile_uri=profile_uri
    )
    if not mhd_output_root_path:
        timestamp = int(datetime.datetime.now().timestamp())
        mhd_output_root_path = Path(f"/tmp/mhd-validation/{resource_id}/{timestamp}")
    mhd_output_root_path.mkdir(exist_ok=True, parents=True)
    mhd_accession_file_prefix = mhd_accession or resource_id
    if not mhd_filename:
        mhd_filename = f"{mhd_accession_file_prefix}.mhd.json"
    if not annoucement_filename:
        annoucement_filename = f"{mhd_accession_file_prefix}.announcement.json"
    announcement_file_path = mhd_output_root_path / Path(annoucement_filename)
    mhd_file_path = mhd_output_root_path / Path(mhd_filename)
    mhd_validation_file_path = mhd_output_root_path / Path(
        f"{mhd_accession_file_prefix}.mhd.validation.json"
    )
    mhd_validation_errors: OrderedDict[str, OrderedDict[str, str]] = OrderedDict()
    convertor.convert(
        repository_name="MetaboLights",
        repository_identifier=resource_id,
        mhd_identifier=mhd_accession or None,
        mhd_output_folder_path=mhd_output_root_path,
        mhd_output_filename=mhd_filename,
        config=config,
    )
    if mhd_file_path.exists():
        logger.info("mhd common model file is created on %s", mhd_file_path)
        validation_errors = validate_mhd_file(str(mhd_file_path))

        if validation_errors:
            logger.info(
                "MHD model validation errors found for %s: %s",
                resource_id,
                validation_errors,
            )
            mhd_validation_errors["mhd_model_errors"] = OrderedDict()

            for key, error in validation_errors:
                mhd_validation_errors["mhd_model_errors"][key] = str(error)
            if not current_errors:
                errors = []
                for key, error in validation_errors:
                    errors.append(f"{key}: {error}")
                policy_result.messages.violations.append(
                    create_mhd_error_message(
                        "rule___500_100_001_02",
                        "MetabolomicsHub common model file",
                        errors,
                    )
                )
        else:
            file_content = json.loads(mhd_file_path.read_text())
            create_announcement_file(
                file_content,
                f"{config.public_http_base_url}/{resource_id}/{mhd_filename}",
                str(announcement_file_path),
            )
            if announcement_file_path.exists():
                file_content = json.loads(announcement_file_path.read_text())
                validator = MhdAnnouncementFileValidator()
                errors = validator.validate(file_content)
                if errors:
                    mhd_validation_errors["mhd_announcement_errors"] = OrderedDict()
                    for key, error in errors:
                        mhd_validation_errors["mhd_announcement_errors"][key] = str(
                            error
                        )

                if errors and not current_errors:
                    policy_result.messages.violations.append(
                        create_mhd_error_message(
                            "rule___500_100_002_02",
                            "MetabolomicsHub announcement file",
                            errors,
                        )
                    )
            else:
                mhd_validation_errors["mhd_announcement_errors"] = OrderedDict()
                message = "MHD announcement file creation failed."
                mhd_validation_errors["mhd_announcement_errors"]["file"] = message
                if not current_errors:
                    policy_result.messages.violations.append(
                        create_mhd_error_message(
                            "rule___500_100_002_01",
                            "MetabolomicsHub announcement file",
                            [message],
                        )
                    )
    else:
        mhd_validation_errors["mhd_model_errors"] = OrderedDict()
        message = "MHD common model file creation failed."
        mhd_validation_errors["mhd_model_errors"]["file"] = message
        if not current_errors:
            policy_result.messages.violations.append(
                create_mhd_error_message(
                    "rule___500_100_001_01",
                    "MetabolomicsHub common model file",
                    [message],
                )
            )
    if mhd_validation_errors:
        logger.info("MHD validation errors are saved on %s", mhd_validation_file_path)
        mhd_validation_errors["status"] = "failed"
    else:
        logger.debug("%s MHD file creation and validation is successfull.", resource_id)
        mhd_validation_errors["status"] = "success"
    with mhd_validation_file_path.open("w") as f:
        json.dump(mhd_validation_errors, f, indent=4)
    return mhd_file_path, announcement_file_path, mhd_validation_file_path


def create_mhd_error_message(identifier: str, mhd_file_type: str, errors: list[str]):
    return PolicyMessage(
        type=PolicyMessageType.ERROR,
        section="general",
        source_file="input",
        priority="CRITICAL",
        identifier=identifier,
        title=f"{mhd_file_type} validation error",
        description="Current study does not comply with "
        "MetabolomicsHub requirements. "
        "Please contact MetaboLights team for help.",
        violation=f"{mhd_file_type} validation failed. " + ", ".join(errors),
        values=errors,
    )


def investigation_value_parser(value: str) -> tuple[None | str, None | str, None | str]:
    parts = value.split("\t")
    if len(parts) == 3:
        _, _, part_3 = value.split("\t")
        term, source, accession = [
            x.strip() for x in part_3.strip("[]").split(",", maxsplit=2)
        ]
    else:
        logger.error("The value has no three parts: %s", value)
        return None, None, None

    return term, source, accession


def isa_table_value_parser(value: str) -> tuple[None | str, None | str, None | str]:
    term, source, accession = [
        x.strip() for x in value.strip("[]").split(",", maxsplit=2)
    ]
    return term, source, accession


def find_rule(
    controls: ValidationControls,
    isa_table_type: str,
    study_category: str,
    template_version: str,
    template_name: str,
    created_at: str,
) -> None | FieldValueValidation:
    selected_controls: dict[str, list[FieldValueValidation]] = getattr(
        controls, isa_table_type + "_file_controls"
    )
    control_list = selected_controls.get(template_name, [])
    rule = None
    for control in control_list:
        criteria = control.selection_criteria
        match = all(
            [
                match_equal(criteria.isa_file_type, isa_table_type),
                match_equal(criteria.study_category_filter, study_category),
                match_equal(criteria.template_version_filter, template_version),
                match_equal(criteria.isa_file_template_name_filter, template_name),
                match_ge(criteria.study_created_at_or_after, created_at),
                match_less(criteria.study_created_before, created_at),
            ]
        )
        if match:
            rule = control
            break

    return rule


def match_equal(criterion, val) -> bool:
    if not criterion:
        return True
    if isinstance(criterion, list):
        if any([x for x in criterion if str(x) == val]):
            return True
    elif isinstance(criterion, str) and criterion == val:
        return True

    return False


def match_ge(criterion, val) -> bool:
    if not criterion:
        return True
    return val >= criterion


def match_less(criterion, val) -> bool:
    if not criterion:
        return True
    return val < criterion


def escape(s: str) -> str:
    return s.replace("\t", " ").replace("\n", " ")


@cached(cache=TTLCache(maxsize=2048, ttl=600))
async def search_exact_match_term(
    ontology_search_service: OntologySearchService,
    term: str,
    rule: FieldValueValidation,
):
    return await ontology_search_service.search(term, rule, exact_match=True)


async def post_process_validation_messages(
    model: MetabolightsStudyModel,
    policy_result: PolicyResult,
    policy_service: PolicyService,
    ontology_search_service: None | OntologySearchService = None,
) -> None:
    if not ontology_search_service or not policy_result:
        return None
    controls = await policy_service.get_control_lists()
    file_templates = await policy_service.get_templates()

    search_validation_rules = {
        "rule_a_200_900_001_01": ("assay", isa_table_value_parser),
        "rule_s_200_900_001_01": ("sample", isa_table_value_parser),
        "rule_i_200_900_001_01": ("investigation", investigation_value_parser),
        "rule_m_200_900_001_01": ("assignment", isa_table_value_parser),
    }
    default_rule_value = ("", lambda x: "", "", "", MetadataFileType.INVESTIGATION)

    new_violations = []
    if not controls:
        logger.error("Policy service does not return control lists")
        return
    created_at = model.study_db_metadata.created_at
    sample_template = model.study_db_metadata.sample_template
    template_version = model.study_db_metadata.template_version

    study_category = model.study_db_metadata.study_category
    category_name = study_category.name.lower().replace("_", "-")

    search_keys = set(search_validation_rules.keys())
    for violation in policy_result.messages.violations:
        identifier = violation.identifier
        if identifier not in search_keys:
            new_violations.append(violation)
            continue
        search_params = search_validation_rules.get(identifier, default_rule_value)
        isa_table_type = search_params[0]
        parser = search_params[1]

        template_name = ""
        if isa_table_type == "sample":
            template_name = sample_template
        elif isa_table_type == "assay":
            assay_file = model.assays.get(violation.source_file, None)
            if assay_file:
                template_name = assay_file.assay_technique.name

        new_values = []
        deleted_values = []
        default_controls = file_templates.configuration.default_file_controls.get(
            MetadataFileType(isa_table_type), []
        )
        field_key = violation.source_column_header
        if not field_key:
            field_key = "__default__"
        default_template_key = None
        for default_control in default_controls:
            match = re.match(default_control.key_pattern, field_key)
            if match:
                default_template_key = default_control.default_key
                break
        default_rule = None
        if default_template_key:
            default_rule = find_rule(
                controls,
                isa_table_type,
                category_name,
                template_version,
                default_template_key,
                created_at,
            )

        rule = find_rule(
            controls,
            isa_table_type,
            category_name,
            template_version,
            template_name,
            created_at,
        )
        selected_rule = rule or default_rule
        is_child_rule = (
            rule and rule.validation_type == OntologyValidationType.CHILD_ONTOLOGY_TERM
        )
        parents = []
        if is_child_rule and rule and rule.allowed_parent_ontology_terms:
            parents = rule.allowed_parent_ontology_terms.parents
        for value in violation.values:
            term, source, accession = parser(value)
            if is_exceptional_term(selected_rule, term, source, accession):
                deleted_values.append(value)
                continue

            if is_child_rule and rule and term:
                search = await search_exact_match_term(
                    ontology_search_service, term, rule
                )
                if not search.result:
                    logger.warning("'%s' is not valid or a child of parents.", value)
                    new_values.append(value)
                elif (
                    search.result[0].term_accession_number != accession
                    or search.result[0].term_source_ref != source
                ):
                    logger.warning(
                        "Current: %s search result: [%s, %s, %s]",
                        value,
                        search.result[0].term,
                        search.result[0].term_source_ref,
                        search.result[0].term_accession_number,
                    )
                    new_values.append(value)
                else:
                    deleted_values.append(value)

            elif accession and source:
                search = await ontology_search_service.search(
                    accession,
                    rule=BaseOntologyValidation(
                        rule_name="exact-term-search-01",
                        field_name="generic",
                        validation_type=OntologyValidationType.SELECTED_ONTOLOGY,
                        ontologies=[source],
                    ),
                    exact_match=True,
                )
                if not search.result:
                    logger.warning("'%s' is not found on ontology service", value)
                    new_values.append(value)
                elif (
                    search.result[0].term != term
                    or search.result[0].term_source_ref != source
                    or search.result[0].term_accession_number != accession
                ):
                    logger.warning(
                        "Term '%s' (%s) not found in %s",
                        search.result[0].term,
                        accession,
                        source,
                    )
                    new_values.append(value)
                else:
                    deleted_values.append(value)

        if new_values:
            if isa_table_type in {"assay", "sample"}:
                field = violation.source_column_header
            else:
                field = new_values[0].split("\t")[0]

            if is_child_rule:
                violation.violation += (
                    f"{field} ontology terms not found on ontology search service"
                    + " or they are not children of parent terms."
                    + "Ontology terms: "
                    + ", ".join([escape(x) for x in new_values])
                    + " Parents: "
                    + ", ".join([str(x) for x in parents])
                )
            else:
                violation.violation = (
                    f"{field} ontology terms not found on ontology search service: "
                    + ", ".join([escape(x) for x in new_values])
                )
            violation.values = new_values
            if study_category in (StudyCategory.MS_MHD_ENABLED,):
                violation.type = PolicyMessageType.ERROR
            new_violations.append(violation)
        else:
            logger.debug(
                "Terms in violation are validated and the violation is removed: %s %s %s",
                violation.identifier,
                violation.source_file,
                ", ".join(violation.values),
            )
    policy_result.messages.violations = new_violations


def is_exceptional_term(
    default_rule: FieldValueValidation, term: str, source: str, accession: str
):
    if not default_rule:
        return False
    accession = accession or ""
    source = source or ""
    term = term or ""
    if default_rule.allowed_placeholders:
        for item in default_rule.allowed_placeholders:
            if (
                item.term_accession_number == accession
                and item.term_source_ref == source
            ):
                return True
    if default_rule.allowed_other_sources:
        for item in default_rule.allowed_other_sources:
            if (
                accession.startswith(item.accession_prefix)
                and item.source_label == source
            ):
                return True
    if default_rule.allowed_missing_ontology_terms:
        for item in default_rule.allowed_missing_ontology_terms:
            if (
                item.term == term
                and item.term_accession_number == accession
                and item.term_source_ref == source
            ):
                return True
        return False


async def get_input_data(
    metadata_service: StudyMetadataService,
    phases: list[ValidationPhase],
    assignment_sheet_limit: None | int = None,
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
        assignment_sheet_limit=assignment_sheet_limit,
    )
