import asyncio
import logging
import uuid
from pathlib import Path
from typing import Union

import click
from metabolights_utils.models.metabolights.model import MetabolightsStudyModel
from metabolights_utils.utils.audit_utils import MetabolightsAuditUtils

from mtbls.application.context.request_tracker import (
    RequestTrackerModel,
    get_request_tracker,
)
from mtbls.application.remote_tasks.common.run_validation import (
    validate_by_policy_service,
)
from mtbls.application.services.interfaces.policy_service import PolicyService
from mtbls.application.services.interfaces.repositories.file_object.file_object_read_repository import (  # noqa: E501
    FileObjectReadRepository,
)
from mtbls.application.services.interfaces.repositories.study.study_read_repository import (  # noqa: E501
    StudyReadRepository,
)
from mtbls.application.services.interfaces.repositories.user.user_read_repository import (  # noqa: E501
    UserReadRepository,
)
from mtbls.application.services.interfaces.study_metadata_service import (
    StudyMetadataService,
)
from mtbls.application.services.study_metadata_service.default_study_provider import (
    DataFileIndexMetabolightsStudyProvider,
    DefaultMetabolightsStudyProvider,
)
from mtbls.application.use_cases.validation.validation_task import (
    convert_to_summary_result,
    get_report_content_from_summary_report,
)
from mtbls.domain.domain_services.modifier.metabolights_study_model_modifier import (
    MetabolightsStudyModelModifier,
)
from mtbls.domain.shared.modifier import StudyMetadataModifierResult
from mtbls.domain.shared.validator.policy import (
    PolicyResult,
    PolicyResultList,
    PolicySummaryResult,
)
from mtbls.domain.shared.validator.types import PolicyMessageType, ValidationPhase
from mtbls.run.cli.validation.container import MtblsCliApplicationContainer
from mtbls.run.config_utils import set_application_configuration


@click.command(no_args_is_help=True, name="validation")
@click.option(
    "--config-file",
    "-c",
    default="config.yaml",
    help="Local config path.",
)
@click.option(
    "--secrets-file",
    "-s",
    default="config-secrets.yaml",
    help="config secrets file path.",
)
@click.option(
    "--validation-reports-root-path",
    "-r",
    default=".validations",
    help="Validation report root path. ",
)
@click.option(
    "--summary-file",
    "-o",
    default="validation-summary.tsv",
    help="Validation results' summary file.",
)
@click.argument("studies_root_path")
def run_validation_cli(
    studies_root_path: str,
    validation_reports_root_path: Union[None, str] = None,
    summary_file: Union[None, str] = None,
    config_file: Union[None, str] = None,
    secrets_file: Union[None, str] = None,
):
    container: MtblsCliApplicationContainer = MtblsCliApplicationContainer()
    success = set_application_configuration(container, config_file, secrets_file)
    if not success:
        click.echo("Configuration error")
        exit(1)
    container.init_resources()
    policy_service: PolicyService = container.services.policy_service()

    study_read_repository: StudyReadRepository = (
        container.repositories.study_read_repository()
    )
    internal_files_object_repository: FileObjectReadRepository = (
        container.repositories.internal_files_object_repository()
    )
    Path(validation_reports_root_path).mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(__name__)
    logger.info("CLI container started")
    request_tracker = get_request_tracker()
    request_tracker.update_request_tracker(
        RequestTrackerModel(
            user_id=0,
            route_path="cli/validate-studies",
            resource_id="",
            client="local",
            request_id=str(uuid.uuid4()),
            task_id="",
        )
    )

    asyncio.run(
        run_validation_and_save_report(
            validation_reports_root_path=Path(validation_reports_root_path),
            studies_root_path=Path(studies_root_path),
            summary_file=Path(summary_file),
            policy_service=policy_service,
            study_read_repository=study_read_repository,
            internal_files_object_repository=internal_files_object_repository,
        )
    )


@click.command(name="create-model")
@click.argument("studies_root_path")
@click.argument("resource_id")
@click.argument("target_path")
def create_study_model_task(
    studies_root_path: str,
    resource_id: str,
    target_path: str,
    config_file: Union[None, str] = None,
    secrets_file: Union[None, str] = None,
):
    container: MtblsCliApplicationContainer = MtblsCliApplicationContainer()
    success = set_application_configuration(container, config_file, secrets_file)
    if not success:
        click.echo("Configuration error")
        exit(1)
    container.init_resources()
    # render secrets in config file

    study_read_repository: StudyReadRepository = (
        container.repositories.study_read_repository()
    )
    internal_files_object_repository: FileObjectReadRepository = (
        container.repositories.internal_files_object_repository()
    )
    user_read_repository: UserReadRepository = (
        container.repositories.user_read_repository()
    )
    logger = logging.getLogger(__name__)
    logger.info("CLI container started")
    request_tracker = get_request_tracker()
    request_tracker.update_request_tracker(
        RequestTrackerModel(
            user_id=0,
            route_path="cli/create-model",
            resource_id="",
            client="local",
            request_id=str(uuid.uuid4()),
            task_id="",
        )
    )

    asyncio.run(
        create_model(
            studies_root_path=Path(studies_root_path),
            resource_id=resource_id,
            target_path=Path(target_path),
            study_read_repository=study_read_repository,
            internal_files_object_repository=internal_files_object_repository,
            user_read_repository=user_read_repository,
        )
    )


async def create_model(
    studies_root_path: Path,
    resource_id: str,
    target_path: Path,
    study_read_repository: StudyReadRepository,
    user_read_repository: UserReadRepository,
    internal_files_object_repository: FileObjectReadRepository,
):
    provider = DataFileIndexMetabolightsStudyProvider(
        resource_id=resource_id,
        data_file_index_file_key="DATA_FILES/data_file_index.json",
        internal_files_object_repository=internal_files_object_repository,
        study_read_repository=study_read_repository,
        user_read_repository=user_read_repository,
    )
    study_path = studies_root_path / Path(resource_id)
    model = await provider.load_study(
        study_id=resource_id,
        study_path=str(study_path),
        connection=provider.db_metadata_collector,
        load_sample_file=True,
        load_assay_files=True,
        load_maf_files=True,
        load_folder_metadata=True,
    )

    with target_path.open("w") as f:
        f.write(model.model_dump_json(indent=4, by_alias=True))


async def run_validation_and_save_report(
    validation_reports_root_path: Path,
    studies_root_path: Path,
    summary_file: Path,
    policy_service: PolicyService,
    study_read_repository: StudyReadRepository,
    user_read_repository: UserReadRepository,
    internal_files_object_repository: FileObjectReadRepository,
) -> PolicySummaryResult:
    logger = logging.getLogger(__name__)
    resource_id = "MTBLS1"
    study = await study_read_repository.get_study_by_accession(resource_id)
    resource_ids = [(study.accession_number, study.release_date, study.status)]
    # resource_ids = await study_read_repository.select_fields(
    #     query_field_options=QueryFieldOptions(
    #         selected_fields=["accession_number", "release_date", "status"],
    #         filters=[EntityFilter(key="status", value=StudyStatus.PUBLIC)],
    #         sort_options=[SortOption(key="submission_date")],
    #     ),
    # )
    # selection = {"REQ20250629211506"}
    provider = DataFileIndexMetabolightsStudyProvider(
        resource_id=resource_id,
        data_file_index_file_key="DATA_FILES/data_file_index.json",
        internal_files_object_repository=internal_files_object_repository,
        study_read_repository=study_read_repository,
        user_read_repository=user_read_repository,
    )

    with summary_file.open("w") as fw:
        for resource_id, release_date, status in resource_ids:
            release_date_str = release_date.strftime("%Y-%m-%d %H-%M-%S")
            study_path = studies_root_path / Path(resource_id)
            audit_folder_root_path = study_path / Path("AUDIT_FILES")
            report_path = validation_reports_root_path / Path(
                f"{resource_id}_validation.tsv"
            )
            try:
                # Load study
                logger.debug("Loading study: %s on %s", resource_id, study_path)
                model = await provider.load_study(
                    study_id=resource_id,
                    study_path=str(study_path),
                    connection=provider.db_metadata_collector,
                    load_sample_file=True,
                    load_assay_files=True,
                    load_maf_files=True,
                    load_folder_metadata=True,
                )
                # Run modifier
                logger.debug("Running modifiers on study %s folder", resource_id)
                modifier_result = await modify_model(
                    resource_id=resource_id, model=model, policy_service=policy_service
                )

                if modifier_result.has_error:
                    logger.error(
                        "Modifier error for %s : %s",
                        resource_id,
                        modifier_result.error_message,
                    )
                    # raise ValueError(modifier_result.error_message)

                audit_folder_name = ""
                if modifier_result.logs:
                    folder_path = MetabolightsAuditUtils.create_audit_folder(
                        str(study_path), str(audit_folder_root_path), "VALIDATION"
                    )
                    audit_folder_name = Path(folder_path).name

                    logger.info(
                        "Audit folder created for study %s: %s",
                        resource_id,
                        audit_folder_name,
                    )
                    await StudyMetadataService.save_metabolights_study_model(
                        mtbls_model=model, output_dir=str(study_path)
                    )
                else:
                    logger.debug(
                        "No model change after modifiers for study: %s.", resource_id
                    )
                updated_model = await get_input_data(
                    study_model_provider=provider,
                    study_id=resource_id,
                    study_path=str(study_path),
                    phases=[
                        ValidationPhase.PHASE_1,
                        ValidationPhase.PHASE_2,
                        # ValidationPhase.PHASE_3,
                        ValidationPhase.PHASE_4,
                    ],
                    connection=provider,
                )
                logger.info("Running validation for study: %s.", resource_id)
                result_list: PolicyResultList = PolicyResultList()
                result: PolicyResult = await validate_by_policy_service(
                    resource_id=resource_id,
                    model=updated_model,
                    modifier_result=modifier_result,
                    policy_service=policy_service,
                )

                result_list.results.append(result)
                summary_result: PolicySummaryResult = await convert_to_summary_result(
                    resource_id=resource_id, result_list=result_list
                )
                summary_report = await get_report_content_from_summary_report(
                    summary_result=summary_result,
                    min_violation_level=None,
                    include_summary_messages=False,
                    include_isa_metadata_updates=True,
                    include_overrides=True,
                    delimiter="\t",
                )
                report_path.write_text(summary_report)
                logger.info(
                    "Validation report '%s' is created for study: %s.",
                    report_path,
                    resource_id,
                )
                error_count = sum(
                    1
                    for x in summary_result.messages.violations
                    if x.type in {PolicyMessageType.ERROR}
                )
                validation_result = "SUCCESS" if error_count == 0 else "ERROR"
                logger.info(
                    "Validation status for study %s: %s. Error count: %s",
                    resource_id,
                    validation_result,
                    error_count,
                )
                fw.write(
                    f"{resource_id}\t"
                    f"{release_date_str}\t"
                    f"{status.value}\t"
                    f"{validation_result}\t"
                    f"{error_count}\n"
                )
                fw.flush()
            except Exception as ex:
                # print(traceback.format_exc())
                logger.exception(ex)
                exception_message = f"{type(ex)} {str(ex)}"
                logger.error(
                    "Failed to validate study %s: %s", resource_id, exception_message
                )
                fw.write(
                    f"{resource_id}\t"
                    f"{release_date_str}\t"
                    f"{status.value}\t"
                    f"Failed to validate\t"
                    f"{exception_message}\n"
                )
                fw.flush()


async def get_input_data(
    study_model_provider: DefaultMetabolightsStudyProvider,
    phases: list[ValidationPhase],
    study_id: str,
    study_path: str,
    connection,
    model: None | MetabolightsStudyModel = None,
    assay_sheet_offset: Union[int, None] = None,
    assay_sheet_limit: Union[int, None] = None,
    assignment_sheet_offset: Union[int, None] = None,
    assignment_sheet_limit: Union[int, None] = None,
):
    provider = study_model_provider
    if not model:
        model = await provider.get_phase1_input_data(study_id, study_path, connection)

    phases.sort(key=lambda x: x.value)
    for phase in phases:
        if phase == ValidationPhase.PHASE_2:
            model = await provider.get_phase2_input_data(
                study_id,
                study_path,
                connection,
                model=model,
                assay_sheet_offset=assay_sheet_offset,
                assay_sheet_limit=assay_sheet_limit,
            )
        elif phase == ValidationPhase.PHASE_3:
            model = await provider.get_phase3_input_data(
                study_id,
                study_path,
                connection,
                model=model,
                assignment_sheet_offset=assignment_sheet_offset,
                assignment_sheet_limit=assignment_sheet_limit,
            )
        elif phase == ValidationPhase.PHASE_4:
            model = await provider.get_phase4_input_data(
                study_id, study_path, connection, model=model
            )
    return model


async def modify_model(
    resource_id: str, model: MetabolightsStudyModel, policy_service: PolicyService
) -> StudyMetadataModifierResult:
    control_lists = await policy_service.get_control_lists()
    templates = await policy_service.get_templates()
    modifier = MetabolightsStudyModelModifier(
        model=model, templates=templates, control_lists=control_lists
    )

    result = StudyMetadataModifierResult(resource_id=resource_id)

    try:
        result.logs = modifier.modify()
    except Exception as ex:
        result.logs = modifier.update_logs
        result.has_error = True
        result.error_message = str(ex)

    return result


if __name__ == "__main__":
    study_root_path = "/nfs/public/rw/metabolomics/prod/data/studies/metadata-files"
    # run_validation_cli([study_root_path])
    create_study_model_task([study_root_path, "MTBLS1", "model_MTBLS1_base.json"])
