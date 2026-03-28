import asyncio
import logging
from pathlib import Path
from typing import Union

import click

from mtbls.application.remote_tasks.common.run_validation import (
    create_validation_run_configuration,
    run_validation_task,
    run_validation_task_with_modifiers,
)
from mtbls.application.use_cases.validation.validation_task import (
    convert_to_summary_result,
    get_report_content_from_summary_report,
)
from mtbls.domain.shared.validator.policy import (
    PolicySummaryResult,
)
from mtbls.domain.shared.validator.types import PolicyMessageType
from mtbls.run.cli.validation.validation_app import ValidationApp

logger = logging.getLogger(__name__)


@click.command(no_args_is_help=True, name="validate")
@click.option(
    "--config-file",
    "-c",
    default="mtbls-ws-config.yaml",
    help="Local config path.",
)
@click.option(
    "--secrets-file",
    "-s",
    default=".secrets/ws3-secrets.yaml",
    help="config secrets file path.",
)
@click.option(
    "--validation-reports-root-path",
    "-o",
    default=".temp-validations",
    help="Validation report root path. ",
)
@click.option(
    "--apply-modifiers",
    is_flag=True,
    default=False,
    help="Apply modifiers before validation.",
)
@click.option(
    "--summary-file",
    help="Validation results' summary file.",
)
@click.option("--selected-studies", help="Comma seperated study ids.", default="")
@click.option(
    "--input-file", help="Input file contains study ids in lines.", default=""
)
def run_validation_cli(
    selected_studies: str,
    input_file: str,
    apply_modifiers: bool = False,
    validation_reports_root_path: Union[None, str] = None,
    summary_file: Union[None, str] = None,
    config_file: Union[None, str] = None,
    secrets_file: Union[None, str] = None,
):
    if not selected_studies and not input_file:
        click.echo("Select --selected-studies or --input-file option.")
    if selected_studies and input_file:
        click.echo("Select only --selected-studies or --input-file option.")
    if selected_studies:
        resource_ids = [
            x.strip() for x in selected_studies.split(",") if x and x.strip()
        ]
    else:
        resource_ids = [
            x.strip()
            for x in Path(input_file).read_text().split("\n")
            if x and x.strip()
        ]

    reports_path = Path(validation_reports_root_path)
    reports_path.mkdir(parents=True, exist_ok=True)
    summary_file_path = Path(summary_file) if summary_file else None
    if summary_file_path:
        summary_file_path.parent.mkdir(parents=True, exist_ok=True)

    app = ValidationApp(config_file=config_file, secrets_file=secrets_file)

    asyncio.run(
        run_validation_and_save_report(
            validation_app=app,
            resource_ids=resource_ids,
            validation_reports_root_path=reports_path,
            summary_file=Path(summary_file) if summary_file else None,
            apply_modifiers=apply_modifiers,
        )
    )


async def run_validation_and_save_report(
    validation_app: ValidationApp,
    resource_ids: list[str],
    validation_reports_root_path: Path,
    summary_file: None | Path,
    apply_modifiers: bool = False,
) -> PolicySummaryResult:
    fw = summary_file.open("w") if summary_file else None
    try:
        if fw:
            fw.write("STUDY_ID\tCREATED_AT\tRELEASE_DATE\tSTATUS\tRESULT\tERROR\n")
        for resource_id in resource_ids:
            report_path = validation_reports_root_path / Path(
                f"{resource_id}_validation.tsv"
            )
            try:
                study = (
                    await validation_app.study_read_repository.get_study_by_accession(
                        resource_id
                    )
                )
                release_date_str = study.release_date.strftime("%Y-%m-%d")
                created_at_str = study.created_at.strftime("%Y-%m-%d")
                config = await create_validation_run_configuration(
                    resource_id=resource_id,
                    temp_folder=None,
                    apply_modifiers=apply_modifiers,
                    metadata_files_object_repository=validation_app.metadata_files_object_repository,
                    mhd_config=validation_app.mhd_config,
                    private_metadata_files_root_path=validation_app.private_metadata_files_root_path,
                    db_connection=validation_app.db_connection,
                )
                if apply_modifiers:
                    result_list = await run_validation_task_with_modifiers(
                        resource_id,
                        study_metadata_service_factory=validation_app.study_metadata_service_factory,
                        internal_files_object_repository=validation_app.internal_files_object_repository,
                        policy_service=validation_app.policy_service,
                        serialize_result=False,
                        ontology_search_service=validation_app.ontology_search_service,
                        validation_run_configuration=config,
                    )
                else:
                    result_list = await run_validation_task(
                        resource_id,
                        modifier_result=None,
                        study_metadata_service_factory=validation_app.study_metadata_service_factory,
                        internal_files_object_repository=validation_app.internal_files_object_repository,
                        policy_service=validation_app.policy_service,
                        serialize_result=False,
                        ontology_search_service=validation_app.ontology_search_service,
                        validation_run_configuration=config,
                    )
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
                if fw:
                    fw.write(
                        f"{resource_id}\t"
                        f"{created_at_str}\t"
                        f"{release_date_str}\t"
                        f"{study.status.name}\t"
                        f"{validation_result}\t"
                        f"{error_count}\n"
                    )
                    fw.flush()

            except Exception as ex:
                if fw:
                    fw.write(
                        f"{resource_id}\t"
                        f"{created_at_str}\t"
                        f"{release_date_str}\t"
                        f"{study.status.name}\t"
                        f"Failed to validate\t"
                        f"{ex}\n"
                    )
                    fw.flush()
    finally:
        if fw:
            fw.close()


if __name__ == "__main__":
    run_validation_cli(
        [
            "--input-file",
            ".temp-validations/validated_studies.txt",
            "--summary-file",
            ".temp-validations/summary.txt",
            "--apply-modifiers",
        ]
    )
