import asyncio
import datetime
import logging
from pathlib import Path
from typing import Union

import click

from mtbls.application.utils.sort_utils import sort_by_study_id
from mtbls.domain.enums.filter_operand import FilterOperand
from mtbls.domain.enums.study_status import StudyStatus
from mtbls.domain.shared.repository.entity_filter import EntityFilter
from mtbls.domain.shared.repository.query_options import QueryFieldOptions
from mtbls.domain.shared.validator.policy import (
    PolicySummaryResult,
)
from mtbls.run.cli.validation.validate import run_validation_and_save_report
from mtbls.run.cli.validation.validation_app import ValidationApp

logger = logging.getLogger(__name__)


@click.command(name="validate")
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
    "--selected-studies-file",
    "-f",
    default=".temp-validations",
    help="Validation report root path. ",
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
def check_release_date_in_past_studies_cli(
    apply_modifiers: bool = False,
    validation_reports_root_path: Union[None, str] = None,
    summary_file: Union[None, str] = None,
    config_file: Union[None, str] = None,
    secrets_file: Union[None, str] = None,
    selected_studies_file: Union[None, str] = None,
):
    reports_path = Path(validation_reports_root_path)
    reports_path.mkdir(parents=True, exist_ok=True)
    summary_file_path = Path(summary_file) if summary_file else None
    if summary_file_path:
        summary_file_path.parent.mkdir(parents=True, exist_ok=True)

    app = ValidationApp(config_file=config_file, secrets_file=secrets_file)

    asyncio.run(
        check_release_date_in_past_studies(
            validation_app=app,
            validation_reports_root_path=reports_path,
            summary_file=Path(summary_file) if summary_file else None,
            apply_modifiers=apply_modifiers,
            selected_studies_file=selected_studies_file,
        )
    )


async def check_release_date_in_past_studies(
    validation_app: ValidationApp,
    validation_reports_root_path: Path,
    summary_file: None | Path,
    apply_modifiers: bool = False,
    selected_studies_file: None | str = None,
) -> PolicySummaryResult:
    if not summary_file:
        summary_file = "summary_file.txt"
    summary_file_path = validation_reports_root_path / Path(summary_file)

    if selected_studies_file:
        selected_studies_file_path = Path(selected_studies_file)
        if not selected_studies_file_path.exists():
            click(f"Selected studies file '{selected_studies_file}' not found")
            exit(1)
        resource_ids = [
            x.strip()
            for x in Path(selected_studies_file).readtext().split("\n")
            if x and x.strip()
        ]
    else:
        now = datetime.datetime.now()

        result = await validation_app.study_read_repository.select_fields(
            query_field_options=QueryFieldOptions(
                filters=[
                    EntityFilter(key="status", value=StudyStatus.PRIVATE),
                    EntityFilter(
                        key="release_date", operand=FilterOperand.LE, value=now
                    ),
                ],
                selected_fields=["accession_number", "release_date", "update_date"],
            )
        )
        resources_map = {x[0]: x for x in result.data}
        resource_ids: list[str] = list(resources_map.keys())
        resource_ids.sort(key=sort_by_study_id, reverse=True)
        resource_ids = [resource_ids[0]]
    if not resource_ids:
        click("There is no study to run validation")
        exit(0)

    await run_validation_and_save_report(
        validation_app=validation_app,
        resource_ids=resource_ids,
        validation_reports_root_path=validation_reports_root_path,
        summary_file=summary_file_path,
        apply_modifiers=apply_modifiers,
    )

    if not summary_file_path.exists():
        click("Result not found")
        exit(1)

    # lines = summary_file_path.read_text().splitlines()
    # for idx, line in enumerate(lines):
    #     if idx == 0 or not line or line.strip():
    #         continue
    #     row = line.split("\t")
    #     if row[2].lower() == "success":


if __name__ == "__main__":
    check_release_date_in_past_studies_cli(["--apply-modifiers"])
