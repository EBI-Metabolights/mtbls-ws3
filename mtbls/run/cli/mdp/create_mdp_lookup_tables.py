import asyncio
import logging
from pathlib import Path
from typing import Union

import click

from mtbls.application.use_cases.mdp.create_mdp_lookup_tables import (
    MtblsLookupTableCreator,
)
from mtbls.run.cli.validation.validation_app import ValidationApp

logger = logging.getLogger(__name__)


@click.command(name="mdp-lookup-tables")
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
    help="File lists the selected study ids.",
)
@click.option(
    "--output-folder-path",
    "-o",
    default=".temp-mdp-lookup-tables",
    help="Output folder path. ",
)
def create_mdp_lookup_tables(
    output_folder_path: Union[None, str] = None,
    config_file: Union[None, str] = None,
    secrets_file: Union[None, str] = None,
    selected_studies_file: Union[None, str] = None,
):
    """Create MDP lookup tables
    for selected studies or all studies if no selection is provided.
    """
    if not selected_studies_file:
        selected_studies = None
    else:
        with Path(selected_studies_file).open("r") as f:
            selected_studies = [line.strip() for line in f if line.strip()]
    app = ValidationApp(config_file=config_file, secrets_file=secrets_file)

    creator = MtblsLookupTableCreator(
        study_metadata_service_factory=app.study_metadata_service_factory,
        study_read_repository=app.study_read_repository,
        selected_studies=selected_studies,
        output_folder_path=output_folder_path,
        column_model_mapping_file_path=None,
    )
    asyncio.run(creator.create_mtbls_lcms_lookup_tables())


if __name__ == "__main__":
    create_mdp_lookup_tables()
