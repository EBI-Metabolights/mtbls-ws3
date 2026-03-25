import asyncio
import logging
from typing import Literal

import click

from mtbls.application.use_cases.indices.kibana_indices.index_manager import (
    reindex_all,
)
from mtbls.run.cli.index.index_app import IndexApp
from mtbls.run.cli.index.kibana_indices.utils import get_data_index_configuration

logger = logging.getLogger(__name__)


@click.command(name="reindex-selected-studies")
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
    "--exclude-studies",
    "-e",
    default=".secrets/ws3-secrets.yaml",
    help="config secrets file path.",
)
@click.option(
    "--test",
    is_flag=True,
    default=False,
    help="use test indices.",
)
@click.argument("scope")
def reindex_all_cli(
    scope: Literal["public", "completed", "all"],
    config_file: str,
    secrets_file: str,
    exclude_studies: str,
    test: bool = False,
):
    """Reindex all studies. Master is MetaboliLights database.
       All indices will be deleted and re-created.
       All studies will be indexed again.

    Args:
        config_file (str): Application config file.
            Default is mtbls-ws-config.yaml
        secrets_file (str): Application secrets file.
            Default is .secrets/ws3-secrets.yaml
        scope (str): public or completed (public + private) or all
    """
    if not exclude_studies:
        exclude_studies = ""
    exclude_list = [x.strip() for x in exclude_studies.split(",") if x and x.strip()]
    try:
        index_app = IndexApp(config_file=config_file, secrets_file=secrets_file)
        data_index_configuration = get_data_index_configuration(scope, test)
        asyncio.run(
            reindex_all(
                search_index_management_gateway=index_app.search_index_management_gateway,
                study_metadata_service_factory=index_app.study_metadata_service_factory,
                study_read_repository=index_app.study_read_repository,
                data_index_configuration=data_index_configuration,
                exclude_studies=exclude_list,
            )
        )
    except Exception as ex:
        click.echo(f"Error {ex}")


if __name__ == "__main__":
    reindex_all_cli()
