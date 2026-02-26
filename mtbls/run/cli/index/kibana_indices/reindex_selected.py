import asyncio
import logging
from typing import Literal

import click

from mtbls.application.use_cases.indices.kibana_indices.index_manager import (
    reindex_selected_studies,
)
from mtbls.run.cli.index.index_app import IndexApp
from mtbls.run.cli.index.kibana_indices.utils import get_data_index_configuration

logger = logging.getLogger(__name__)


@click.command(name="reindex-selected")
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
    "--test",
    is_flag=True,
    default=False,
    help="use test indices.",
)
@click.argument("selected_studies")
@click.argument("scope")
def reindex_selected_cli(
    selected_studies: str,
    scope: Literal["public", "completed", "all"],
    config_file: str,
    secrets_file: str,
    test: bool = True,
):
    """Update selected studies in target indices.
       Selected study indices will be deleted and reindexed.

    Args:
        config_file (str): Application config file.
            Default is mtbls-ws-config.yaml
        secrets_file (str): Application secrets file.
            Default is .secrets/ws3-secrets.yaml
        selected_studies (str): Comma seperated study ids.
        scope (str): public or completed (public + private) or all
    """
    try:
        if not selected_studies:
            click.echo("Select at least one study.")
            exit(1)
        index_app = IndexApp(config_file=config_file, secrets_file=secrets_file)
        data_index_configuration = get_data_index_configuration(scope, test)

        selected_studies = [
            x.strip() for x in selected_studies.split(",") if x and x.strip()
        ]
        click.echo(f"Selected studies: {selected_studies}")
        asyncio.run(
            reindex_selected_studies(
                selected_studies=selected_studies,
                search_index_management_gateway=index_app.search_index_management_gateway,
                study_metadata_service_factory=index_app.study_metadata_service_factory,
                study_read_repository=index_app.study_read_repository,
                data_index_configuration=data_index_configuration,
            )
        )
    except Exception as ex:
        click.echo(f"Error {ex}")


if __name__ == "__main__":
    # selected_studies = [
    #     "MTBLS134",
    #     "MTBLS378",
    #     "MTBLS519",
    #     "MTBLS553",
    #     "MTBLS640",
    #     "MTBLS700",
    #     "MTBLS719",
    # ]
    reindex_selected_cli(["MTBLS1,MTBLS2"])
