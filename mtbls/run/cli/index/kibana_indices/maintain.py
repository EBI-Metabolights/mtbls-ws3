import asyncio
import logging
from typing import Literal

import click

from mtbls.application.use_cases.indices.kibana_indices.index_manager import (
    maintain_indices,
)
from mtbls.run.cli.index.index_app import IndexApp
from mtbls.run.cli.index.kibana_indices.utils import get_data_index_configuration

logger = logging.getLogger(__name__)


@click.command(name="maintain")
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
@click.argument("scope")
def maintain_cli(
    config_file: str,
    secrets_file: str,
    scope: Literal["public", "completed", "all"],
    test: bool,
):
    """Update all studies in target indices. Master is MetaboliLights database.
       If a study is deleted from database, index will be deleted.
       If there are new and updated studies in database, they will be reindexed

    Args:
        config_file (str): Application config file.
            Default is mtbls-ws-config.yaml
        secrets_file (str): Application secrets file.
            Default is .secrets/ws3-secrets.yaml
        scope (str): public or completed (public + private) or all
    """
    try:
        index_app = IndexApp(config_file=config_file, secrets_file=secrets_file)
        data_index_configuration = get_data_index_configuration(scope, test)
        asyncio.run(
            maintain_indices(
                search_index_management_gateway=index_app.search_index_management_gateway,
                study_metadata_service_factory=index_app.study_metadata_service_factory,
                study_read_repository=index_app.study_read_repository,
                data_index_configuration=data_index_configuration,
            )
        )

    except Exception as ex:
        click.echo(f"Error {ex}")


if __name__ == "__main__":
    maintain_cli()
