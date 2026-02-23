import asyncio
import logging

import click

from mtbls.application.use_cases.indices.kibana_indices.index_manager import (
    DataIndexConfiguration,
    maintain_indices,
)
from mtbls.run.cli.index.index_app import IndexApp

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
    default=".mtbls-ws-config-secrets/.secrets.yaml",
    help="config secrets file path.",
)
def maintain_cli(config_file: str, secrets_file: str):
    """Update all studies in target indices. Master is MetaboliLights database.
       If a study is deleted from database, index will be deleted.
       If there are new and updated studies in database, they will be reindexed

    Args:
        config_file (str): Application config file.
            Default is mtbls-ws-config.yaml
        secrets_file (str): Application secrets file.
            Default is .mtbls-ws-config-secrets/.secrets.yaml
    """
    try:
        index_app = IndexApp(config_file=config_file, secrets_file=secrets_file)
        data_index_configuration = DataIndexConfiguration()
        asyncio.run(
            maintain_indices(
                data_index_client=index_app.data_index_client,
                study_metadata_service_factory=index_app.study_metadata_service_factory,
                study_read_repository=index_app.study_read_repository,
                data_index_configuration=data_index_configuration,
            )
        )

    except Exception as ex:
        click.echo(f"Error {ex}")


if __name__ == "__main__":
    maintain_cli()
