import asyncio
import logging

import click

from mtbls.application.use_cases.indices.kibana_indices.index_manager import (
    DataIndexConfiguration,
    reindex_all,
)
from mtbls.run.cli.index.index_app import IndexApp

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
    default=".mtbls-ws-config-secrets/.secrets.yaml",
    help="config secrets file path.",
)
@click.option(
    "--exclude-studies",
    "-e",
    default=".mtbls-ws-config-secrets/.secrets.yaml",
    help="config secrets file path.",
)
def reindex_all_cli(config_file: str, secrets_file: str, exclude_studies: str):
    """Reindex all studies. Master is MetaboliLights database.
       All indices will be deleted and re-created.
       All studies will be indexed again.

    Args:
        config_file (str): Application config file.
            Default is mtbls-ws-config.yaml
        secrets_file (str): Application secrets file.
            Default is .mtbls-ws-config-secrets/.secrets.yaml
    """
    if not exclude_studies:
        exclude_studies = ""
    exclude_list = [x.strip() for x in exclude_studies.split(",") if x and x.strip()]
    try:
        index_app = IndexApp(config_file=config_file, secrets_file=secrets_file)
        data_index_configuration = DataIndexConfiguration()
        asyncio.run(
            reindex_all(
                data_index_client=index_app.data_index_client,
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
