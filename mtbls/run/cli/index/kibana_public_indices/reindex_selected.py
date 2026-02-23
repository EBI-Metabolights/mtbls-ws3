import asyncio
import logging

import click

from mtbls.application.use_cases.indices.kibana_indices.index_manager import (
    DataIndexConfiguration,
    reindex_selected_studies,
)
from mtbls.run.cli.index.index_app import IndexApp

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
    default=".mtbls-ws-config-secrets/.secrets.yaml",
    help="config secrets file path.",
)
@click.argument("selected_studies")
def reindex_selected_cli(selected_studies: str, config_file: str, secrets_file: str):
    """Update selected studies in target indices.
       Selected study indices will be deleted and reindexed.

    Args:
        config_file (str): Application config file.
            Default is mtbls-ws-config.yaml
        secrets_file (str): Application secrets file.
            Default is .mtbls-ws-config-secrets/.secrets.yaml
        selected_studies (str): Comma seperated study ids.
    """
    try:
        index_app = IndexApp(config_file=config_file, secrets_file=secrets_file)
        data_index_configuration = DataIndexConfiguration()
        if not selected_studies:
            click.echo("Select at least one study.")
            exit(1)
        selected_studies = [
            x.strip() for x in selected_studies.split(",") if x and x.strip()
        ]
        click.echo(f"Selected studies: {selected_studies}")
        asyncio.run(
            reindex_selected_studies(
                selected_studies=selected_studies,
                data_index_client=index_app.data_index_client,
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
