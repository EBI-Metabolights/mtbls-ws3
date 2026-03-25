import asyncio
import logging

import click

from mtbls.application.use_cases.indices.study_search_index.update_json_files import (
    update_study_metadata_json_files,
)
from mtbls.domain.enums.study_status import StudyStatus
from mtbls.run.cli.index.index_app import IndexApp

logger = logging.getLogger(__name__)


@click.command(name="build-metadata-json-files")
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
def build_metadata_json_files(config_file: str, secrets_file: str):
    """Create json files from study metadata files.

    Args:
        config_file (str): Application config file.
            Default is mtbls-ws-config.yaml
        secrets_file (str): Application secrets file.
            Default is .secrets/ws3-secrets.yaml
    """
    try:
        index_app = IndexApp(config_file=config_file, secrets_file=secrets_file)
        asyncio.run(
            update_study_metadata_json_files(
                study_read_repository=index_app.study_read_repository,
                index_cache_files_object_repository=index_app.index_cache_files_object_repository,
                metadata_files_object_repository=index_app.metadata_files_object_repository,
                http_client=index_app.http_client,
                target_study_status_list=[StudyStatus.PUBLIC, StudyStatus.PRIVATE],
            )
        )
    except Exception as ex:
        click.echo(f"Error {ex}")


if __name__ == "__main__":
    build_metadata_json_files()
