import asyncio
import logging
from typing import Literal

import click

from mtbls.application.use_cases.indices.study_search_index.maintain import (
    maintain_study_search_index,
)
from mtbls.domain.enums.study_status import StudyStatus
from mtbls.run.cli.index.index_app import IndexApp

logger = logging.getLogger(__name__)


@click.command(name="reindex-all")
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
    "--index-update-field",
    "-i",
    default="modifiedTime",
    help="Index field to check last update time of the document.",
)
@click.option(
    "--db-update-field",
    "-d",
    default="update_date",
    help="Database field to check last update time.",
)
@click.option(
    "--temp-folder",
    "-t",
    help="temp folder.",
)
@click.argument("index_name")
@click.argument("scope")
def maintain_study_search_index_cli(
    config_file: str,
    secrets_file: str,
    index_name: str,
    index_update_field: str,
    db_update_field: str,
    temp_folder: None | str = None,
    scope: Literal["public", "completed", "all"] = "public",
):
    """Reindex public studies for UI search.

    Args:
        index_name (str): Name of target index
        scope (str): public or completed (public + private) or all

    Optional:
        config_file (str): Application config file.
            Default is mtbls-ws-config.yaml
        secrets_file (str): Application secrets file.
            Default is .secrets/ws3-secrets.yaml
        index_update_field (str): Field name to check index update time
        db_update_field (str): Database field name to check study update time
        temp_folder (None | str, optional): Template folder to process metadata files.
            Defaults to None.
    """
    try:
        index_app = IndexApp(config_file=config_file, secrets_file=secrets_file)
        index_mapping_file = (
            "resources/es/mappings/complete_study_search_index_mappings.json"
        )
        index_settings_file = (
            "resources/es/mappings/complete_study_search_index_settings.json"
        )
        if scope == "public":
            target_study_status_list = [StudyStatus.PUBLIC]
        elif scope == "completed":
            target_study_status_list = [StudyStatus.PRIVATE, StudyStatus.PUBLIC]
        elif scope == "all":
            target_study_status_list = [
                StudyStatus.PRIVATE,
                StudyStatus.PUBLIC,
                StudyStatus.PROVISIONAL,
            ]
        else:
            click.echo(
                f"Invalid scope definition: {scope}. Please select 'public' or 'complete'"
            )
            exit(1)
        asyncio.run(
            maintain_study_search_index(
                study_read_repository=index_app.study_read_repository,
                http_client=index_app.http_client,
                index_cache_files_object_repository=index_app.index_cache_files_object_repository,
                metadata_files_object_repository=index_app.metadata_files_object_repository,
                search_index_management_gateway=index_app.search_index_management_gateway,
                index_name=index_name,
                index_mapping_file=index_mapping_file,
                index_settings_file=index_settings_file,
                index_update_field=index_update_field,
                db_update_field=db_update_field,
                temp_folder=temp_folder,
                target_study_status_list=target_study_status_list,
            )
        )
    except Exception as ex:
        click.echo(f"Error {ex}")


if __name__ == "__main__":
    maintain_study_search_index_cli()
