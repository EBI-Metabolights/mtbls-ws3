import asyncio
import logging
import os
import uuid
from pathlib import Path
from typing import Union

import click

from mtbls.application.context.request_tracker import (
    RequestTrackerModel,
    get_request_tracker,
)
from mtbls.application.services.interfaces.http_client import HttpClient
from mtbls.application.services.interfaces.repositories.file_object.file_object_write_repository import (  # noqa: E501
    FileObjectWriteRepository,
)
from mtbls.application.services.interfaces.repositories.study.study_read_repository import (  # noqa: E501
    StudyReadRepository,
)
from mtbls.application.services.interfaces.search_index_management_gateway import (
    SearchIndexManagementGateway,
)
from mtbls.domain.enums.study_status import StudyStatus
from mtbls.presentation.cli.indices.public_study_search.update_json_files import (
    update_study_metadata_json_files,
)
from mtbls.presentation.cli.indices.sync.sync_search_index import (
    sync_search_index,
)
from mtbls.run.cli.es.containers import EsCliApplicationContainer
from mtbls.run.config_utils import set_application_configuration


@click.command(name="index")
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
    "--build-metadata-jsons/--no-build-metadata-jsons",
    default=True,
    show_default=True,
    help="Build cached metadata JSON files before syncing the Elasticsearch index.",
)
def run_es_cli(
    config_file: Union[None, str] = None,
    secrets_file: Union[None, str] = None,
    build_metadata_jsons: bool = True,
):
    container: EsCliApplicationContainer = EsCliApplicationContainer()

    success = set_application_configuration(container, config_file, secrets_file)
    if not success:
        click.echo("Configuration error")
        exit(1)
    container.init_resources()

    study_read_repository: StudyReadRepository = container.repositories.study_read_repository()
    http_client: HttpClient = container.gateways.http_client()
    index_cache_files_object_repository: FileObjectWriteRepository = (
        container.repositories.index_cache_files_object_repository()
    )
    metadata_files_object_repository: FileObjectWriteRepository = (
        container.repositories.metadata_files_object_repository()
    )
    search_index_manager: SearchIndexManagementGateway = container.gateways.search_index_management_gateway()
    logger = logging.getLogger(__name__)
    logger.info("CLI container started")
    request_tracker = get_request_tracker()
    request_tracker.update_request_tracker(
        RequestTrackerModel(
            user_id=0,
            route_path="cli/index-public-studies",
            resource_id="",
            client="local",
            request_id=str(uuid.uuid4()),
            task_id="",
        )
    )
    index_name = os.getenv("SEARCH_INDEX_NAME") or "public-study-search-index"
    search_visibility = (os.getenv("SEARCH_VISIBILITY") or "public").strip().lower()
    if search_visibility == "public":
        study_statuses = [StudyStatus.PUBLIC]
    elif search_visibility == "all":
        study_statuses = [StudyStatus.PRIVATE, StudyStatus.PUBLIC]
    else:
        logger.warning(
            "Invalid SEARCH_VISIBILITY=%r; defaulting to 'public'.",
            search_visibility,
        )
        study_statuses = [StudyStatus.PUBLIC]
    mappings_file_path = Path(
        # "resources/es/mappings/public_study_search_index_mapping.json"
        "resources/es/mappings/complete_study_search_index_mapping.json"
    )
    if build_metadata_jsons:
        asyncio.run(
            update_study_metadata_json_files(
                study_read_repository,
                http_client=http_client,
                index_cache_files_object_repository=index_cache_files_object_repository,
                metadata_files_object_repository=metadata_files_object_repository,
                reindex_all=True,
            )
        )
    asyncio.run(
        sync_search_index(
            index_name=index_name,
            index_management_gateway=search_index_manager,
            study_read_repository=study_read_repository,
            index_cache_files_object_repository=index_cache_files_object_repository,
            recreate_index=True,
            mappings_file_path=mappings_file_path,
            study_statuses=study_statuses,
        )
    )


if __name__ == "__main__":
    run_es_cli()
