import asyncio
import logging
import uuid
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
from mtbls.presentation.cli.indices.public_study_search.update_json_files import (
    update_study_metadata_json_files,
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
    default=".mtbls-ws-config-secrets/.secrets.yaml",
    help="config secrets file path.",
)
def run_es_cli(
    config_file: Union[None, str] = None,
    secrets_file: Union[None, str] = None,
):
    container: EsCliApplicationContainer = EsCliApplicationContainer()

    success = set_application_configuration(container, config_file, secrets_file)
    if not success:
        click.echo("Configuration error")
        exit(1)
    container.init_resources()

    study_read_repository: StudyReadRepository = (
        container.repositories.study_read_repository()
    )
    http_client: HttpClient = container.gateways.http_client()
    index_cache_files_object_repository: FileObjectWriteRepository = (
        container.repositories.index_cache_files_object_repository()
    )
    metadata_files_object_repository: FileObjectWriteRepository = (
        container.repositories.metadata_files_object_repository()
    )

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

    asyncio.run(
        update_study_metadata_json_files(
            study_read_repository,
            http_client=http_client,
            index_cache_files_object_repository=index_cache_files_object_repository,
            metadata_files_object_repository=metadata_files_object_repository,
            reindex_all=True,
        )
    )


if __name__ == "__main__":
    run_es_cli()
