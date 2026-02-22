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

    # logger.info("CLI container started")
    # request_tracker = get_request_tracker()
    # request_tracker.update_request_tracker(
    #     RequestTrackerModel(
    #         user_id=0,
    #         route_path="cli/index-public-studies",
    #         resource_id="",
    #         client="local",
    #         request_id=str(uuid.uuid4()),
    #         task_id="",
    #     )
    # )
    # if subcommand == "update-metadata-json-files":
    #     asyncio.run(
    #         update_study_metadata_json_files(
    #             study_read_repository,
    #             http_client=http_client,
    #             index_cache_files_object_repository=index_cache_files_object_repository,
    #             metadata_files_object_repository=metadata_files_object_repository,
    #             reindex_all=True,
    #         )
    #     )
    # elif subcommand == "maintain-indices":
    #     data_index_configuration = DataIndexConfiguration()
    #     asyncio.run(
    #         maintain_indices(
    #             data_index_client=data_index_client,
    #             study_metadata_service_factory=study_metadata_service_factory,
    #             study_read_repository=study_read_repository,
    #             data_index_configuration=data_index_configuration,
    #         )
    #     )
    # elif subcommand == "reindex-selected-studies":
    #     data_index_configuration = DataIndexConfiguration()
    #     # selected_studies = [f"MTBLS{x}" for x in range(1, 1000)]

    #     selected_studies = [
    #         "MTBLS134",
    #         "MTBLS378",
    #         "MTBLS519",
    #         "MTBLS553",
    #         "MTBLS640",
    #         "MTBLS700",
    #         "MTBLS719",
    #     ]
    #     asyncio.run(
    #         reindex_selected_studies(
    #             selected_studies=selected_studies,
    #             data_index_client=data_index_client,
    #             study_metadata_service_factory=study_metadata_service_factory,
    #             study_read_repository=study_read_repository,
    #             data_index_configuration=data_index_configuration,
    #         )
    #     )


if __name__ == "__main__":
    maintain_cli()
