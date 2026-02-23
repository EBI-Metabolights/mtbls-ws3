import asyncio
from pathlib import Path
from typing import Union

import click

from mtbls.application.services.interfaces.study_metadata_service_factory import (
    StudyMetadataServiceFactory,
)
from mtbls.run.cli.validation.validation_app import ValidationApp


@click.command(name="create-input-json")
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
    "--target_path",
    "-o",
    default="",
    help="output file path. Default is ./.temp-output/<resource_id>.json",
)
@click.argument("resource_id")
def create_input_json_cli(
    resource_id: str,
    target_path: Union[None, str] = None,
    config_file: Union[None, str] = None,
    secrets_file: Union[None, str] = None,
):
    app = ValidationApp(config_file=config_file, secrets_file=secrets_file)
    if not resource_id:
        click.echo("Resource Id is not valid")
        exit(1)
    if not target_path:
        target = Path(f"./.temp-output/{resource_id}.json")
    else:
        target = Path(target_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        asyncio.run(
            create_input_json(
                resource_id=resource_id,
                target_path=target,
                study_metadata_service_factory=app.study_metadata_service_factory,
            )
        )
        click.echo(f"'{target}' validation input file is created.")
    except Exception as ex:
        click.echo(f"Error: {ex}")
        exit(1)


async def create_input_json(
    resource_id: str,
    target_path: Path,
    study_metadata_service_factory: StudyMetadataServiceFactory,
):
    service = await study_metadata_service_factory.create_service(resource_id)
    with service:
        model = await service.load_study_model(
            load_sample_file=True,
            load_assay_files=True,
            load_maf_files=True,
            load_folder_metadata=True,
            load_db_metadata=True,
        )

    with target_path.open("w") as f:
        f.write(model.model_dump_json(indent=4, by_alias=True))


if __name__ == "__main__":
    create_input_json_cli()
