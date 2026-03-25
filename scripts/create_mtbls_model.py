import asyncio
import json
import pathlib

from mtbls.run.cli.validation.create_input_json import create_input_json
from mtbls.run.cli.validation.validation_app import ValidationApp
from mtbls.run.config_utils import get_application_config_files

if __name__ == "__main__":
    output_folder_path = ".temp-test-outputs"
    study_id = "MTBLS1"
    pathlib.Path(output_folder_path).mkdir(parents=True, exist_ok=True)
    target_path = pathlib.Path(output_folder_path) / pathlib.Path(f"{study_id}.json")
    config_file, secrets_file = get_application_config_files()
    app = ValidationApp(config_file=config_file, secrets_file=secrets_file)
    model = asyncio.run(
        create_input_json(
            study_id,
            target_path,
            study_metadata_service_factory=app.study_metadata_service_factory,
        )
    )
    with target_path.open("w") as f:
        json.dump(model.model_dump(by_alias=True), f, indent=4)
