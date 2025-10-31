import json
import logging
import pathlib
import uuid
from typing import Union

from mtbls.application.services.interfaces.policy_service import PolicyService
from mtbls.application.services.interfaces.repositories.file_object.file_object_write_repository import (  # noqa: E501
    FileObjectWriteRepository,
)
from mtbls.application.services.interfaces.validation_override_service import (
    ValidationOverrideService,
)
from mtbls.domain.entities.study_file import ResourceCategory
from mtbls.domain.entities.validation.validation_override import ValidationOverrideList
from mtbls.domain.shared.validator.validation import (
    VersionedValidationsMap,
)

logger = logging.getLogger(__name__)


class FileSystemValidationOverrideService(ValidationOverrideService):
    def __init__(
        self,
        policy_service: PolicyService,
        file_object_repository: FileObjectWriteRepository,
        validation_overrides_object_key: str = "validation-overrides/validation-overrides.json",  # noqa: E501
        temp_directory: str = "/tmp/validation-overrides-tmp",
    ):
        super().__init__()
        self.file_object_repository = file_object_repository
        self.policy_service = policy_service
        self.study_bucket = file_object_repository.get_bucket()
        self.temp_directory = (
            pathlib.Path(temp_directory)
            if temp_directory
            else pathlib.Path("/tmp/validation-overrides-tmp")
        )
        self.temp_directory.mkdir(parents=True, exist_ok=True)
        self.validation_overrides_object_key = (
            validation_overrides_object_key
            if validation_overrides_object_key
            else "validation-overrides/validation-overrides.json"
        )
        self.validation_overrides_parent_object_key = "validation-overrides"

    async def get_validation_definitions(self) -> VersionedValidationsMap:
        return await self.policy_service.get_rule_definitions()

    async def _initiate_validation_override(
        self, resource_id: str
    ) -> Union[None, ValidationOverrideList]:
        if not await self.file_object_repository.exists(
            resource_id=resource_id,
            object_key=self.validation_overrides_parent_object_key,
        ):
            await self.file_object_repository.create_folder_object(
                resource_id=resource_id,
                object_key=self.validation_overrides_parent_object_key,
            )
            study_object = await self.file_object_repository.get_info(
                resource_id=resource_id,
                object_key=self.validation_overrides_parent_object_key,
            )
            await self.file_object_repository.object_created(study_object=study_object)

        json_exists = await self.file_object_repository.exists(
            resource_id=resource_id,
            object_key=self.validation_overrides_object_key,
        )
        if not json_exists:
            version = ""
            definitions = await self.get_validation_definitions()
            version = definitions.validation_version
            overrides = ValidationOverrideList(resource_id=resource_id, version=version)
            return overrides
        return None

    async def get_validation_overrides(
        self, resource_id: str
    ) -> ValidationOverrideList:
        temp_filename = pathlib.Path(f"{str(uuid.uuid4())}.json")
        tmp_file_path = self.temp_directory / temp_filename
        initiated_overrides = await self._initiate_validation_override(
            resource_id=resource_id
        )
        if initiated_overrides:
            await self.save_validation_overrides(
                resource_id=resource_id, validation_overrides=initiated_overrides
            )
            study_object = await self.file_object_repository.get_info(
                resource_id=resource_id, object_key=self.validation_overrides_object_key
            )
            study_object.category = ResourceCategory.INTERNAL_RESOURCE
            await self.file_object_repository.object_created(study_object=study_object)
            return initiated_overrides
        try:
            await self.file_object_repository.download(
                resource_id=resource_id,
                object_key=self.validation_overrides_object_key,
                target_path=str(tmp_file_path),
            )

            with tmp_file_path.open() as f:
                validations_obj = json.load(f)
            return ValidationOverrideList.model_validate(validations_obj)
        finally:
            tmp_file_path.unlink(missing_ok=True)

    async def save_validation_overrides(
        self, resource_id: str, validation_overrides: ValidationOverrideList
    ) -> bool:
        temp_filename = pathlib.Path(f"{str(uuid.uuid4())}.json")
        tmp_file_path = self.temp_directory / temp_filename
        source_uri = f"file://{str(tmp_file_path)}"
        try:
            with tmp_file_path.open("w") as f:
                f.write(validation_overrides.model_dump_json(indent=4))

            await self._initiate_validation_override(resource_id=resource_id)
            await self.file_object_repository.put_object(
                resource_id=resource_id,
                object_key=self.validation_overrides_object_key,
                source_uri=source_uri,
            )
            study_object = await self.file_object_repository.get_info(
                resource_id=resource_id, object_key=self.validation_overrides_object_key
            )
            study_object.category = ResourceCategory.INTERNAL_RESOURCE
            await self.file_object_repository.object_updated(study_object=study_object)
        finally:
            tmp_file_path.unlink(missing_ok=True)
