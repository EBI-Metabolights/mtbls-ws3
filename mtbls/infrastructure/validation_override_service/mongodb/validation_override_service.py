import datetime
import logging
from pathlib import Path
from typing import Union

from mtbls.application.services.interfaces.policy_service import PolicyService
from mtbls.application.services.interfaces.repositories.file_object.validation.validation_override_repository import (  # noqa: E501
    ValidationOverrideRepository,
)
from mtbls.application.services.interfaces.validation_override_service import (
    ValidationOverrideService,
)
from mtbls.domain.entities.study_file import ResourceCategory, StudyFileOutput
from mtbls.domain.entities.validation.validation_override import (
    ValidationOverrideFileObject,
    ValidationOverrideList,
)
from mtbls.domain.shared.data_types import UtcDatetime
from mtbls.domain.shared.repository.entity_filter import EntityFilter
from mtbls.domain.shared.repository.query_options import QueryOptions
from mtbls.domain.shared.validator.validation import (
    VersionedValidationsMap,
)

logger = logging.getLogger(__name__)


class MongoDbValidationOverrideService(ValidationOverrideService):
    def __init__(
        self,
        validation_override_repository: ValidationOverrideRepository,
        policy_service: PolicyService,
        validation_overrides_object_key: str = "validation-overrides/validation-overrides.json",  # noqa: E501
    ):
        self.validation_override_repository = validation_override_repository
        self.policy_service = policy_service
        self.study_bucket = self.validation_override_repository.study_bucket

        self.validation_overrides_object_key = (
            validation_overrides_object_key
            if validation_overrides_object_key
            else "validation-overrides/validation-overrides.json"
        )
        self.validation_overrides_path = Path(validation_overrides_object_key)
        self.suffix = self.validation_overrides_path.suffix
        self.parent_object_key = str(Path(validation_overrides_object_key).parent)
        if self.parent_object_key == ".":
            self.parent_object_key = ""

    async def get_validation_definitions(self) -> VersionedValidationsMap:
        return await self.policy_service.get_rule_definitions()

    async def _initiate_validation_override(
        self, resource_id: str
    ) -> Union[None, ValidationOverrideFileObject]:
        filters = [
            EntityFilter(key="resourceId", value=resource_id),
        ]
        files = await self.validation_override_repository.find(
            query_options=QueryOptions(filters=filters)
        )

        overrides_model = None
        if files and files.data:
            overrides_model = ValidationOverrideFileObject.model_validate(files.data[0])

        if not overrides_model:
            version = ""
            definitions = await self.get_validation_definitions()
            version = definitions.validation_version
            now = datetime.datetime.now(datetime.timezone.utc)
            overrides = ValidationOverrideFileObject(
                bucket_name=self.study_bucket.value,
                resource_id=resource_id,
                numeric_resource_id=int(
                    resource_id.removeprefix("REQ").removeprefix("MTBLS")
                ),
                object_key=self.validation_overrides_object_key,
                created_at=now,
                data=ValidationOverrideList(resource_id=resource_id, version=version),
            )
            overrides = await self.validation_override_repository.create(overrides)
            overrides_model = ValidationOverrideFileObject.model_validate(overrides)
            await self.validation_override_repository.object_created(
                self.get_study_object(
                    id_=overrides.id_, resource_id=resource_id, created_at=now
                )
            )
        return overrides_model

    async def get_validation_overrides(
        self, resource_id: str
    ) -> ValidationOverrideList:
        overrides = await self._initiate_validation_override(resource_id=resource_id)
        return overrides.data

    async def save_validation_overrides(
        self, resource_id: str, validation_overrides: ValidationOverrideList
    ) -> bool:
        overrides = await self._initiate_validation_override(resource_id=resource_id)
        overrides.data = validation_overrides
        now = datetime.datetime.now(datetime.timezone.utc)
        overrides.updated_at = now
        updated_object = await self.validation_override_repository.update(overrides)
        await self.validation_override_repository.object_updated(
            self.get_study_object(
                id_=updated_object.id_, resource_id=resource_id, updated_at=now
            )
        )
        return updated_object is not None

    def get_study_object(
        self,
        id_: str,
        resource_id: str,
        created_at: Union[None, datetime.datetime] = None,
        updated_at: Union[None, datetime.datetime] = None,
        tags: dict[str, Union[None, str, int, float, bool, UtcDatetime]] = None,
    ) -> StudyFileOutput:
        return StudyFileOutput(
            id_=id_,
            bucket_name=self.study_bucket.value,
            object_key=self.validation_overrides_object_key,
            parent_object_key=self.parent_object_key,
            resource_id=resource_id,
            basename=self.validation_overrides_path.name,
            numeric_resource_id=int(
                resource_id.removeprefix("REQ").removeprefix("MTBLS")
            ),
            extension=self.validation_overrides_path.suffix,
            category=ResourceCategory.INTERNAL_RESOURCE,
            created_at=created_at,
            updated_at=updated_at,
            tags=tags if tags else {},
        )
