import datetime
import json
import logging
import pathlib
from typing import Any, Union
from urllib.parse import quote

from mtbls.application.services.interfaces.repositories.file_object.file_object_observer import (
    FileObjectObserver,
)
from mtbls.application.services.interfaces.repositories.file_object.file_object_read_repository import (
    FileObjectReadRepository,
)
from mtbls.application.services.interfaces.repositories.file_object.file_object_write_repository import (
    FileObjectWriteRepository,
)
from mtbls.domain.entities.base_entity import BaseEntity
from mtbls.domain.entities.base_file_object import BaseFileObject
from mtbls.domain.entities.study_file import (
    ResourceCategory,
    StudyFileOutput,
)
from mtbls.domain.exceptions.repository import (
    StudyObjectNotFoundError,
)
from mtbls.domain.shared.repository.study_bucket import StudyBucket
from mtbls.infrastructure.persistence.db.mongodb.config import MongoDbConnection
from mtbls.infrastructure.repositories.default.mongodb.default_read_repository import (
    MongoDbDefaultReadRepository,
)

logger = logging.getLogger(__name__)


class MongoDbFileObjectReadRepository(
    FileObjectReadRepository,
    MongoDbDefaultReadRepository[BaseFileObject, str],
):
    def __init__(
        self,
        connection: MongoDbConnection,
        collection_name: str,
        output_entity_class: type[BaseEntity],
        study_bucket: StudyBucket,
        resource_category: ResourceCategory = ResourceCategory.UNKNOWN_RESOURCE,
        observer: FileObjectObserver = None,
    ):
        super(MongoDbDefaultReadRepository, self).__init__(
            connection=connection,
            collection_name=collection_name,
            output_entity_class=output_entity_class,
        )
        super(FileObjectWriteRepository, self).__init__(
            study_bucket=study_bucket, observers=[observer]
        )
        self.study_bucket = study_bucket
        self.resource_category = resource_category

    def get_bucket(self) -> StudyBucket:
        return self.study_bucket

    async def list(
        self, resource_id: str, object_key: Union[None, str] = None
    ) -> list[StudyFileOutput]:
        result = await self.collection.find(
            {"resourceId": resource_id, "parentObjectId": object_key},
            {"_id": 0, "data": 0},
        )

        resources = [StudyFileOutput.model_validate(x) for x in result]

        return resources

    async def exists(
        self,
        resource_id: str,
        object_key: Union[None, str] = None,
    ) -> bool:
        if not resource_id:
            return False
        result = await self.collection.find_one(
            {"resourceId": resource_id, "objectId": object_key},
            {"_id": 1, "data": 0},
        )
        return True if result else False

    async def get_info(self, resource_id: str, object_key: str) -> StudyFileOutput:
        result = await self.collection.find_one(
            {"resourceId": resource_id, "parentObjectId": object_key},
            {"_id": 0, "data": 0},
        )

        return StudyFileOutput.model_validate(result) if result else None

    async def get_uri(self, resource_id: str, object_key: str) -> str:
        cn = self.connection
        collection = self.collection.name
        return f"mongodb://{cn.host}/{cn.database}/{collection}/{resource_id}/{quote(object_key)}"

    async def download(
        self, resource_id: str, object_key: str, target_path: str
    ) -> StudyFileOutput:
        result = await self.collection.find_one(
            {"resourceId": resource_id, "objectId": object_key},
            {"_id": 1},
        )
        if result:
            target = pathlib.Path(target_path)
            target.parent.mkdir(parents=True, exist_ok=True)
            with target.open("w") as f:
                json.dump(result["data"], f)
            return StudyFileOutput.model_validate(result)
        raise StudyObjectNotFoundError(resource_id, self.study_bucket.value, object_key)

    async def get_study_object(
        self,
        resource_id: str,
        object_key: str,
        is_directory: bool = False,
        resource_category: ResourceCategory = ResourceCategory.UNKNOWN_RESOURCE,
        tags: Union[None, dict[str, Any]] = None,
        max_suffix_length: int = 6,
    ) -> StudyFileOutput:
        # Get file or directory metadata
        parent_object_key = ""
        if object_key:
            parent_object_key = str(pathlib.Path(object_key).parent)
            if parent_object_key == ".":
                parent_object_key = ""

        is_symlink = False
        now = datetime.datetime.now(datetime.timezone.utc)
        created_at = now
        updated_at = None
        size_in_bytes = None
        size_in_str = ""
        object_path = pathlib.Path(object_key)
        suffix = ""
        if not is_directory:
            suffixes = [x for x in object_path.suffixes if len(x) <= max_suffix_length]
            if suffixes and len(suffixes) == object_path.suffixes:
                suffix = "".join(suffixes)
            else:
                suffix = object_path.suffix if object_path.suffix else ""

        permission_in_oct = None
        numeric_resource_id = int(resource_id.removeprefix("MTBLS").removeprefix("REQ"))
        return StudyFileOutput(
            bucket_name=self.study_bucket.value(),
            resource_id=resource_id,
            numeric_resource_id=numeric_resource_id,
            object_key=object_key,
            parent_object_key=parent_object_key,
            created_at=created_at,
            updated_at=updated_at,
            size_in_bytes=size_in_bytes,
            is_directory=is_directory,
            is_link=is_symlink,
            size_in_str=size_in_str,
            permission_in_oct=permission_in_oct,
            basename=object_path.name,
            extension=suffix,
            category=resource_category,
            tags=tags if tags else {},
        )
