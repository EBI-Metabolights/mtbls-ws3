import datetime
import json
import logging
import pathlib
import re
from typing import Any, Union
from urllib.parse import quote

import httpx
from pymongo.results import DeleteResult

from mtbls.application.services.interfaces.repositories.file_object.file_object_observer import (  # noqa: E501
    FileObjectObserver,
)
from mtbls.application.services.interfaces.repositories.file_object.file_object_write_repository import (  # noqa: E501
    FileObjectWriteRepository,
)
from mtbls.domain.entities.base_entity import BaseEntity
from mtbls.domain.entities.base_file_object import BaseFileObject
from mtbls.domain.entities.study_file import ResourceCategory, StudyFileOutput
from mtbls.domain.exceptions.repository import (
    StudyObjectAlreadyExistsError,
    StudyObjectNotFoundError,
    UnaccessibleUriError,
    UnsupportedUriError,
)
from mtbls.domain.shared.repository.study_bucket import StudyBucket
from mtbls.infrastructure.persistence.db.mongodb.config import MongoDbConnection
from mtbls.infrastructure.repositories.default.mongodb.default_write_repository import (
    MongoDbDefaultWriteRepository,
)

logger = logging.getLogger(__name__)


class MongoDbFileObjectWriteRepository(
    FileObjectWriteRepository,
    MongoDbDefaultWriteRepository[BaseFileObject, BaseFileObject, str],
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
        super(MongoDbDefaultWriteRepository, self).__init__(
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

    async def put_object(
        self,
        resource_id: str,
        object_key: str,
        source_uri: str,
        override: bool = True,
    ) -> bool:
        current = await self.collection.find_one(
            {"resourceId": resource_id, "objectId": object_key},
            {"_id": 1, "data": 0},
        )
        file_exists = current is not None
        if file_exists and not override:
            raise StudyObjectAlreadyExistsError(
                resource_id, self.study_bucket.value, object_key
            )

        if source_uri and source_uri.startswith("file://"):
            result = await self.put_with_local_file_provider(
                source_uri, resource_id, object_key, file_exists
            )
            uploaded = True
        elif re.match(r"^https?://", source_uri):
            result = await self.put_with_http_file_provider(
                source_uri, resource_id, object_key, file_exists
            )
            uploaded = True

        if uploaded:
            if file_exists:
                study_object = await self.object_updated(
                    StudyFileOutput.model_validate(x) for x in current
                )
            else:
                study_object = await self.get_study_object(resource_id, object_key)

                await self.object_created(study_object)
            return result

        raise NotImplementedError(source_uri)

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

    async def create_folder_object(
        self,
        resource_id: str,
        object_key: str,
        exist_ok: bool = True,
    ) -> bool:
        current = await self.collection.find_one(
            {"resourceId": resource_id, "objectId": object_key},
            {"_id": 1, "data": 0},
        )
        file_exists = current is not None
        if file_exists and not exist_ok:
            raise StudyObjectAlreadyExistsError(
                resource_id, self.study_bucket.value, object_key
            )

        study_object = await self.get_study_object(
            resource_id, object_key, is_directory=True
        )

        await self.object_created(study_object)
        return True

    async def put_with_local_file_provider(
        self, source_uri: str, resource_id: str, object_key: str
    ) -> bool:
        source_path = await self._convert_uri_to_path(source_uri)
        source = pathlib.Path(source_path)
        with source.open() as f:
            data = json.load(f)
        return self.update_data(resource_id, object_key, data)

    async def update_data(
        self, resource_id: str, object_key: str, data: dict[str, Any]
    ):
        result = await self.collection.find_one(
            {"resourceId": resource_id, "objectId": object_key},
            {"_id": 1, "data": 0},
        )
        if result:
            self.collection.update_one(
                {"resourceId": resource_id, "objectId": object_key},
                {"$set": {"data": data}},
            )
        else:
            study_object = await self.get_study_object(
                resource_id=resource_id,
                object_key=object_key,
                is_directory=False,
                resource_category=self.resource_category,
            )
            input_json = study_object.model_validate(by_alias=True, exclude="_id")
            input_json["data"] = data
            self.collection.insert_one(input_json)

    async def _convert_uri_to_path(self, uri: str) -> pathlib.Path:
        if not uri or not uri.startswith("file://"):
            raise UnsupportedUriError(uri)

        uri_path = pathlib.Path(uri.replace("file://", "", 1))
        if not uri_path.exists():
            raise UnaccessibleUriError(uri_path)
        return uri_path

    async def put_with_http_file_provider(
        self, source_uri: str, resource_id: str, object_key: str
    ) -> bool:
        if not source_uri or not re.match(r"^https?://", source_uri):
            raise UnsupportedUriError(source_uri)
        response = httpx.get(source_uri)
        response.raise_for_status()
        data = json.loads(response.text)
        return self.update_data(resource_id, object_key, data)

    async def delete_object(
        self,
        resource_id: str,
        object_key: str,
        ignore_not_exist: bool = True,
    ) -> bool:
        result = await self.collection.find_one(
            {"resourceId": resource_id, "objectId": object_key},
            {"_id": 1, "data": 0},
        )
        if not result and ignore_not_exist:
            return False
        collection = self.collection
        delete_result: DeleteResult = await collection.delete_one(
            {"_id": result["_id"]}
        )
        if delete_result.deleted_count > 0:
            study_object = StudyFileOutput.model_validate(result)
            await self.object_deleted(study_object)
            return True
        return False
