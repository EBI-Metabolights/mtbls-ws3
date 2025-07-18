import enum
from typing import Union

from metabolights_utils.common import CamelCaseModel
from pydantic import field_validator

from mtbls.domain.entities.base_entity import BaseStudyFile, BaseStudyFileInput
from mtbls.domain.shared.data_types import Integer, UtcDatetime


class ResourceCategory(enum.StrEnum):
    METADATA_RESOURCE = "metadata"
    DATA_RESOURCE = "data"
    SUPPLEMENTARY_RESOURCE = "supplementary"
    INTERNAL_RESOURCE = "internal"
    ROOT_RESOURCE = "root"
    FOLDER_RESOURCE = "folder"
    UNKNOWN_RESOURCE = "unknown"


class HashAlgorithm(enum.StrEnum):
    SHA1 = "sha1"
    SHA256 = "sha256"
    SHA512 = "sha512"
    MD5 = "md5"


class StudyFileInput(CamelCaseModel, BaseStudyFileInput):
    bucket_name: Union[None, str] = None
    resource_id: Union[None, str] = None
    numeric_resource_id: Union[None, int] = None
    basename: Union[None, str] = None
    object_key: Union[None, str] = None
    parent_object_key: Union[None, str] = None
    created_at: Union[None, str, UtcDatetime] = None
    updated_at: Union[None, str, UtcDatetime] = None
    size_in_bytes: Union[None, Integer] = None
    size_in_str: Union[None, str] = None
    is_directory: bool = False
    is_link: bool = False
    hashes: dict[HashAlgorithm, str] = {}
    permission_in_oct: Union[None, str] = None
    extension: str = ""
    category: ResourceCategory = ResourceCategory.UNKNOWN_RESOURCE
    tags: dict[str, Union[str, int, float, bool, UtcDatetime]] = {}

    @field_validator("category", mode="before")
    @classmethod
    def new_type_validator(cls, value):
        if isinstance(value, ResourceCategory):
            return value
        if isinstance(value, str):
            return ResourceCategory(value)

        return ResourceCategory.UNKNOWN_RESOURCE


class StudyFileOutput(StudyFileInput, BaseStudyFile): ...
