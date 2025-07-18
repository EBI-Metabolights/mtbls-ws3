import logging
import os
import pathlib
import shutil
from typing import Any, Union

from pydantic import BaseModel

from mtbls.domain.exceptions.repository import (
    StudyObjectAlreadyExistsError,
    StudyObjectIsNotFileError,
    StudyObjectIsNotFolderError,
    StudyObjectNotFoundError,
    UnexpectedStudyObjectTypeError,
)
from mtbls.domain.shared.repository.study_bucket import StudyBucket
from mtbls.infrastructure.repositories.file_object.default.nfs.study_folder_config import (
    StudyFolderConfiguration,
)

logger = logging.getLogger(__name__)


class StudyFolderManager:
    def __init__(self, config: Union[dict[str, Any], StudyFolderConfiguration]):
        self.config = config
        if isinstance(config, dict):
            self.config = StudyFolderConfiguration.model_validate(config)
        paths = self.config.mounted_paths
        self._convert_to_real_paths(paths)

        self.root_path_bucket_str_map: dict[StudyBucket, str] = {
            StudyBucket.AUDIT_FILES: paths.audit_files_root_path,
            StudyBucket.PRIVATE_METADATA_FILES: paths.private_metadata_files_root_path,
            StudyBucket.DELETED_PRIVATE_METADATA_FILES: paths.deleted_private_metadata_files_root_path,
            StudyBucket.INTERNAL_FILES: paths.internal_files_root_path,
            StudyBucket.AUDIT_FILES: paths.audit_files_root_path,
            StudyBucket.PRIVATE_DATA_FILES: paths.private_data_files_root_path,
            StudyBucket.PUBLIC_DATA_FILES: paths.public_data_files_root_path,
            StudyBucket.DELETED_PRIVATE_DATA_FILES: paths.deleted_private_data_files_root_path,
            StudyBucket.UPLOADED_FILES: paths.uploaded_files_root_path,
            StudyBucket.DELETED_UPLOADED_FILES: paths.deleted_uploaded_files_root_path,
        }
        self.root_path_bucket_map: dict[str, str] = {
            x: pathlib.Path(self.root_path_bucket_str_map[x])
            for x in self.root_path_bucket_str_map
        }
        self.root_path_bucket_name_map: dict[str, str] = {
            x.value: self.root_path_bucket_map[x] for x in self.root_path_bucket_map
        }

    @staticmethod
    def _convert_to_real_paths(paths: BaseModel):
        for field_name in paths.model_fields:
            value = paths.model_fields[field_name]
            if field_name.endswith("_path"):
                value = getattr(paths, field_name)
                if str(value):
                    setattr(paths, field_name, os.path.realpath(value))
        return paths

    def get_study_folder_path(
        self, resource_id: str, bucket_name: str, object_key: Union[None, str] = None
    ) -> pathlib.Path:
        root_path = self.root_path_bucket_name_map[bucket_name]
        subpath = pathlib.Path(
            pathlib.Path(resource_id) / pathlib.Path(object_key.lstrip("/"))
            if object_key
            else resource_id
        )
        return root_path / subpath

    def create_study_folder_path(
        self,
        resource_id: str,
        bucket_name: str,
        object_key: Union[None, str] = None,
        exist_ok: bool = False,
    ) -> bool:
        folder_path = self.get_study_folder_path(resource_id, bucket_name, object_key)
        valid_link = False
        if folder_path.is_symlink() and folder_path.resolve().exists():
            valid_link = True

        if not exist_ok and (folder_path.exists() or valid_link):
            raise StudyObjectAlreadyExistsError(
                resource_id=resource_id, bucket_name=bucket_name, object_key=object_key
            )
        if folder_path.is_symlink() and not valid_link:
            folder_path.unlink()
        logger.info("Folder created: %s", str(folder_path))
        folder_path.mkdir(parents=True, exist_ok=exist_ok)
        return True

    def delete(
        self,
        resource_id: str,
        bucket_name: str,
        object_key: Union[None, str] = None,
        ignore_not_exist: bool = False,
    ) -> bool:
        folder_path = self.get_study_folder_path(
            resource_id=resource_id, bucket_name=bucket_name, object_key=object_key
        )
        exist = folder_path.exists()
        if not exist and not ignore_not_exist:
            raise StudyObjectNotFoundError(
                resource_id=resource_id,
                bucket_name=bucket_name,
                object_key=object_key,
            )
        if not exist and ignore_not_exist:
            return False

        if folder_path.is_dir():
            return self.delete_study_folder_path(
                resource_id=resource_id,
                bucket_name=bucket_name,
                object_key=object_key,
                ignore_not_exist=ignore_not_exist,
            )
        if folder_path.is_file():
            return self.delete_study_folder_file(
                resource_id=resource_id,
                bucket_name=bucket_name,
                object_key=object_key,
                ignore_not_exist=ignore_not_exist,
            )

        raise UnexpectedStudyObjectTypeError(
            resource_id=resource_id, bucket_name=bucket_name, object_key=object_key
        )

    def delete_study_folder_path(
        self,
        resource_id: str,
        bucket_name: str,
        object_key: Union[None, str] = None,
        ignore_not_exist: bool = False,
    ) -> bool:
        folder_path = self.get_study_folder_path(
            resource_id=resource_id, bucket_name=bucket_name, object_key=object_key
        )
        if folder_path.is_symlink():
            if not folder_path.resolve():
                folder_path.unlink()
                return True
            if folder_path.resolve().exists() and folder_path.resolve().is_dir():
                folder_path.unlink()
                return True
            raise StudyObjectIsNotFolderError(
                resource_id=resource_id, bucket_name=bucket_name, object_key=object_key
            )
        if folder_path.exists():
            if folder_path.is_dir():
                shutil.rmtree(folder_path, ignore_errors=True)
                logger.info("Folder deleted: %s", str(folder_path))
                return True
            raise StudyObjectIsNotFolderError(
                resource_id=resource_id, bucket_name=bucket_name, object_key=object_key
            )
        if not ignore_not_exist:
            raise StudyObjectNotFoundError(
                resource_id=resource_id, bucket_name=bucket_name, object_key=object_key
            )
        return False

    def delete_study_folder_file(
        self,
        resource_id: str,
        bucket_name: str,
        object_key: Union[None, str] = None,
        ignore_not_exist: bool = False,
    ) -> bool:
        file_path = self.get_study_folder_path(
            resource_id=resource_id, bucket_name=bucket_name, object_key=object_key
        )
        if file_path.is_symlink():
            if not file_path.resolve():
                file_path.unlink()
                return True
            if file_path.resolve().exists() and file_path.resolve().is_file():
                file_path.unlink()
                return True
            raise StudyObjectIsNotFileError(
                resource_id=resource_id, bucket_name=bucket_name, object_key=object_key
            )
        if file_path.exists():
            if file_path.is_file():
                file_path.unlink(missing_ok=True)
                logger.info("File deleted: %s", str(file_path))
                return True
            raise StudyObjectIsNotFileError(
                resource_id=resource_id, bucket_name=bucket_name, object_key=object_key
            )
        if not ignore_not_exist:
            raise StudyObjectNotFoundError(
                resource_id=resource_id, bucket_name=bucket_name, object_key=object_key
            )
        return False
