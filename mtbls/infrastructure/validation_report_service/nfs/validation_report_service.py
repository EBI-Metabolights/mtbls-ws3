import json
import logging
import pathlib
import re
import uuid
from typing import Union

from mtbls.application.services.interfaces.validation_report_service import (
    ValidationReportService,
)
from mtbls.domain.entities.study_file import StudyFileOutput
from mtbls.domain.exceptions.repository import StudyObjectNotFoundError
from mtbls.domain.shared.data_types import ZeroOrPositiveInt
from mtbls.domain.shared.validation_result_file import ValidationResultFile
from mtbls.domain.shared.validator.policy import PolicySummaryResult
from mtbls.infrastructure.repositories.file_object.default.nfs.file_object_write_repository import (  # noqa: E501
    FileSystemObjectWriteRepository,
)

logger = logging.getLogger(__name__)


class FileSystemValidationReportService(ValidationReportService):
    def __init__(
        self,
        file_object_repository: FileSystemObjectWriteRepository,
        validation_history_object_key: str = "validation-history",
        temp_directory: str = "/tmp/validation-history-tmp",
    ):
        self.file_object_repository = file_object_repository
        self.study_bucket = file_object_repository.get_bucket()

        self.temp_directory = (
            pathlib.Path(temp_directory)
            if temp_directory
            else pathlib.Path("/tmp/validation-history-tmp")
        )
        self.temp_directory.mkdir(parents=True, exist_ok=True)
        self.validation_history_object_key = (
            validation_history_object_key.rstrip("/")
            if validation_history_object_key
            else "validation-history"
        )

    async def find_all(
        self,
        resource_id: str,
        offset: Union[None, ZeroOrPositiveInt],
        limit: Union[None, ZeroOrPositiveInt],
    ) -> list[ValidationResultFile]:
        await self._initiate_validation_history_folder(resource_id=resource_id)
        items: list[StudyFileOutput] = await self.file_object_repository.list(
            resource_id=resource_id,
            object_key=self.validation_history_object_key,
        )
        files: list[ValidationResultFile] = []
        for item in items:
            match = re.match(r"validation-history__(.+)__(.+).json$", item.basename)
            if match:
                groups = match.groups()
                definition = ValidationResultFile(
                    validation_time=groups[0], task_id=groups[1]
                )
                files.append(definition)
        files.sort(key=lambda x: x.validation_time, reverse=True)
        if offset:
            files = files[offset:]
        if limit:
            files = files[:limit]
        return files

    async def find_by_task_id(
        self, resource_id: str, task_id: str
    ) -> ValidationResultFile:
        await self._initiate_validation_history_folder(resource_id=resource_id)
        items: list[StudyFileOutput] = await self.file_object_repository.list(
            resource_id=resource_id,
            object_key=self.validation_history_object_key,
        )
        for item in items:
            match = re.match(r"validation-history__(.+)__(.+).json$", item.basename)
            if match:
                groups = match.groups()
                if task_id == groups[1]:
                    return ValidationResultFile(
                        validation_time=groups[0], task_id=groups[1]
                    )
        raise StudyObjectNotFoundError(
            resource_id,
            self.study_bucket.value,
            f"validation-history__*__{task_id}.json",
        )

    async def find_by_validation_time(
        self, resource_id: str, validation_time: str
    ) -> ValidationResultFile:
        await self._initiate_validation_history_folder(resource_id=resource_id)
        items: list[StudyFileOutput] = await self.file_object_repository.list(
            resource_id=resource_id,
            object_key=self.validation_history_object_key,
        )
        for item in items:
            match = re.match(r"validation-history__(.+)__(.+).json$", item.basename)
            if match:
                groups = match.groups()
                if validation_time == groups[0]:
                    return ValidationResultFile(
                        validation_time=groups[0], task_id=groups[1]
                    )
        raise StudyObjectNotFoundError(
            resource_id,
            self.study_bucket.value,
            f"validation-history__{validation_time}__*.json",
        )

    async def save_validation_report(
        self, resource_id: str, task_id: str, validation_result: PolicySummaryResult
    ) -> bool:
        temp_filename = pathlib.Path(f"{str(uuid.uuid4())}.json")
        tmp_file_path = self.temp_directory / temp_filename
        source_uri = f"file://{str(tmp_file_path)}"
        time_str = validation_result.start_time.strftime("%Y-%m-%d_%H-%M-%S")
        object_key = f"{self.validation_history_object_key}/validation-history__{time_str}__{task_id}.json"  # noqa: E501
        try:
            with tmp_file_path.open("w") as f:
                f.write(validation_result.model_dump_json(indent=4, by_alias=True))

            await self._initiate_validation_history_folder(resource_id=resource_id)
            file_exists = await self.file_object_repository.exists(
                resource_id=resource_id, object_key=object_key
            )
            await self.file_object_repository.put_object(
                resource_id=resource_id,
                object_key=object_key,
                source_uri=source_uri,
            )
            study_object = await self.file_object_repository.get_info(
                resource_id=resource_id, object_key=object_key
            )
            if file_exists:
                await self.file_object_repository.object_updated(
                    study_object=study_object
                )
            else:
                await self.file_object_repository.object_created(
                    study_object=study_object
                )

        finally:
            tmp_file_path.unlink(missing_ok=True)

    async def _initiate_validation_history_folder(self, resource_id: str):
        if not await self.file_object_repository.exists(
            resource_id=resource_id,
            object_key=self.validation_history_object_key,
        ):
            await self.file_object_repository.create_folder_object(
                resource_id=resource_id,
                object_key=self.validation_history_object_key,
                exist_ok=True,
            )
            study_object = await self.file_object_repository.get_info(
                resource_id=resource_id,
                object_key=self.validation_history_object_key,
            )
            await self.file_object_repository.object_created(study_object=study_object)

    async def load_validation_report_by_task_id(
        self, resource_id: str, task_id: str
    ) -> PolicySummaryResult:
        await self._initiate_validation_history_folder(resource_id=resource_id)
        selected_object = await self.find_by_task_id(
            resource_id=resource_id, task_id=task_id
        )
        return await self._load_validation_report(
            resource_id=resource_id, selected_object=selected_object
        )

    async def load_validation_report_by_validation_time(
        self, resource_id: str, validation_time: str
    ) -> PolicySummaryResult:
        await self._initiate_validation_history_folder(resource_id=resource_id)
        selected_object = await self.find_by_validation_time(
            resource_id=resource_id, validation_time=validation_time
        )
        return self._load_validation_report(
            resource_id=resource_id, selected_object=selected_object
        )

    async def _load_validation_report(
        self, resource_id: str, selected_object: ValidationResultFile
    ) -> PolicySummaryResult:
        task_id = selected_object.task_id
        time_str = selected_object.validation_time
        object_key = f"{self.validation_history_object_key}/validation-history__{time_str}__{task_id}.json"  # noqa: E501
        temp_filename = pathlib.Path(f"{str(uuid.uuid4())}.json")
        tmp_file_path = self.temp_directory / temp_filename

        try:
            await self.file_object_repository.download(
                resource_id=resource_id,
                object_key=object_key,
                target_path=str(tmp_file_path),
            )

            with tmp_file_path.open() as f:
                validations_obj = json.load(f)
            result = PolicySummaryResult.model_validate(validations_obj)
            return result
        except Exception as ex:
            logger.exception(ex)
            raise ex
        finally:
            tmp_file_path.unlink(missing_ok=True)
