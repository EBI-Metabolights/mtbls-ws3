import datetime
import logging
from pathlib import Path
from typing import Any

from mtbls.application.services.interfaces.repositories.file_object.file_object_observer import (
    FileObjectObserver,
)
from mtbls.application.services.interfaces.repositories.file_object.validation.validation_report_repository import (
    ValidationReportRepository,
)
from mtbls.domain.entities.base_entity import BaseEntity
from mtbls.domain.entities.study_file import ResourceCategory, StudyFileOutput
from mtbls.domain.entities.validation_report import ValidationReport
from mtbls.domain.exceptions.repository import StudyObjectNotFoundError
from mtbls.domain.shared.repository.entity_filter import EntityFilter
from mtbls.domain.shared.repository.query_options import QueryOptions
from mtbls.domain.shared.repository.study_bucket import StudyBucket
from mtbls.domain.shared.validation_result_file import ValidationResultFile
from mtbls.domain.shared.validator.policy import PolicySummaryResult
from mtbls.infrastructure.persistence.db.mongodb.config import (
    MongoDbConnection,
)
from mtbls.infrastructure.repositories.default.mongodb.default_write_repository import (
    MongoDbDefaultWriteRepository,
)

logger = logging.getLogger(__name__)


class MongoDbValidationReportRepository(
    MongoDbDefaultWriteRepository[ValidationReport, ValidationReport, str],
    ValidationReportRepository,
):
    def __init__(
        self,
        connection: MongoDbConnection,
        output_entity_class: type[BaseEntity] = ValidationReport,
        study_bucket: StudyBucket = StudyBucket.INTERNAL_FILES,
        collection_name: str = "validation_reports",
        validation_history_object_key: str = "validation-history",
        observer: FileObjectObserver = None,
    ):
        super(MongoDbDefaultWriteRepository, self).__init__(
            connection=connection,
            collection_name=collection_name,
            output_entity_class=output_entity_class,
        )
        super(ValidationReportRepository, self).__init__(
            study_bucket, observers=[observer]
        )
        self.validation_history_object_key = (
            validation_history_object_key.rstrip("/")
            if validation_history_object_key
            else "validation-history"
        )
        self.parent_object_key = str(Path(validation_history_object_key).parent)
        if self.parent_object_key == ".":
            self.parent_object_key = ""

    async def find_all(self, resource_id: str) -> list[ValidationResultFile]:
        filters = {"resourceId": resource_id}
        return self.find_with_filter(filters=filters)

    async def find_with_filter(self, filters: dict[str, Any]):
        files = await self.collection.find(
            filters,
            {"_id": 1, "startTime": 1, "taskId": 1},
        ).sort({"startTime": -1})
        return [
            ValidationResultFile(
                validation_time=self._format_datetime(x["startTime"]),
                task_id=x["taskId"],
            )
            for x in files
        ]

    def _format_datetime(self, date_value: datetime.datetime):
        return date_value.strftime("%Y-%m-%d_%H-%M-%S")

    async def find_by_task_id(
        self, resource_id: str, task_id: str
    ) -> ValidationResultFile:
        filters = {"resourceId": resource_id, "taskId": task_id}
        result = self.find_with_filter(filters=filters)
        if result:
            return result
        raise StudyObjectNotFoundError(
            resource_id,
            self.study_bucket.value,
            f"validation-history__*__{task_id}.json",
        )

    async def find_by_validation_time(
        self, resource_id: str, validation_time: str
    ) -> ValidationResultFile:
        filters = {"resourceId": resource_id, "startTime": validation_time}
        result = self.find_with_filter(filters=filters)
        if result:
            return result
        raise StudyObjectNotFoundError(
            resource_id,
            self.study_bucket.value,
            validation_time,
        )

    async def save_validation_report(
        self, resource_id: str, task_id: str, validation_result: PolicySummaryResult
    ) -> bool:
        time_str = validation_result.start_time.strftime("%Y-%m-%d_%H-%M-%S")
        filters = {"resourceId": resource_id, "taskId": task_id}
        result = self.find_with_filter(filters=filters)

        object_key = f"{self.validation_history_object_key}/validation-history__{time_str}__{task_id}.json"

        created = False
        if result:
            report = await self.update(validation_result)
        else:
            created = True
            report = await self.create(validation_result)

        now = datetime.datetime.now(datetime.timezone.utc)
        object_key_path = Path(object_key)
        numeric_resource_id = (resource_id.removeprefix("REQ").removeprefix("MTBLS"),)
        study_object = StudyFileOutput(
            id_=report.id_,
            resource_id=resource_id,
            numeric_resource_id=numeric_resource_id,
            bucket_name=self.study_bucket.value,
            basename=object_key_path.name,
            object_key=object_key,
            parent_object_key=self.validation_history_object_key,
            created_at=now if created else None,
            updated_at=now if not created else None,
            is_directory=True,
            category=ResourceCategory.INTERNAL_RESOURCE,
        )
        if created:
            await self.object_created(study_object=study_object)
        else:
            await self.object_updated(study_object=study_object)
        return True

    async def load_validation_report_by_task_id(
        self, resource_id: str, task_id: str
    ) -> PolicySummaryResult:
        await self._initiate_validation_history_folder(resource_id=resource_id)
        filters = [
            EntityFilter(key="resourceId", value=resource_id),
            EntityFilter(key="taskId", value=task_id),
        ]
        reports = await self.find(query_options=QueryOptions(filters=filters))
        if reports.data:
            return reports.data[0].data

        raise StudyObjectNotFoundError(resource_id, self.study_bucket.value, task_id)

    async def load_validation_report_by_validation_time(
        self, resource_id: str, validation_time: str
    ) -> PolicySummaryResult:
        filters = [
            EntityFilter(key="resourceId", value=resource_id),
            EntityFilter(key="startTime", value=validation_time),
        ]
        reports = await self.find(query_options=QueryOptions(filters=filters))
        if reports.data:
            return reports.data[0].data
        raise StudyObjectNotFoundError(
            resource_id, self.study_bucket.value, validation_time
        )
