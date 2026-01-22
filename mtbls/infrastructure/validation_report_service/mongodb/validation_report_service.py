import datetime
import logging
from pathlib import Path
from typing import Any, Union

from mtbls.application.services.interfaces.validation_report_service import (
    ValidationReportService,
)
from mtbls.domain.entities.study_file import ResourceCategory, StudyFileOutput
from mtbls.domain.entities.validation.validation_report import ValidationReport
from mtbls.domain.exceptions.repository import StudyObjectNotFoundError
from mtbls.domain.shared.data_types import ZeroOrPositiveInt
from mtbls.domain.shared.repository.entity_filter import EntityFilter
from mtbls.domain.shared.repository.query_options import QueryOptions
from mtbls.domain.shared.validation_result_file import ValidationResultFile
from mtbls.domain.shared.validator.policy import PolicySummaryResult
from mtbls.infrastructure.repositories.file_object.validation.reports.mongodb.validation_override_repository import (  # noqa: E501
    MongoDbValidationReportRepository,
)

logger = logging.getLogger(__name__)


class MongoDbValidationReportService(ValidationReportService):
    def __init__(
        self,
        validation_report_repository: MongoDbValidationReportRepository,
        validation_history_object_key: str = "validation-history",
    ):
        self.validation_report_repository = validation_report_repository
        self.collection = self.validation_report_repository.collection
        self.write_repository = validation_report_repository
        self.study_bucket = self.validation_report_repository.study_bucket
        self.validation_history_object_key = (
            validation_history_object_key.rstrip("/")
            if validation_history_object_key
            else "validation-history"
        )
        self.parent_object_key = str(Path(validation_history_object_key).parent)
        if self.parent_object_key == ".":
            self.parent_object_key = ""

    async def find_all(
        self,
        resource_id: str,
        offset: Union[None, ZeroOrPositiveInt],
        limit: Union[None, ZeroOrPositiveInt],
    ) -> list[ValidationResultFile]:
        filters = {"resourceId": resource_id}
        return await self.find_with_filter(filters=filters, offset=offset, limit=limit)

    async def find_with_filter(
        self,
        filters: dict[str, Any],
        offset: Union[None, ZeroOrPositiveInt] = None,
        limit: Union[None, ZeroOrPositiveInt] = None,
    ):
        files = self.collection.find(
            filters, {"data.taskId": 1, "data.startTime": 1}
        ).sort({"data.startTime": -1})
        if offset:
            files = files.skip(skip=offset)
        if limit:
            files = files.limit(limit=limit)
        values = [x for x in files]
        return [
            ValidationResultFile(
                validation_time=self._format_datetime(x["data"]["startTime"]),
                task_id=x["data"]["taskId"],
            )
            for x in values
        ]

    def _format_datetime(self, date_value: datetime.datetime):
        return date_value.strftime("%Y-%m-%d_%H-%M-%S")

    async def find_by_task_id(
        self, resource_id: str, task_id: str
    ) -> ValidationResultFile:
        filters = {"resourceId": resource_id, "data.taskId": task_id}
        result = await self.find_with_filter(filters=filters)
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
        filters = {"resourceId": resource_id, "data.startTime": validation_time}
        result = await self.find_with_filter(filters=filters)
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
        object_key = f"{self.validation_history_object_key}/validation-history__{time_str}__{task_id}.json"  # noqa: E501
        filters = {"resourceId": resource_id, "data.taskId": task_id}
        result = await self.find_with_filter(filters=filters)
        file = self.collection.find_one(filters, {"data": 0, "_id": 1})
        now = datetime.datetime.now(datetime.timezone.utc)
        validation_result.task_id = task_id
        if file:
            report = ValidationReport.model_validate(file)
            report.updated_at = now
            report.data = validation_result
            await self.write_repository.update(report)

        else:
            object_key_path = Path(object_key)
            numeric_resource_id = int(
                resource_id.removeprefix("REQ").removeprefix("MTBLS")
            )
            report = ValidationReport(
                resource_id=resource_id,
                numeric_resource_id=numeric_resource_id,
                bucket_name=self.study_bucket.value,
                basename=object_key_path.name,
                object_key=object_key,
                parent_object_key=self.validation_history_object_key,
                is_directory=False,
                created_at=now,
                category=ResourceCategory.INTERNAL_RESOURCE,
                data=validation_result,
                extension=object_key_path.suffix,
            )
            await self.write_repository.create(report)

        study_object = StudyFileOutput.model_validate(report.model_dump(exclude="data"))
        if result:
            await self.validation_report_repository.object_updated(
                study_object=study_object
            )
        else:
            await self.validation_report_repository.object_created(
                study_object=study_object
            )

        return True

    async def load_validation_report_by_task_id(
        self, resource_id: str, task_id: str
    ) -> PolicySummaryResult:
        filters = [
            EntityFilter(key="resourceId", value=resource_id),
            EntityFilter(key="data.taskId", value=task_id),
        ]
        reports = await self.validation_report_repository.find(
            query_options=QueryOptions(filters=filters)
        )
        if reports.data:
            return reports.data[0].data

        raise StudyObjectNotFoundError(resource_id, self.study_bucket.value, task_id)

    async def load_validation_report_by_validation_time(
        self, resource_id: str, validation_time: str
    ) -> PolicySummaryResult:
        filters = [
            EntityFilter(key="resourceId", value=resource_id),
            EntityFilter(key="data.startTime", value=validation_time),
        ]
        reports = await self.validation_report_repository.find(
            query_options=QueryOptions(filters=filters)
        )
        if reports.data:
            return reports.data[0].data
        raise StudyObjectNotFoundError(
            resource_id, self.study_bucket.value, validation_time
        )
