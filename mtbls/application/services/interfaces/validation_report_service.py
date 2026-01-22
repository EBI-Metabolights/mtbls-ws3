import abc
from typing import Union

from mtbls.domain.entities.validation.validation_report import ValidationReport
from mtbls.domain.shared.data_types import ZeroOrPositiveInt
from mtbls.domain.shared.validator.policy import PolicySummaryResult


class ValidationReportService(abc.ABC):
    @abc.abstractmethod
    async def find_all(
        self,
        resource_id: str,
        offset: Union[None, ZeroOrPositiveInt],
        limit: Union[None, ZeroOrPositiveInt],
    ) -> list[ValidationReport]: ...

    @abc.abstractmethod
    async def find_by_task_id(
        self, resource_id: str, task_id: str
    ) -> ValidationReport: ...

    @abc.abstractmethod
    async def find_by_validation_time(
        self, resource_id: str, validation_time: str
    ) -> ValidationReport: ...

    @abc.abstractmethod
    async def save_validation_report(
        self, resource_id: str, task_id: str, validation_result: PolicySummaryResult
    ) -> bool: ...

    @abc.abstractmethod
    async def load_validation_report_by_task_id(
        self, resource_id: str, task_id: str
    ) -> PolicySummaryResult: ...

    @abc.abstractmethod
    async def load_validation_report_by_validation_time(
        self, resource_id: str, validation_time: str
    ) -> PolicySummaryResult: ...
