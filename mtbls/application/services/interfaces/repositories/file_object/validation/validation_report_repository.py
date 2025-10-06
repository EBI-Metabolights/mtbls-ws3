import abc
from typing import Union

from mtbls.application.services.interfaces.repositories.default.abstract_write_repository import (  # noqa: E501
    AbstractWriteRepository,
)
from mtbls.application.services.interfaces.repositories.file_object.base import (
    BaseFileObjectRepository,
)
from mtbls.application.services.interfaces.repositories.file_object.file_object_observer import (  # noqa: E501
    FileObjectObserver,
)
from mtbls.domain.entities.validation_report import ValidationReport


class ValidationReportRepository(
    BaseFileObjectRepository,
    AbstractWriteRepository[ValidationReport, ValidationReport, str],
    abc.ABC,
):
    def __init__(self, study_bucket, observer: Union[None, FileObjectObserver]):
        super().__init__(study_bucket, observers=[observer])
