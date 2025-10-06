from mtbls.application.services.interfaces.repositories.default.abstract_write_repository import (  # noqa: E501
    AbstractWriteRepository,
)
from mtbls.application.services.interfaces.repositories.file_object.base import (
    BaseFileObjectRepository,
)
from mtbls.domain.entities.validation_override import (
    ValidationOverrideFileObject,
)


class ValidationOverrideRepository(
    BaseFileObjectRepository,
    AbstractWriteRepository[
        ValidationOverrideFileObject, ValidationOverrideFileObject, str
    ],
):
    def __init__(self, study_bucket):
        super().__init__(study_bucket)
