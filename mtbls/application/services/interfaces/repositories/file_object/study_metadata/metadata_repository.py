from mtbls.application.services.interfaces.repositories.default.abstract_write_repository import (  # noqa: E501
    AbstractWriteRepository,
)
from mtbls.application.services.interfaces.repositories.file_object.base import (
    BaseFileObjectRepository,
)
from mtbls.domain.entities.investigation import InvestigationFileObject
from mtbls.domain.entities.isa_table import (
    IsaTableFileObject,
    IsaTableRowObject,
)


class InvestigationObjectRepository(
    BaseFileObjectRepository,
    AbstractWriteRepository[InvestigationFileObject, InvestigationFileObject, str],
):
    def __init__(self, study_bucket):
        super().__init__(study_bucket)


class IsaTableObjectRepository(
    BaseFileObjectRepository,
    AbstractWriteRepository[IsaTableFileObject, IsaTableFileObject, str],
):
    def __init__(self, study_bucket):
        super().__init__(study_bucket)


class IsaTableRowObjectRepository(
    BaseFileObjectRepository,
    AbstractWriteRepository[IsaTableRowObject, IsaTableRowObject, str],
):
    def __init__(self, study_bucket):
        super().__init__(study_bucket)
