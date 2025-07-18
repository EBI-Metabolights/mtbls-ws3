from mtbls.application.services.interfaces.repositories.default.abstract_write_repository import (
    AbstractWriteRepository,
)
from mtbls.domain.entities.study import StudyInput, StudyOutput


class StudyWriteRepository(AbstractWriteRepository[StudyInput, StudyOutput, int]): ...
