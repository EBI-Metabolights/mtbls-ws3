import abc

from mtbls.application.services.interfaces.repositories.default.abstract_read_repository import (  # noqa: E501
    AbstractReadRepository,
)
from mtbls.domain.entities.study_file import StudyFileOutput


class StudyFileReadRepository(
    AbstractReadRepository[StudyFileOutput, str],
    abc.ABC,
): ...
