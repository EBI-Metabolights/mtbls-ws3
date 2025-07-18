from typing import Generic, TypeVar, Union

from mtbls.domain.entities.study_file import StudyFileOutput

T = TypeVar("T")


class BaseFileObject(StudyFileOutput, Generic[T]):
    data: Union[None, T] = None
