from typing import Generic, TypeVar, Union

from mtbls.domain.entities.study_file import StudyDataFileOutput

T = TypeVar("T")


class BaseFileObject(StudyDataFileOutput, Generic[T]):
    data: Union[None, T] = None
