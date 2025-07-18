import abc
from typing import Sequence, Union

from mtbls.application.services.interfaces.repositories.file_object.file_object_observer import (
    FileObjectObserver,
    FileObjectRepositorySource,
)
from mtbls.domain.entities.study_file import StudyFileInput, StudyFileOutput
from mtbls.domain.shared.repository.study_bucket import StudyBucket


class BaseObjectRepository(abc.ABC):
    def __init__(self, study_bucket: StudyBucket):
        self.study_bucket = study_bucket


class BaseFileObjectRepository(
    BaseObjectRepository, FileObjectRepositorySource, abc.ABC
):
    def __init__(
        self,
        study_bucket,
        observers: Union[None, Sequence[FileObjectObserver]] = None,
    ):
        super().__init__(study_bucket)
        if observers:
            self._observers: set[FileObjectObserver] = set(x for x in observers if x)

    async def attach(self, observer: FileObjectObserver):
        self._observers.add(observer)
        await observer.repository_registered(self)

    async def detach(self, observer: FileObjectObserver):
        self._observers.remove(observer)
        await observer.repository_unregistered(self)

    async def object_updated(self, study_object: StudyFileOutput):
        for observer in self._observers:
            await observer.object_updated(study_object)

    async def object_created(self, study_object: StudyFileInput):
        for observer in self._observers:
            await observer.object_created(study_object)

    async def object_deleted(self, study_object: StudyFileOutput):
        for observer in self._observers:
            await observer.object_deleted(study_object)
