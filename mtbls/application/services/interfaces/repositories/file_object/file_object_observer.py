# Step 2: The Observer Interface

from mtbls.domain.entities.study_file import StudyFileInput, StudyFileOutput


class FileObjectRepositorySource: ...


class FileObjectObserver:
    async def repository_registered(repository: FileObjectRepositorySource): ...

    async def repository_unregistered(repository: FileObjectRepositorySource): ...

    async def object_updated(self, study_object: StudyFileOutput): ...

    async def object_deleted(self, study_object: StudyFileOutput): ...

    async def object_created(self, study_object: StudyFileInput): ...


class DefaultFileObjectObserver(FileObjectObserver):
    def __init__(self):
        super().__init__()
        self.registered_repositories: set[FileObjectRepositorySource] = set()

    async def repository_registered(self, repository: FileObjectRepositorySource):
        self.registered_repositories.add(repository)

    async def repository_unregistered(self, repository: FileObjectRepositorySource):
        self.registered_repositories.discard(repository)

    async def object_updated(self, study_object: StudyFileOutput): ...

    async def object_deleted(self, study_object: StudyFileOutput): ...

    async def object_created(self, study_object: StudyFileInput): ...
