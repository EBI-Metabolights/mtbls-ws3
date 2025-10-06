from dependency_injector import containers, providers

from mtbls.application.services.interfaces.repositories.file_object.file_object_write_repository import (  # noqa: E501
    FileObjectWriteRepository,
)
from mtbls.application.services.interfaces.repositories.study.study_read_repository import (  # noqa: E501
    StudyReadRepository,
)
from mtbls.application.services.interfaces.repositories.study_file.study_file_write_repository import (  # noqa: E501
    StudyFileRepository,
)
from mtbls.application.services.interfaces.repositories.user.user_read_repository import (  # noqa: E501
    UserReadRepository,
)
from mtbls.application.services.interfaces.repositories.user.user_write_repository import (  # noqa: E501
    UserWriteRepository,
)
from mtbls.domain.shared.repository.study_bucket import StudyBucket
from mtbls.infrastructure.persistence.db.alias_generator import AliasGenerator
from mtbls.infrastructure.persistence.db.model.alias_generator import (
    DbTableAliasGeneratorImpl,
)
from mtbls.infrastructure.persistence.db.model.entity_mapper import EntityMapper
from mtbls.infrastructure.repositories.file_object.default.nfs.file_object_write_repository import (  # noqa: E501
    FileSystemObjectWriteRepository,
)
from mtbls.infrastructure.repositories.file_object.default.nfs.study_folder_manager import (  # noqa: E501
    StudyFolderManager,
)
from mtbls.infrastructure.repositories.study.db.study_read_repository import (
    SqlDbStudyReadRepository,
)
from mtbls.infrastructure.repositories.study_file.sql_db.study_file_repository import (
    SqlDbStudyFileRepository,
)
from mtbls.infrastructure.repositories.user.db.user_read_repository import (
    SqlDbUserReadRepository,
)
from mtbls.infrastructure.repositories.user.db.user_write_repository import (
    SqlDbUserWriteRepository,
)


class RepositoriesContainer(containers.DeclarativeContainer):
    config = providers.Configuration()
    entity_mapper: EntityMapper = providers.Singleton(EntityMapper)

    alias_generator: AliasGenerator = providers.Singleton(
        DbTableAliasGeneratorImpl, entity_mapper
    )

    gateways = providers.DependenciesContainer()
    services = providers.DependenciesContainer()
    study_read_repository: StudyReadRepository = providers.Singleton(
        SqlDbStudyReadRepository,
        entity_mapper=entity_mapper,
        alias_generator=alias_generator,
        database_client=gateways.database_client,
    )

    user_write_repository: UserWriteRepository = providers.Singleton(
        SqlDbUserWriteRepository,
        entity_mapper=entity_mapper,
        alias_generator=alias_generator,
        database_client=gateways.database_client,
    )

    user_read_repository: UserReadRepository = providers.Singleton(
        SqlDbUserReadRepository,
        entity_mapper=entity_mapper,
        alias_generator=alias_generator,
        database_client=gateways.database_client,
    )

    study_file_repository: StudyFileRepository = providers.Singleton(
        SqlDbStudyFileRepository,
        entity_mapper=entity_mapper,
        alias_generator=alias_generator,
        database_client=gateways.database_client,
    )

    folder_manager = providers.Singleton(
        StudyFolderManager, config=config.repositories.study_folders
    )

    internal_files_object_repository: FileObjectWriteRepository = providers.Singleton(
        FileSystemObjectWriteRepository,
        folder_manager=folder_manager,
        study_bucket=StudyBucket.INTERNAL_FILES,
        observer=study_file_repository,
    )
    audit_files_object_repository: FileObjectWriteRepository = providers.Singleton(
        FileSystemObjectWriteRepository,
        folder_manager=folder_manager,
        study_bucket=StudyBucket.AUDIT_FILES,
        observer=study_file_repository,
    )
    metadata_files_object_repository: FileObjectWriteRepository = providers.Singleton(
        FileSystemObjectWriteRepository,
        folder_manager=folder_manager,
        study_bucket=StudyBucket.PRIVATE_METADATA_FILES,
        observer=study_file_repository,
    )
