from logging import config as logging_config

from dependency_injector import containers, providers

from mtbls.application.context.request_tracker import RequestTracker
from mtbls.application.services.interfaces.http_client import HttpClient
from mtbls.application.services.interfaces.policy_service import PolicyService
from mtbls.application.services.interfaces.repositories.file_object.file_object_write_repository import (  # noqa: E501
    FileObjectWriteRepository,
)
from mtbls.application.services.interfaces.repositories.study.study_read_repository import (  # noqa: E501
    StudyReadRepository,
)
from mtbls.application.services.interfaces.repositories.user.user_read_repository import (  # noqa: E501
    UserReadRepository,
)
from mtbls.domain.shared.repository.study_bucket import StudyBucket
from mtbls.infrastructure.http_client.httpx.httpx_client import HttpxClient
from mtbls.infrastructure.persistence.db.alias_generator import AliasGenerator
from mtbls.infrastructure.persistence.db.db_client import DatabaseClient
from mtbls.infrastructure.persistence.db.model.alias_generator import (
    DbTableAliasGeneratorImpl,
)
from mtbls.infrastructure.persistence.db.model.entity_mapper import EntityMapper
from mtbls.infrastructure.persistence.db.postgresql.db_client_impl import (
    DatabaseClientImpl,
)
from mtbls.infrastructure.policy_service.opa.opa_service import OpaPolicyService
from mtbls.infrastructure.repositories.file_object.default.nfs.file_object_write_repository import (  # noqa: E501
    FileSystemObjectWriteRepository,
)
from mtbls.infrastructure.repositories.file_object.default.nfs.study_folder_manager import (  # noqa: E501
    StudyFolderManager,
)
from mtbls.infrastructure.repositories.study.db.study_read_repository import (
    SqlDbStudyReadRepository,
)
from mtbls.infrastructure.repositories.user.db.user_read_repository import (
    SqlDbUserReadRepository,
)


class MtblsCliCoreContainer(containers.DeclarativeContainer):
    config = providers.Configuration()

    logging_config = providers.Resource(
        logging_config.dictConfig,
        config=config.run.cli.logging,
    )


class GatewaysContainer(containers.DeclarativeContainer):
    config = providers.Configuration()
    runtime_config = providers.Configuration()
    database_client: DatabaseClient = providers.Singleton(
        DatabaseClientImpl,
        db_connection=config.database.postgresql.connection,
        db_pool_size=runtime_config.db_pool_size,
    )
    http_client: HttpClient = providers.Singleton(
        HttpxClient, max_timeount_in_seconds=60
    )


class RepositoriesContainer(containers.DeclarativeContainer):
    config = providers.Configuration()
    entity_mapper: EntityMapper = providers.Singleton(EntityMapper)

    alias_generator: AliasGenerator = providers.Singleton(
        DbTableAliasGeneratorImpl, entity_mapper
    )

    gateways = providers.DependenciesContainer()
    study_read_repository: StudyReadRepository = providers.Singleton(
        SqlDbStudyReadRepository,
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
    folder_manager: StudyFolderManager = providers.Singleton(
        StudyFolderManager, config=config.repositories.study_folders
    )

    internal_files_object_repository: FileObjectWriteRepository = providers.Singleton(
        FileSystemObjectWriteRepository,
        folder_manager=folder_manager,
        study_bucket=StudyBucket.INTERNAL_FILES,
        http_client=gateways.http_client,
        observer=None,
    )


class MtblsCliServicesContainer(containers.DeclarativeContainer):
    config = providers.Configuration()
    gateways = providers.DependenciesContainer()
    policy_service: PolicyService = providers.Singleton(
        OpaPolicyService,
        http_client=gateways.http_client,
        config=config.policy_service.opa,
    )

    request_tracker: RequestTracker = providers.Singleton(RequestTracker)


class MtblsCliApplicationContainer(containers.DeclarativeContainer):
    config = providers.Configuration()
    secrets = providers.Configuration()
    core = providers.Container(
        MtblsCliCoreContainer,
        config=config,
    )

    gateways = providers.Container(
        GatewaysContainer, config=config.gateways, runtime_config={"db_pool_size": 0}
    )

    services = providers.Container(
        MtblsCliServicesContainer, config=config.services, gateways=gateways
    )

    repositories = providers.Container(
        RepositoriesContainer, config=config, gateways=gateways
    )
