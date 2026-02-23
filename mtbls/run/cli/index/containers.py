from logging import config as logging_config

from dependency_injector import containers, providers

from mtbls.application.context.request_tracker import RequestTracker
from mtbls.application.services.interfaces.data_index_client import DataIndexClient
from mtbls.application.services.interfaces.http_client import HttpClient
from mtbls.application.services.interfaces.repositories.file_object.file_object_write_repository import (  # noqa: E501
    FileObjectWriteRepository,
)
from mtbls.application.services.interfaces.repositories.study.study_read_repository import (  # noqa: E501
    StudyReadRepository,
)
from mtbls.application.services.interfaces.repositories.user.user_read_repository import (  # noqa: E501
    UserReadRepository,
)
from mtbls.application.services.interfaces.study_metadata_service_factory import (
    StudyMetadataServiceFactory,
)
from mtbls.domain.domain_services.configuration_generator import create_config_from_dict
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
from mtbls.infrastructure.search.es.es_client import (
    ElasticsearchClient,
    ElasticsearchClientConfig,
)
from mtbls.infrastructure.study_metadata_service.mongodb.mongodb_study_metadata_service_factory import (  # noqa: E501
    MongoDbStudyMetadataServiceFactory,
)
from mtbls.infrastructure.study_metadata_service.nfs.nfs_study_metadata_service_factory import (  # noqa: E501
    FileObjectStudyMetadataServiceFactory,
)


class CoreContainer(containers.DeclarativeContainer):
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
    elastic_config: ElasticsearchClientConfig = providers.Resource(
        create_config_from_dict,
        ElasticsearchClientConfig,
        config.database.elasticsearch.connection,
    )
    data_index_client: DataIndexClient = providers.Singleton(
        ElasticsearchClient, config=elastic_config, auth_method="basic_auth"
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

    index_cache_files_object_repository: FileObjectWriteRepository = (
        providers.Singleton(
            FileSystemObjectWriteRepository,
            folder_manager=folder_manager,
            study_bucket=StudyBucket.INDICES_CACHE_FILES,
            http_client=gateways.http_client,
            observer=None,
        )
    )
    metadata_files_object_repository: FileObjectWriteRepository = providers.Singleton(
        FileSystemObjectWriteRepository,
        folder_manager=folder_manager,
        study_bucket=StudyBucket.PRIVATE_METADATA_FILES,
        http_client=gateways.http_client,
        observer=None,
    )
    audit_files_object_repository: FileObjectWriteRepository = providers.Singleton(
        FileSystemObjectWriteRepository,
        folder_manager=folder_manager,
        study_bucket=StudyBucket.AUDIT_FILES,
        http_client=gateways.http_client,
        observer=None,
    )
    internal_files_object_repository: FileObjectWriteRepository = providers.Singleton(
        FileSystemObjectWriteRepository,
        folder_manager=folder_manager,
        study_bucket=StudyBucket.INTERNAL_FILES,
        http_client=gateways.http_client,
        observer=None,
    )


class ServicesContainer(containers.DeclarativeContainer):
    config = providers.Configuration()
    repository_config = providers.Configuration()
    gateways = providers.DependenciesContainer()
    repositories = providers.DependenciesContainer()
    gateways = providers.DependenciesContainer()

    request_tracker: RequestTracker = providers.Singleton(RequestTracker)

    study_metadata_service_factory: StudyMetadataServiceFactory = providers.Selector(
        selector=repository_config.active_target_repository.study_metadata,
        mongodb=providers.Singleton(
            MongoDbStudyMetadataServiceFactory,
            study_file_repository=repositories.study_file_repository,
            investigation_object_repository=repositories.investigation_object_repository,
            isa_table_object_repository=repositories.isa_table_object_repository,
            isa_table_row_object_repository=repositories.isa_table_row_object_repository,
            study_read_repository=repositories.study_read_repository,
            user_read_repository=repositories.user_read_repository,
            temp_path="/tmp/study-metadata-service",
        ),
        nfs=providers.Singleton(
            FileObjectStudyMetadataServiceFactory,
            study_file_repository=None,
            metadata_files_object_repository=repositories.metadata_files_object_repository,
            audit_files_object_repository=repositories.audit_files_object_repository,
            internal_files_object_repository=repositories.internal_files_object_repository,
            study_read_repository=repositories.study_read_repository,
            user_read_repository=repositories.user_read_repository,
            temp_path="/tmp/study-metadata-service",
        ),
    )


class EsCliApplicationContainer(containers.DeclarativeContainer):
    config = providers.Configuration()
    secrets = providers.Configuration()
    core = providers.Container(
        CoreContainer,
        config=config,
    )

    gateways = providers.Container(
        GatewaysContainer, config=config.gateways, runtime_config={"db_pool_size": 0}
    )

    repositories = providers.Container(
        RepositoriesContainer, config=config, gateways=gateways
    )

    services = providers.Container(
        ServicesContainer,
        config=config.services,
        gateways=gateways,
        repositories=repositories,
        repository_config=config.repositories,
    )
