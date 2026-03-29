from dependency_injector import containers, providers

from mtbls.application.context.request_tracker import RequestTracker
from mtbls.application.services.interfaces.cache_service import CacheService
from mtbls.application.services.interfaces.http_client import HttpClient
from mtbls.application.services.interfaces.ontology_search_service import (
    OntologySearchService,
)
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
from mtbls.application.services.interfaces.study_metadata_service_factory import (
    StudyMetadataServiceFactory,
)
from mtbls.application.services.interfaces.validation_override_service import (
    ValidationOverrideService,
)
from mtbls.application.services.interfaces.validation_report_service import (
    ValidationReportService,
)
from mtbls.domain.domain_services.configuration_generator import create_config_from_dict
from mtbls.domain.shared.mhd_configuration import MhdConfiguration
from mtbls.domain.shared.repository.study_bucket import StudyBucket
from mtbls.infrastructure.caching.redis.redis_impl import RedisCacheImpl
from mtbls.infrastructure.http_client.httpx.httpx_client import HttpxClient
from mtbls.infrastructure.ontology_search.ols.ols_search_service import (
    OlsOntologySearchService,
)
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
from mtbls.infrastructure.study_metadata_service.mongodb.mongodb_study_metadata_service_factory import (  # noqa: E501
    MongoDbStudyMetadataServiceFactory,
)
from mtbls.infrastructure.study_metadata_service.nfs.nfs_study_metadata_service_factory import (  # noqa: E501
    FileObjectStudyMetadataServiceFactory,
)
from mtbls.infrastructure.validation_override_service.mongodb.validation_override_service import (
    MongoDbValidationOverrideService,
)
from mtbls.infrastructure.validation_override_service.nfs.validation_override_service import (
    FileSystemValidationOverrideService,
)
from mtbls.infrastructure.validation_report_service.mongodb.validation_report_service import (
    MongoDbValidationReportService,
)
from mtbls.infrastructure.validation_report_service.nfs.validation_report_service import (
    FileSystemValidationReportService,
)
from mtbls.run.cli.logging_config import configure_cli_logging


class ValidationCoreContainer(containers.DeclarativeContainer):
    config = providers.Configuration()

    logging_config = providers.Resource(
        configure_cli_logging,
        config=config.run.cli.logging,
    )


class ValidationGatewaysContainer(containers.DeclarativeContainer):
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


class ValidationRepositoriesContainer(containers.DeclarativeContainer):
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
    audit_files_object_repository: FileObjectWriteRepository = providers.Singleton(
        FileSystemObjectWriteRepository,
        folder_manager=folder_manager,
        study_bucket=StudyBucket.AUDIT_FILES,
        http_client=gateways.http_client,
        observer=None,
    )
    metadata_files_object_repository: FileObjectWriteRepository = providers.Singleton(
        FileSystemObjectWriteRepository,
        folder_manager=folder_manager,
        study_bucket=StudyBucket.PRIVATE_METADATA_FILES,
        http_client=gateways.http_client,
        observer=None,
    )


class ValidationServicesContainer(containers.DeclarativeContainer):
    config = providers.Configuration()
    cache_config = providers.Configuration()
    repository_config = providers.Configuration()
    gateways = providers.DependenciesContainer()
    repositories = providers.DependenciesContainer()
    cache_service: CacheService = providers.Singleton(
        RedisCacheImpl,
        config=cache_config,
    )
    policy_service: PolicyService = providers.Singleton(
        OpaPolicyService,
        http_client=gateways.http_client,
        config=config.policy_service.opa,
    )

    request_tracker: RequestTracker = providers.Singleton(RequestTracker)
    policy_service: PolicyService = providers.Factory(
        OpaPolicyService,
        http_client=gateways.http_client,
        config=config.policy_service.opa,
    )
    study_metadata_service_factory: StudyMetadataServiceFactory = providers.Selector(
        selector=repository_config.active_target_repository.study_metadata,
        mongodb=providers.Singleton(
            MongoDbStudyMetadataServiceFactory,
            study_read_repository=repositories.study_read_repository,
            user_read_repository=repositories.user_read_repository,
            study_data_file_repository=None,
            investigation_object_repository=repositories.investigation_object_repository,
            isa_table_object_repository=repositories.isa_table_object_repository,
            isa_table_row_object_repository=repositories.isa_table_row_object_repository,
            temp_path="/tmp/study-metadata-service",
        ),
        nfs=providers.Singleton(
            FileObjectStudyMetadataServiceFactory,
            study_data_file_repository=None,
            metadata_files_object_repository=repositories.metadata_files_object_repository,
            audit_files_object_repository=repositories.audit_files_object_repository,
            internal_files_object_repository=repositories.internal_files_object_repository,
            study_read_repository=repositories.study_read_repository,
            user_read_repository=repositories.user_read_repository,
            temp_path="/tmp/study-metadata-service",
        ),
    )
    ontology_search_service: OntologySearchService = providers.Singleton(
        OlsOntologySearchService,
        http_client=gateways.http_client,
        cache_service=cache_service,
        config=config.ontology_search_service.ols,
    )

    validation_override_service: ValidationOverrideService = providers.Selector(
        selector=repository_config.active_target_repository.validation_overrides,
        mongodb=providers.Singleton(
            MongoDbValidationOverrideService,
            validation_override_repository=repositories.validation_override_repository,
            policy_service=policy_service,
            validation_overrides_object_key="validation-overrides/validation-overrides.json",  # noqa: E501
        ),
        nfs=providers.Singleton(
            FileSystemValidationOverrideService,
            file_object_repository=repositories.internal_files_object_repository,
            policy_service=policy_service,
            validation_overrides_object_key="validation-overrides/validation-overrides.json",
            temp_directory="/tmp/validation-overrides-tmp",
        ),
    )
    validation_report_service: ValidationReportService = providers.Selector(
        selector=repository_config.active_target_repository.validation_reports,
        mongodb=providers.Singleton(
            MongoDbValidationReportService,
            validation_report_repository=repositories.validation_report_repository,
            validation_history_object_key="validation-history",
        ),
        nfs=providers.Singleton(
            FileSystemValidationReportService,
            file_object_repository=repositories.internal_files_object_repository,
            validation_history_object_key="validation-history",
        ),
    )


class ValidationApplicationContainer(containers.DeclarativeContainer):
    config = providers.Configuration()
    secrets = providers.Configuration()
    core = providers.Container(
        ValidationCoreContainer,
        config=config,
    )

    gateways = providers.Container(
        ValidationGatewaysContainer,
        config=config.gateways,
        runtime_config={"db_pool_size": 0},
    )

    repositories = providers.Container(
        ValidationRepositoriesContainer, config=config, gateways=gateways
    )

    services = providers.Container(
        ValidationServicesContainer,
        config=config.services,
        gateways=gateways,
        repositories=repositories,
        repository_config=config.repositories,
        cache_config=config.gateways.cache.redis.connection,
    )

    mhd_configuration: MhdConfiguration = providers.Resource(
        create_config_from_dict,
        MhdConfiguration,
        config.run.common_worker.mhd,
    )
