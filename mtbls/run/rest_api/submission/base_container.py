from dependency_injector import containers, providers

from mtbls.application.services.interfaces.async_task.conection import PubSubConnection
from mtbls.application.services.interfaces.http_client import HttpClient
from mtbls.application.services.interfaces.repositories.file_object.file_object_write_repository import (  # noqa: E501
    FileObjectWriteRepository,
)
from mtbls.application.services.interfaces.repositories.statistic.statistic_read_repository import (  # noqa: E501
    StatisticReadRepository,
)
from mtbls.application.services.interfaces.repositories.study.study_read_repository import (  # noqa: E501
    StudyReadRepository,
)
from mtbls.application.services.interfaces.repositories.study.study_write_repository import (  # noqa: E501
    StudyWriteRepository,
)
from mtbls.application.services.interfaces.repositories.user.user_read_repository import (  # noqa: E501
    UserReadRepository,
)
from mtbls.application.services.interfaces.repositories.user.user_write_repository import (  # noqa: E501
    UserWriteRepository,
)
from mtbls.application.services.interfaces.search_port import SearchPort
from mtbls.domain.shared.repository.study_bucket import StudyBucket
from mtbls.infrastructure.http_client.httpx.httpx_client import HttpxClient
from mtbls.infrastructure.persistence.db.alias_generator import AliasGenerator
from mtbls.infrastructure.persistence.db.db_client import DatabaseClient
from mtbls.infrastructure.persistence.db.model.alias_generator import (
    DbTableAliasGeneratorImpl,
)
from mtbls.infrastructure.persistence.db.model.entity_mapper import EntityMapper

# from mtbls.infrastructure.persistence.db.mongodb.config import (
#     MongoDbConnection,
# )
from mtbls.infrastructure.persistence.db.postgresql.db_client_impl import (
    DatabaseClientImpl,
)
from mtbls.infrastructure.pub_sub.connection.redis import RedisConnectionProvider
from mtbls.infrastructure.repositories.file_object.default.nfs.file_object_write_repository import (  # noqa: E501
    FileSystemObjectWriteRepository,
)
from mtbls.infrastructure.repositories.file_object.default.nfs.study_folder_manager import (  # noqa: E501
    StudyFolderManager,
)
from mtbls.infrastructure.repositories.statistic.sql_db.statistic_read_repository import (  # noqa: E501
    SqlDbStatisticReadRepository,
)  # noqa: E501
from mtbls.infrastructure.repositories.study.db.study_read_repository import (
    SqlDbStudyReadRepository,
)
from mtbls.infrastructure.repositories.study.db.study_write_repository import (
    SqlDbStudyWriteRepository,
)
from mtbls.infrastructure.repositories.user.db.user_read_repository import (
    SqlDbUserReadRepository,
)
from mtbls.infrastructure.repositories.user.db.user_write_repository import (
    SqlDbUserWriteRepository,
)
from mtbls.infrastructure.search.es.es_client import ElasticsearchClient, ElasticsearchClientConfig
from mtbls.infrastructure.search.es.study.es_study_search_gateway import ElasticsearchStudyGateway


class GatewaysContainer(containers.DeclarativeContainer):
    config = providers.Configuration()
    runtime_config = providers.Configuration()
    database_client: DatabaseClient = providers.Singleton(
        DatabaseClientImpl,
        db_connection=config.database.postgresql.connection,
        db_pool_size=runtime_config.db_pool_size,
    )
    
    elasticsearch_client: ElasticsearchClient  = providers.Singleton(
        ElasticsearchClient,
        config=providers.Factory(
            ElasticsearchClientConfig,
            hosts=config.search.elasticsearch.connection.hosts,
            api_key=config.search.elasticsearch.connection.api_key,
            request_timeout_in_seconds=config.search.elasticsearch.connection.request_timeout_in_seconds.as_float(),
            verify_certs=config.search.elasticsearch.connection.verify_certs.as_bool(),
        )
    )
    elasticsearch_study_gateway: SearchPort = providers.Singleton(
        ElasticsearchStudyGateway,
        config=config.search.elasticsearch, # currently we rely on default config values. If we ever want to change them we can use the example above.
    )
    

    # mongodb_connection: MongoDbConnection = providers.Resource(
    #     create_config_from_dict,
    #     MongoDbConnection,
    #     config.database.mongodb.connection,
    # )

    pub_sub_broker: PubSubConnection = providers.Singleton(
        RedisConnectionProvider,
        config=config.cache.redis.connection,
    )

    pub_sub_backend: PubSubConnection = pub_sub_broker

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
    services = providers.DependenciesContainer()
    study_read_repository: StudyReadRepository = providers.Singleton(
        SqlDbStudyReadRepository,
        entity_mapper=entity_mapper,
        alias_generator=alias_generator,
        database_client=gateways.database_client,
    )
    study_write_repository: StudyWriteRepository = providers.Singleton(
        SqlDbStudyWriteRepository,
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

    statistic_read_repository: StatisticReadRepository = providers.Singleton(
        SqlDbStatisticReadRepository,
        entity_mapper=entity_mapper,
        alias_generator=alias_generator,
        database_client=gateways.database_client,
    )

    # study_file_repository: StudyFileRepository = providers.Singleton(
    #     MongoDbStudyFileRepository,
    #     connection=gateways.mongodb_connection,
    #     study_objects_collection_name="study_files",
    # )

    # study_file_repository: StudyFileRepository = providers.Singleton(
    #     SqlDbStudyFileRepository,
    #     entity_mapper=entity_mapper,
    #     alias_generator=alias_generator,
    #     database_client=gateways.database_client,
    # )

    folder_manager = providers.Singleton(
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

    # investigation_object_repository: InvestigationObjectRepository = ( # noqa: E501
    #     providers.Singleton(
    #         MongoDbInvestigationObjectRepository,
    #         connection=gateways.mongodb_connection,
    #         collection_name="investigation_files",
    #         study_bucket=StudyBucket.PRIVATE_METADATA_FILES,
    #         observer=study_file_repository,
    #     )
    # )
    # isa_table_object_repository: IsaTableObjectRepository = providers.Singleton( # noqa: E501
    #     MongoDbIsaTableObjectRepository,
    #     connection=gateways.mongodb_connection,
    #     collection_name="isa_table_files",
    #     study_bucket=StudyBucket.PRIVATE_METADATA_FILES,
    #     observer=study_file_repository,
    # )
    # isa_table_row_object_repository: IsaTableRowObjectRepository = providers.Singleton( # noqa: E501
    #     MongoDbIsaTableRowObjectRepository,
    #     connection=gateways.mongodb_connection,
    #     collection_name="isa_table_rows",
    #     study_bucket=StudyBucket.PRIVATE_METADATA_FILES,
    # )
    # validation_override_repository: ValidationOverrideRepository = providers.Singleton( # noqa: E501
    #     MongoDbValidationOverrideRepository,
    #     connection=gateways.mongodb_connection,
    #     collection_name="validation_overrides",
    #     observer=study_file_repository,
    # )
    # validation_report_repository: ValidationReportRepository = providers.Singleton( # noqa: E501
    #     MongoDbValidationReportRepository,
    #     connection=gateways.mongodb_connection,
    #     study_bucket=StudyBucket.INTERNAL_FILES,
    #     collection_name="validation_reports",
    #     validation_history_object_key="validation-history",
    #     observer=study_file_repository,
    # )
