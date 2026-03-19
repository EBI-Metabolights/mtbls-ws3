from logging import config as logging_config

from dependency_injector import containers, providers

from mtbls.application.context.request_tracker import RequestTracker
from mtbls.application.services.interfaces.async_task.async_task_service import (
    AsyncTaskService,
)
from mtbls.application.services.interfaces.async_task.utils import (
    get_async_task_registry,
)
from mtbls.application.services.interfaces.auth.authentication_service import (
    AuthenticationService,
    UserProfileService,
)
from mtbls.application.services.interfaces.auth.authorization_service import (
    AuthorizationService,
)
from mtbls.application.services.interfaces.cache_service import CacheService
from mtbls.application.services.interfaces.health_check_service import (
    SystemHealthCheckService,
)
from mtbls.application.services.interfaces.ontology_search_service import (
    OntologySearchService,
)
from mtbls.application.services.interfaces.policy_service import PolicyService
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
from mtbls.infrastructure.auth.keycloak.keycloak_authentication import (
    KeycloakAuthenticationService,
)
from mtbls.infrastructure.auth.mtbls_ws2.mtbls_ws2_authentication_proxy import (
    MtblsWs2AuthenticationProxy,
)
from mtbls.infrastructure.auth.standalone.standalone_authentication_service import (
    AuthenticationServiceImpl,
)
from mtbls.infrastructure.auth.standalone.standalone_authorization_service import (
    AuthorizationServiceImpl,
)
from mtbls.infrastructure.caching.redis.redis_impl import RedisCacheImpl
from mtbls.infrastructure.ontology_search.ols.ols_search_service import (
    OlsOntologySearchService,
)
from mtbls.infrastructure.policy_service.opa.opa_service import OpaPolicyService
from mtbls.infrastructure.pub_sub.celery.celery_impl import CeleryAsyncTaskService
from mtbls.infrastructure.study_metadata_service.mongodb.mongodb_study_metadata_service_factory import (  # noqa: E501
    MongoDbStudyMetadataServiceFactory,
)
from mtbls.infrastructure.study_metadata_service.nfs.nfs_study_metadata_service_factory import (  # noqa: E501
    FileObjectStudyMetadataServiceFactory,
)
from mtbls.infrastructure.system_health_check_service.remote.remote_system_health_check_service import (  # noqa: E501
    RemoteSystemHealthCheckService,
)
from mtbls.infrastructure.system_health_check_service.standalone.standalone_system_health_check_service import (
    StandaloneSystemHealthCheckService,
)
from mtbls.infrastructure.validation_override_service.mongodb.validation_override_service import (  # noqa: E501
    MongoDbValidationOverrideService,
)
from mtbls.infrastructure.validation_override_service.nfs.validation_override_service import (  # noqa: E501
    FileSystemValidationOverrideService,
)
from mtbls.infrastructure.validation_report_service.mongodb.validation_report_service import (  # noqa: E501
    MongoDbValidationReportService,
)
from mtbls.infrastructure.validation_report_service.nfs.validation_report_service import (  # noqa: E501
    FileSystemValidationReportService,
)
from mtbls.presentation.rest_api.core.core_router import set_oauth2_redirect_endpoint
from mtbls.presentation.rest_api.core.models import ApiServerConfiguration
from mtbls.presentation.rest_api.groups.auth.v1.routers.oauth2_scheme import (
    OAuth2ClientCredentials,
    get_oauth2_scheme,
)
from mtbls.presentation.rest_api.groups.submission.v1.routers.dataset_license.schemas import (  # noqa: E501
    DatasetLicenseInfoConfiguration,
)
from mtbls.run.config import ModuleConfiguration
from mtbls.run.rest_api.submission.base_container import (
    GatewaysContainer,
    RepositoriesContainer,
)


class Ws3CoreContainer(containers.DeclarativeContainer):
    config = providers.Configuration()

    logging_config = providers.Resource(
        logging_config.dictConfig,
        config=config.run.submission.logging,
    )
    async_task_registry = providers.Resource(get_async_task_registry)


class Ws3ServicesContainer(containers.DeclarativeContainer):
    config = providers.Configuration()
    repository_config = providers.Configuration()
    core = providers.DependenciesContainer()

    repositories = providers.DependenciesContainer()
    gateways = providers.DependenciesContainer()
    cache_config = providers.Configuration()

    cache_service: CacheService = providers.Singleton(
        RedisCacheImpl,
        config=cache_config,
    )
    policy_service: PolicyService = providers.Singleton(
        OpaPolicyService,
        http_client=gateways.http_client,
        config=config.policy_service.opa,
    )

    ontology_search_service: OntologySearchService = providers.Singleton(
        OlsOntologySearchService,
        http_client=gateways.http_client,
        cache_service=cache_service,
        config=config.ontology_search_service.ols,
    )

    system_health_check_service: SystemHealthCheckService = providers.Selector(
        selector=config.system_health_check.active_health_check_service,
        remote=providers.Singleton(
            RemoteSystemHealthCheckService,
            config.system_health_check.remote,
            http_client=gateways.http_client,
        ),
        standalone=providers.Singleton(
            StandaloneSystemHealthCheckService,
            config.system_health_check.standalone,
            http_client=gateways.http_client,
        ),
    )

    async_task_service: AsyncTaskService = providers.Singleton(
        CeleryAsyncTaskService,
        broker=gateways.pub_sub_broker,
        backend=gateways.pub_sub_backend,
        app_name="default",
        queue_names=["common", "validation", "datamover", "compute", ""],
        async_task_registry=core.async_task_registry,
    )

    oauth2_scheme: OAuth2ClientCredentials = providers.Resource(get_oauth2_scheme)
    user_profile_service: UserProfileService = providers.Singleton(
        KeycloakAuthenticationService,
        config=config.authentication.keycloak,
        cache_service=cache_service,
    )
    authentication_service: AuthenticationService = providers.Selector(
        config.authentication.active_authentication_service,
        standalone=providers.Singleton(
            AuthenticationServiceImpl,
            config=config.authentication.standalone,
            cache_service=cache_service,
            user_read_repository=repositories.user_read_repository,
        ),
        mtbls_ws2=providers.Singleton(
            MtblsWs2AuthenticationProxy,
            config=config.authentication.mtbls_ws2,
            cache_service=cache_service,
            http_client=gateways.http_client,
            user_read_repository=repositories.user_read_repository,
        ),
        keycloak=providers.Singleton(
            KeycloakAuthenticationService,
            config=config.authentication.keycloak,
            cache_service=cache_service,
        ),
    )
    request_tracker: RequestTracker = providers.Singleton(RequestTracker)
    authorization_service: AuthorizationService = providers.Singleton(
        AuthorizationServiceImpl,
        user_read_repository=repositories.user_read_repository,
        study_read_repository=repositories.study_read_repository,
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


class Ws3ApplicationContainer(containers.DeclarativeContainer):
    config = providers.Configuration()
    secrets = providers.Configuration()
    core = providers.Container(
        Ws3CoreContainer,
        config=config,
    )

    gateways = providers.Container(
        GatewaysContainer, config=config.gateways, runtime_config={"db_pool_size": 4}
    )

    repositories = providers.Container(
        RepositoriesContainer, config=config, gateways=gateways
    )

    services = providers.Container(
        Ws3ServicesContainer,
        config=config.services,
        repository_config=config.repositories,
        cache_config=config.gateways.cache.redis.connection,
        core=core,
        repositories=repositories,
        gateways=gateways,
    )

    module_config: ModuleConfiguration = providers.Resource(
        create_config_from_dict,
        ModuleConfiguration,
        config.run.submission.module_config,
    )

    default_dataset_license_config: DatasetLicenseInfoConfiguration = (
        providers.Resource(
            create_config_from_dict,
            DatasetLicenseInfoConfiguration,
            config.run.submission.default_dataset_license,
        )
    )

    api_server_config: ApiServerConfiguration = providers.Resource(
        create_config_from_dict,
        ApiServerConfiguration,
        config.run.submission.api_server_config,
    )

    oauth2_endpoint = providers.Resource(
        set_oauth2_redirect_endpoint, api_server_config
    )
    mhd_configuration: MhdConfiguration = providers.Resource(
        create_config_from_dict,
        MhdConfiguration,
        config.run.submission.mhd,
    )
