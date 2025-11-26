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
)
from mtbls.application.services.interfaces.auth.authorization_service import (
    AuthorizationService,
)
from mtbls.application.services.interfaces.cache_service import CacheService
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
from mtbls.infrastructure.auth.mtbls_ws2.mtbls_ws2_authentication_proxy import (
    MtblsWs2AuthenticationProxy,
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
from mtbls.infrastructure.study_metadata_service.nfs.nfs_study_metadata_service_factory import (  # noqa: E501
    FileObjectStudyMetadataServiceFactory,
)
from mtbls.infrastructure.validation_override_service.nfs.validation_override_service import (  # noqa: E501
    FileSystemValidationOverrideService,
)
from mtbls.infrastructure.validation_report_service.nfs.validation_report_service import (  # noqa: E501
    FileSystemValidationReportService,
)
from mtbls.presentation.rest_api.groups.auth.v1.routers.oauth2_scheme import (
    OAuth2ClientCredentials,
    get_oauth2_scheme,
)
from mtbls.run.config import ModuleConfiguration
from mtbls.run.rest_api.submission.base_container import (
    GatewaysContainer,
    RepositoriesContainer,
)


class Ws3WorkerCoreContainer(containers.DeclarativeContainer):
    config = providers.Configuration()

    logging_config = providers.Resource(
        logging_config.dictConfig,
        config=config.run.common_worker.logging,
    )

    async_task_registry = providers.Resource(get_async_task_registry)


class Ws3WorkerServicesContainer(containers.DeclarativeContainer):
    config = providers.Configuration()
    core = providers.DependenciesContainer()

    repositories = providers.DependenciesContainer()
    gateways = providers.DependenciesContainer()
    cache_config = providers.Configuration()

    cache_service: CacheService = providers.Factory(
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
    async_task_service: AsyncTaskService = providers.Singleton(
        CeleryAsyncTaskService,
        broker=gateways.pub_sub_broker,
        backend=gateways.pub_sub_backend,
        app_name="default",
        queue_names=["common", "validation", "datamover", "compute", ""],
        async_task_registry=core.async_task_registry,
    )
    oauth2_scheme: OAuth2ClientCredentials = providers.Resource(get_oauth2_scheme)

    authentication_service: AuthenticationService = providers.Singleton(
        MtblsWs2AuthenticationProxy,
        config=config.authentication.mtbls_ws2,
        cache_service=cache_service,
        user_read_repository=repositories.user_read_repository,
        http_client=gateways.http_client,
    )
    request_tracker: RequestTracker = providers.Singleton(RequestTracker)
    authorization_service: AuthorizationService = providers.Singleton(
        AuthorizationServiceImpl,
        user_read_repository=repositories.user_read_repository,
        study_read_repository=repositories.study_read_repository,
    )

    validation_override_service: ValidationOverrideService = providers.Singleton(
        FileSystemValidationOverrideService,
        file_object_repository=repositories.internal_files_object_repository,
        policy_service=policy_service,
        validation_overrides_object_key="validation-overrides/validation-overrides.json",
        temp_directory="/tmp/validation-overrides-tmp",
    )
    validation_report_service: ValidationReportService = providers.Singleton(
        FileSystemValidationReportService,
        file_object_repository=repositories.internal_files_object_repository,
        validation_history_object_key="validation-history",
    )

    study_metadata_service_factory: StudyMetadataServiceFactory = providers.Singleton(
        FileObjectStudyMetadataServiceFactory,
        study_file_repository=None,
        metadata_files_object_repository=repositories.metadata_files_object_repository,
        audit_files_object_repository=repositories.audit_files_object_repository,
        internal_files_object_repository=repositories.internal_files_object_repository,
        study_read_repository=repositories.study_read_repository,
        user_read_repository=repositories.user_read_repository,
        temp_path="/tmp/study-metadata-service",
    )


class Ws3WorkerApplicationContainer(containers.DeclarativeContainer):
    config = providers.Configuration()
    secrets = providers.Configuration()

    core = providers.Container(
        Ws3WorkerCoreContainer,
        config=config,
    )

    gateways = providers.Container(
        GatewaysContainer,
        config=config.gateways,
    )

    repositories = providers.Container(
        RepositoriesContainer,
        config=config,
        gateways=gateways,
    )
    services = providers.Container(
        Ws3WorkerServicesContainer,
        config=config.services,
        cache_config=config.gateways.cache.redis.connection,
        core=core,
        repositories=repositories,
        gateways=gateways,
    )

    module_config: ModuleConfiguration = providers.Resource(
        create_config_from_dict,
        ModuleConfiguration,
        config.run.common_worker.module_config,
    )
