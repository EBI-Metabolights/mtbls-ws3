import os
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
from mtbls.application.services.interfaces.health_check_service import (
    SystemHealthCheckService,
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
from mtbls.infrastructure.auth.standalone.standalone_authentication_service import (
    AuthenticationServiceImpl,
)
from mtbls.infrastructure.auth.standalone.standalone_authorization_service import (
    AuthorizationServiceImpl,
)
from mtbls.infrastructure.cache.redis.redis_impl import RedisCacheImpl
from mtbls.infrastructure.policy_service.opa.opa_service import OpaPolicyService
from mtbls.infrastructure.pub_sub.celery.celery_impl import CeleryAsyncTaskService
from mtbls.infrastructure.study_metadata_service.nfs.nfs_study_metadata_service_factory import (
    FileObjectStudyMetadataServiceFactory,
)
from mtbls.infrastructure.system_health_check_service.remote.remote_system_health_check_service import (
    RemoteSystemHealthCheckService,
)
from mtbls.infrastructure.validation_override_service.nfs.validation_override_service import (
    FileSystemValidationOverrideService,
)
from mtbls.infrastructure.validation_report_service.nfs.validation_report_service import (
    FileSystemValidationReportService,
)
from mtbls.presentation.rest_api.core.core_router import set_oauth2_redirect_endpoint
from mtbls.presentation.rest_api.core.models import ApiServerConfiguration
from mtbls.presentation.rest_api.groups.auth.v1.routers.oauth2_scheme import (
    OAuth2ClientCredentials,
    get_oauth2_scheme,
)
from mtbls.presentation.rest_api.groups.submission.v1.routers.dataset_license.schemas import (
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
    core = providers.DependenciesContainer()

    repositories = providers.DependenciesContainer()
    gateways = providers.DependenciesContainer()
    cache_config = providers.Configuration()

    policy_service: PolicyService = providers.Singleton(
        OpaPolicyService,
        config.policy_service.opa,
    )

    system_health_check_service: SystemHealthCheckService = providers.Singleton(
        RemoteSystemHealthCheckService,
        config.system_health_check.remote,
    )

    async_task_service: AsyncTaskService = providers.Singleton(
        CeleryAsyncTaskService,
        broker=gateways.pub_sub_broker,
        backend=gateways.pub_sub_backend,
        app_name="default",
        queue_names=["common", "validation", "datamover", "compute", ""],
        async_task_registry=core.async_task_registry,
    )

    cache_service: CacheService = providers.Singleton(
        RedisCacheImpl,
        config=cache_config,
    )
    oauth2_scheme: OAuth2ClientCredentials = providers.Resource(get_oauth2_scheme)

    authentication_service: AuthenticationService = providers.Singleton(
        AuthenticationServiceImpl,
        config=config.authentication.standalone,
        cache_service=cache_service,
        user_read_repository=repositories.user_read_repository,
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

    # study_metadata_service_factory: StudyMetadataServiceFactory = providers.Singleton(
    #     MongoDbStudyMetadataServiceFactory,
    #     study_file_repository=repositories.study_file_repository,
    #     investigation_object_repository=repositories.investigation_object_repository,
    #     isa_table_object_repository=repositories.isa_table_object_repository,
    #     isa_table_row_object_repository=repositories.isa_table_row_object_repository,
    #     study_read_repository=repositories.study_read_repository,
    #     temp_path="/tmp/study-metadata-service",
    # )

    study_metadata_service_factory: StudyMetadataServiceFactory = providers.Singleton(
        FileObjectStudyMetadataServiceFactory,
        study_file_repository=None,
        metadata_files_object_repository=repositories.metadata_files_object_repository,
        audit_files_object_repository=repositories.audit_files_object_repository,
        internal_files_object_repository=repositories.internal_files_object_repository,
        study_read_repository=repositories.study_read_repository,
        temp_path="/tmp/study-metadata-service",
    )

    # validation_override_service: ValidationOverrideService = providers.Singleton(
    #     MongoDbValidationOverrideService,
    #     validation_override_repository=repositories.validation_override_repository,
    #     policy_service=policy_service,
    #     validation_overrides_object_key="validation-overrides/validation-overrides.json",
    # )
    # validation_report_service: ValidationReportService = providers.Singleton(
    #     MongoDbValidationReportService,
    #     validation_report_repository=repositories.validation_report_repository,
    #     validation_history_object_key="validation-history",
    # )


CONFIG_FILE = os.environ.get("CONFIG_FILE", "submission-config.yaml")
CONFIG_SECRETS_FILE = os.environ.get(
    "CONFIG_SECRETS_FILE",
    ".submission-config-secrets/.secrets.yaml",
)
print("Config file path:", CONFIG_FILE)
print("Config file secrets path:", CONFIG_SECRETS_FILE)

print("Submission server config file path", CONFIG_FILE)


class Ws3ApplicationContainer(containers.DeclarativeContainer):
    config = providers.Configuration(yaml_files=[CONFIG_FILE])
    secrets = providers.Configuration(yaml_files=[CONFIG_SECRETS_FILE])
    core = providers.Container(
        Ws3CoreContainer,
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
        Ws3ServicesContainer,
        config=config.services,
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
