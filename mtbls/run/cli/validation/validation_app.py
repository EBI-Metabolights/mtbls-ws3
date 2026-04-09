import logging

from mtbls.application.services.interfaces.auth.authentication_service import (
    UserProfileService,
)
from mtbls.application.services.interfaces.http_client import HttpClient
from mtbls.application.services.interfaces.ontology_search_service import (
    OntologySearchService,
)
from mtbls.application.services.interfaces.policy_service import PolicyService
from mtbls.application.services.interfaces.repositories.file_object.file_object_write_repository import (
    FileObjectWriteRepository,
)
from mtbls.application.services.interfaces.repositories.study.study_read_repository import (  # noqa: E501
    StudyReadRepository,
)
from mtbls.application.services.interfaces.repositories.user.user_read_repository import (
    UserReadRepository,
)
from mtbls.application.services.interfaces.study_metadata_service_factory import (
    StudyMetadataServiceFactory,
)
from mtbls.run.cli.validation.container import ValidationApplicationContainer
from mtbls.run.config_utils import set_application_configuration

logger = logging.getLogger(__name__)


class ValidationApp:
    def __init__(self, config_file: None | str, secrets_file: None | str):
        self.container: ValidationApplicationContainer = (
            ValidationApplicationContainer()
        )

        success = set_application_configuration(
            self.container, config_file, secrets_file
        )
        if not success:
            raise Exception("Application failed.")
        self.container.init_resources()

        self.study_read_repository: StudyReadRepository = (
            self.container.repositories.study_read_repository()
        )
        self.http_client: HttpClient = self.container.gateways.http_client()

        self.metadata_files_object_repository: FileObjectWriteRepository = (
            self.container.repositories.metadata_files_object_repository()
        )
        self.study_metadata_service_factory: StudyMetadataServiceFactory = (
            self.container.services.study_metadata_service_factory()
        )
        self.policy_service: PolicyService = self.container.services.policy_service()
        self.internal_files_object_repository: FileObjectWriteRepository = (
            self.container.repositories.internal_files_object_repository()
        )
        self.user_read_repository: UserReadRepository = (
            self.container.repositories.user_read_repository()
        )
        self.ontology_search_service: OntologySearchService = (
            self.container.services.ontology_search_service()
        )
        self.user_profile_service: UserProfileService = (
            self.container.services.user_profile_service()
        )
        self.mhd_config = self.container.mhd_configuration()
        self.private_metadata_files_root_path = self.container.config.repositories.study_folders.mounted_paths.private_metadata_files_root_path()
        self.db_connection = (
            self.container.config.gateways.database.postgresql.connection()
        )
        self.validation_override_service = (
            self.container.services.validation_override_service()
        )
        self.validation_report_service = (
            self.container.services.validation_report_service()
        )
        logger.info("Validation CLI container started")
