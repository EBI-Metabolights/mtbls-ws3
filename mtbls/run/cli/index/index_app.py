import logging

from mtbls.application.services.interfaces.http_client import HttpClient
from mtbls.application.services.interfaces.repositories.file_object.file_object_write_repository import (  # noqa: E501
    FileObjectWriteRepository,
)
from mtbls.application.services.interfaces.repositories.study.study_read_repository import (  # noqa: E501
    StudyReadRepository,
)
from mtbls.application.services.interfaces.search_index_management_gateway import (
    SearchIndexManagementGateway,
)
from mtbls.application.services.interfaces.study_metadata_service_factory import (
    StudyMetadataServiceFactory,
)
from mtbls.run.cli.index.containers import EsCliApplicationContainer
from mtbls.run.config_utils import set_application_configuration

logger = logging.getLogger(__name__)


class IndexApp:
    def __init__(self, config_file: str, secrets_file: str):
        self.container: EsCliApplicationContainer = EsCliApplicationContainer()

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
        self.index_cache_files_object_repository: FileObjectWriteRepository = (
            self.container.repositories.index_cache_files_object_repository()
        )
        self.metadata_files_object_repository: FileObjectWriteRepository = (
            self.container.repositories.metadata_files_object_repository()
        )
        self.study_metadata_service_factory: StudyMetadataServiceFactory = (
            self.container.services.study_metadata_service_factory()
        )
        self.search_index_management_gateway: SearchIndexManagementGateway = (
            self.container.gateways.search_index_management_gateway()
        )
        logger.info("CLI container started")
