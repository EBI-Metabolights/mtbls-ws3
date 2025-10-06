import logging

from mtbls.application.services.interfaces.repositories.file_object.file_object_observer import (  # noqa: E501
    FileObjectObserver,
)
from mtbls.application.services.interfaces.repositories.file_object.validation.validation_override_repository import (  # noqa: E501
    ValidationOverrideRepository,
)
from mtbls.domain.entities.base_entity import BaseEntity
from mtbls.domain.entities.validation_override import (
    ValidationOverrideFileObject,
)
from mtbls.domain.shared.repository.study_bucket import StudyBucket
from mtbls.infrastructure.persistence.db.mongodb.config import (
    MongoDbConnection,
)
from mtbls.infrastructure.repositories.default.mongodb.default_write_repository import (
    MongoDbDefaultWriteRepository,
)

logger = logging.getLogger(__name__)


class MongoDbValidationOverrideRepository(
    ValidationOverrideRepository,
    MongoDbDefaultWriteRepository[
        ValidationOverrideFileObject, ValidationOverrideFileObject, str
    ],
):
    def __init__(
        self,
        connection: MongoDbConnection,
        collection_name: str = "validation_overrides",
        output_entity_class: type[BaseEntity] = ValidationOverrideFileObject,
        study_bucket: StudyBucket = StudyBucket.INTERNAL_FILES,
        observer: FileObjectObserver = None,
    ):
        super(ValidationOverrideRepository, self).__init__(
            study_bucket=study_bucket, observers=[observer]
        )
        super(MongoDbDefaultWriteRepository, self).__init__(
            connection=connection,
            collection_name=collection_name,
            output_entity_class=output_entity_class,
        )
