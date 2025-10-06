import logging

from mtbls.application.services.interfaces.repositories.file_object.validation.validation_override_repository import (  # noqa: E501
    ValidationOverrideRepository,
)
from mtbls.domain.entities.base_entity import BaseEntity
from mtbls.domain.entities.validation_override import (
    ValidationOverrideFileObject,
)
from mtbls.infrastructure.persistence.db.mongodb.config import (
    MongoDbConnection,
)
from mtbls.infrastructure.repositories.default.mongodb.default_write_repository import (
    MongoDbDefaultWriteRepository,
)

logger = logging.getLogger(__name__)


class FileSystemValidationOverrideRepository(
    MongoDbDefaultWriteRepository[
        ValidationOverrideFileObject, ValidationOverrideFileObject, str
    ],
    ValidationOverrideRepository,
):
    def __init__(
        self,
        connection: MongoDbConnection,
        collection_name: str = "validation_overrides",
        output_entity_class: type[BaseEntity] = ValidationOverrideFileObject,
    ):
        MongoDbDefaultWriteRepository.__init__(
            self,
            connection=connection,
            collection_name=collection_name,
            output_entity_class=output_entity_class,
        )

        ValidationOverrideRepository.__init__(self)
