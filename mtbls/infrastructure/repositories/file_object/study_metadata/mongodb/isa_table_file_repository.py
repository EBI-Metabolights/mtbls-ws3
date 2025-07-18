import logging

from mtbls.application.services.interfaces.repositories.file_object.file_object_observer import (
    FileObjectObserver,
)
from mtbls.application.services.interfaces.repositories.file_object.study_metadata.metadata_repository import (
    IsaTableObjectRepository,
)
from mtbls.domain.entities.base_entity import BaseEntity
from mtbls.domain.entities.isa_table import IsaTableFileObject
from mtbls.domain.shared.repository.study_bucket import StudyBucket
from mtbls.infrastructure.persistence.db.mongodb.config import (
    MongoDbConnection,
)
from mtbls.infrastructure.repositories.default.mongodb.default_write_repository import (
    MongoDbDefaultWriteRepository,
)

logger = logging.getLogger(__name__)


class MongoDbIsaTableObjectRepository(
    MongoDbDefaultWriteRepository[IsaTableFileObject, IsaTableFileObject, str],
    IsaTableObjectRepository,
):
    def __init__(
        self,
        connection: MongoDbConnection,
        collection_name: str = "isa_table_files",
        output_entity_class: type[BaseEntity] = IsaTableFileObject,
        study_bucket: StudyBucket = StudyBucket.PRIVATE_METADATA_FILES,
        observer: FileObjectObserver = None,
    ):
        super(MongoDbDefaultWriteRepository, self).__init__(
            connection=connection,
            collection_name=collection_name,
            output_entity_class=output_entity_class,
        )
        super(IsaTableObjectRepository, self).__init__(
            study_bucket, observers=[observer]
        )
