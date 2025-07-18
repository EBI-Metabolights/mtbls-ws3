import logging
from typing import Generic

from bson.objectid import ObjectId
from pymongo.results import DeleteResult, InsertManyResult, UpdateResult

from mtbls.application.services.interfaces.repositories.default.abstract_write_repository import (
    AbstractWriteRepository,
)
from mtbls.domain.entities.base_entity import BaseEntity
from mtbls.domain.shared.data_types import ID_TYPE, INPUT_TYPE, OUTPUT_TYPE
from mtbls.infrastructure.persistence.db.mongodb.config import (
    MongoDbConnection,
)
from mtbls.infrastructure.repositories.default.mongodb.default_read_repository import (
    MongoDbDefaultReadRepository,
)

logger = logging.getLogger(__name__)


class MongoDbDefaultWriteRepository(
    MongoDbDefaultReadRepository[OUTPUT_TYPE, ID_TYPE],
    AbstractWriteRepository[INPUT_TYPE, OUTPUT_TYPE, ID_TYPE],
    Generic[INPUT_TYPE, OUTPUT_TYPE, ID_TYPE],
):
    def __init__(
        self,
        connection: MongoDbConnection,
        collection_name: str,
        output_entity_class: type[BaseEntity],
    ):
        super(MongoDbDefaultReadRepository, self).__init__(
            connection=connection,
            collection_name=collection_name,
            output_entity_class=output_entity_class,
        )

    async def create(self, entity: INPUT_TYPE) -> OUTPUT_TYPE:
        input_json = entity.model_dump(by_alias=True, exclude="id_")
        result = self.collection.insert_one(input_json)
        inserted = self.collection.find_one({"_id": result.inserted_id})
        return self.output_entity_class.model_validate(inserted)

    async def create_many(self, entities: list[INPUT_TYPE]) -> list[OUTPUT_TYPE]:
        input_json = []
        for entity in entities:
            input_json.append(entity.model_dump(by_alias=True, exclude="id_"))
        result: InsertManyResult = self.collection.insert_many(input_json)
        return [str(x) for x in result.inserted_ids]

    async def update(self, entity: INPUT_TYPE) -> OUTPUT_TYPE:
        input_json = entity.model_dump(by_alias=True, exclude="id_")
        result: UpdateResult = self.collection.update_one(
            {"_id": ObjectId(entity.id_)}, {"$set": input_json}
        )
        if result.modified_count > 0:
            result = self.collection.find_one({"_id": ObjectId(entity.id_)})
            return self.output_entity_class.model_validate(result)
        raise ValueError("")

    async def delete(self, id_: str) -> bool:
        result: DeleteResult = self.collection.delete_one({"_id": ObjectId(id_)})
        if result.deleted_count > 0:
            return True
        return False
