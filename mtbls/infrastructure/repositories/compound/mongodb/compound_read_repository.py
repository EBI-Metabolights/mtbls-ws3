
import logging
from typing import Optional

from mtbls.application.services.interfaces.repositories.compound.compound_read_repository import (  # noqa: E501
    CompoundReadRepository,
)
from mtbls.domain.entities.compound import Compound
from mtbls.infrastructure.persistence.db.mongodb.db_client import (
    DocumentDatabaseClient,
)

logger = logging.getLogger(__name__)


class MongoCompoundReadRepository(CompoundReadRepository):
    def __init__(
        self,
        database_client: DocumentDatabaseClient,
        collection_name: str = "compounds",
    ) -> None:
        self.database_client = database_client
        self.collection_name = collection_name

    async def get_compound_by_id(self, id_: str) -> Optional[Compound]:
        async with self.database_client.database() as database:
            collection = database[self.collection_name]
            doc = collection.find_one({"id": id_})
            if not doc:
                return None
            try:
                return Compound.from_mongo(doc)
            except Exception as ex:
                logger.exception(ex)
                return None
