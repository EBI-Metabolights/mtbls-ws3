import logging
from typing import List, Optional, Tuple

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
        include_raw: bool = True,
    ) -> None:
        self.database_client = database_client
        self.collection_name = collection_name
        self.include_raw = include_raw

    async def get_compound_by_id(self, id_: str) -> Optional[Compound]:
        async with self.database_client.database() as database:
            collection = database[self.collection_name]
            doc = collection.find_one({"id": id_})
            if not doc:
                return None
            try:
                return Compound.from_mongo_with_raw(doc, include_raw=self.include_raw)
            except Exception as ex:
                logger.exception(ex)
                return None

    async def get_compounds_by_ids(
        self, ids: List[str]
    ) -> Tuple[List[Compound], List[str]]:
        """
        Retrieve multiple compounds by their IDs using MongoDB $in operator.

        Args:
            ids: List of compound IDs to retrieve

        Returns:
            Tuple of (found_compounds, missing_ids)
        """
        if not ids:
            return ([], [])

        async with self.database_client.database() as database:
            collection = database[self.collection_name]
            cursor = collection.find({"id": {"$in": ids}})
            docs = list(cursor)

        # Build compounds from documents
        found_compounds: List[Compound] = []
        found_ids: set[str] = set()

        for doc in docs:
            try:
                compound = Compound.from_mongo_with_raw(
                    doc, include_raw=self.include_raw
                )
                found_compounds.append(compound)
                found_ids.add(doc.get("id", ""))
            except Exception as ex:
                logger.exception("Failed to parse compound document: %s", ex)

        # Determine which IDs were not found
        missing_ids = [id_ for id_ in ids if id_ not in found_ids]

        return (found_compounds, missing_ids)
