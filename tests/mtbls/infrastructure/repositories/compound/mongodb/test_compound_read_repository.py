from unittest.mock import AsyncMock, MagicMock

import pytest

from mtbls.infrastructure.repositories.compound.mongodb.compound_read_repository import (
    MongoCompoundReadRepository,
)


class TestGetCompoundsByIds:
    """Tests for batch compound retrieval."""

    @pytest.fixture
    def mock_database_client(self):
        client = MagicMock()
        return client

    @pytest.fixture
    def repository(self, mock_database_client):
        return MongoCompoundReadRepository(
            database_client=mock_database_client,
            collection_name="compounds",
            include_raw=False,
        )

    @pytest.mark.asyncio
    async def test_returns_empty_for_empty_input(self, repository):
        compounds, missing_ids = await repository.get_compounds_by_ids([])
        assert compounds == []
        assert missing_ids == []

    @pytest.mark.asyncio
    async def test_returns_found_compounds(self, mock_database_client, repository):
        # Set up mock collection with documents
        mock_collection = MagicMock()
        mock_collection.find.return_value = [
            {"id": "MTBLC1", "name": "aspirin", "inchiKey": "ABC"},
            {"id": "MTBLC2", "name": "ibuprofen", "inchiKey": "DEF"},
        ]

        mock_db = MagicMock()
        mock_db.__getitem__ = MagicMock(return_value=mock_collection)

        mock_database_client.database = MagicMock(return_value=AsyncMock())
        mock_database_client.database.return_value.__aenter__ = AsyncMock(return_value=mock_db)
        mock_database_client.database.return_value.__aexit__ = AsyncMock()

        compounds, missing_ids = await repository.get_compounds_by_ids(["MTBLC1", "MTBLC2"])

        assert len(compounds) == 2
        assert missing_ids == []
        mock_collection.find.assert_called_once_with({"id": {"$in": ["MTBLC1", "MTBLC2"]}})

    @pytest.mark.asyncio
    async def test_returns_missing_ids(self, mock_database_client, repository):
        # Set up mock collection with only one document found
        mock_collection = MagicMock()
        mock_collection.find.return_value = [
            {"id": "MTBLC1", "name": "aspirin", "inchiKey": "ABC"},
        ]

        mock_db = MagicMock()
        mock_db.__getitem__ = MagicMock(return_value=mock_collection)

        mock_database_client.database = MagicMock(return_value=AsyncMock())
        mock_database_client.database.return_value.__aenter__ = AsyncMock(return_value=mock_db)
        mock_database_client.database.return_value.__aexit__ = AsyncMock()

        compounds, missing_ids = await repository.get_compounds_by_ids(["MTBLC1", "MTBLC999"])

        assert len(compounds) == 1
        assert compounds[0].id == "MTBLC1"
        assert missing_ids == ["MTBLC999"]

    @pytest.mark.asyncio
    async def test_handles_all_missing(self, mock_database_client, repository):
        # Set up mock collection with no documents found
        mock_collection = MagicMock()
        mock_collection.find.return_value = []

        mock_db = MagicMock()
        mock_db.__getitem__ = MagicMock(return_value=mock_collection)

        mock_database_client.database = MagicMock(return_value=AsyncMock())
        mock_database_client.database.return_value.__aenter__ = AsyncMock(return_value=mock_db)
        mock_database_client.database.return_value.__aexit__ = AsyncMock()

        compounds, missing_ids = await repository.get_compounds_by_ids(["MTBLC999", "MTBLC888"])

        assert compounds == []
        assert set(missing_ids) == {"MTBLC999", "MTBLC888"}

    @pytest.mark.asyncio
    async def test_uses_in_operator_for_batch_query(self, mock_database_client, repository):
        mock_collection = MagicMock()
        mock_collection.find.return_value = []

        mock_db = MagicMock()
        mock_db.__getitem__ = MagicMock(return_value=mock_collection)

        mock_database_client.database = MagicMock(return_value=AsyncMock())
        mock_database_client.database.return_value.__aenter__ = AsyncMock(return_value=mock_db)
        mock_database_client.database.return_value.__aexit__ = AsyncMock()

        await repository.get_compounds_by_ids(["MTBLC1", "MTBLC2", "MTBLC3"])

        # Verify the $in operator is used for efficient batch query
        call_args = mock_collection.find.call_args
        query = call_args[0][0]
        assert "$in" in query["id"]
        assert set(query["id"]["$in"]) == {"MTBLC1", "MTBLC2", "MTBLC3"}
