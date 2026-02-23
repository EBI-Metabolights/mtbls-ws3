from unittest.mock import AsyncMock, MagicMock

import pytest

from mtbls.infrastructure.search.es.assignment.es_assignment_configuration import (
    AssignmentElasticSearchConfiguration,
)
from mtbls.infrastructure.search.es.assignment.es_assignment_search_gateway import (
    ElasticsearchAssignmentGateway,
)


class TestElasticsearchAssignmentGatewayInit:
    """Tests for gateway initialization."""

    def test_init_with_none_config_uses_defaults(self):
        mock_client = MagicMock()
        gateway = ElasticsearchAssignmentGateway(client=mock_client, config=None)

        assert gateway.config.api_key_name == "assignment"
        assert gateway.config.index_name == "assignment"
        assert gateway.config.max_study_ids == 10000

    def test_init_with_dict_config(self):
        mock_client = MagicMock()
        config_dict = {
            "api_key_name": "custom_key",
            "index_name": "custom_index",
            "max_study_ids": 5000,
        }
        gateway = ElasticsearchAssignmentGateway(client=mock_client, config=config_dict)

        assert gateway.config.api_key_name == "custom_key"
        assert gateway.config.index_name == "custom_index"
        assert gateway.config.max_study_ids == 5000

    def test_init_with_config_object(self):
        mock_client = MagicMock()
        config = AssignmentElasticSearchConfiguration(
            api_key_name="test_key",
            index_name="test_index",
            max_study_ids=1000,
        )
        gateway = ElasticsearchAssignmentGateway(client=mock_client, config=config)

        assert gateway.config.api_key_name == "test_key"
        assert gateway.config.index_name == "test_index"
        assert gateway.config.max_study_ids == 1000


class TestFindStudyIdsByCompounds:
    """Tests for find_study_ids_by_compounds method."""

    @pytest.fixture
    def mock_client(self):
        client = MagicMock()
        client.search = AsyncMock()
        return client

    @pytest.fixture
    def gateway(self, mock_client):
        return ElasticsearchAssignmentGateway(client=mock_client, config=None)

    @pytest.mark.asyncio
    async def test_empty_input_returns_empty_list(self, gateway, mock_client):
        result = await gateway.find_study_ids_by_compounds()

        assert result == []
        mock_client.search.assert_not_called()

    @pytest.mark.asyncio
    async def test_none_inputs_returns_empty_list(self, gateway, mock_client):
        result = await gateway.find_study_ids_by_compounds(
            database_identifiers=None,
            metabolite_identifications=None,
        )

        assert result == []
        mock_client.search.assert_not_called()

    @pytest.mark.asyncio
    async def test_empty_lists_returns_empty_list(self, gateway, mock_client):
        result = await gateway.find_study_ids_by_compounds(
            database_identifiers=[],
            metabolite_identifications=[],
        )

        assert result == []
        mock_client.search.assert_not_called()

    @pytest.mark.asyncio
    async def test_find_study_ids_with_database_identifiers(self, gateway, mock_client):
        mock_client.search.return_value = {
            "aggregations": {
                "unique_studies": {
                    "buckets": [
                        {"key": "MTBLS1", "doc_count": 5},
                        {"key": "MTBLS42", "doc_count": 3},
                    ]
                }
            }
        }

        result = await gateway.find_study_ids_by_compounds(database_identifiers=["HMDB0031111"])

        assert result == ["MTBLS1", "MTBLS42"]
        mock_client.search.assert_called_once()

    @pytest.mark.asyncio
    async def test_find_study_ids_with_multiple_database_identifiers(self, gateway, mock_client):
        mock_client.search.return_value = {
            "aggregations": {
                "unique_studies": {
                    "buckets": [
                        {"key": "MTBLS1", "doc_count": 10},
                        {"key": "MTBLS2", "doc_count": 5},
                        {"key": "MTBLS3", "doc_count": 2},
                    ]
                }
            }
        }

        result = await gateway.find_study_ids_by_compounds(database_identifiers=["HMDB0031111", "HMDB0000001"])

        assert result == ["MTBLS1", "MTBLS2", "MTBLS3"]
        mock_client.search.assert_called_once()

    @pytest.mark.asyncio
    async def test_all_operator_builds_bucket_selector(self, gateway, mock_client):
        mock_client.search.return_value = {
            "aggregations": {"unique_studies": {"buckets": [{"key": "MTBLS1", "doc_count": 1}]}}
        }
        await gateway.find_study_ids_by_compounds(
            database_identifiers=["HMDB1", "HMDB2"],
            metabolite_identifications=["Aspirin"],
            database_identifiers_operator="all",
            metabolite_identifications_operator="any",
        )

        call_kwargs = mock_client.search.call_args.kwargs
        aggs = call_kwargs["body"]["aggs"]["unique_studies"]["aggs"]

        # expect per-value filters for the "all" list
        assert "db_all_0" in aggs and "db_all_1" in aggs
        # expect an "any" filter for names because the other list is "all"
        assert "met_any" in aggs
        # bucket selector should require all parts
        selector = aggs["require_all"]["bucket_selector"]
        script = selector["script"]
        assert "params.db_all_0 > 0" in script
        assert "params.db_all_1 > 0" in script
        assert "params.met_any > 0" in script

    @pytest.mark.asyncio
    async def test_find_study_ids_with_metabolite_identifications(self, gateway, mock_client):
        mock_client.search.return_value = {
            "aggregations": {
                "unique_studies": {
                    "buckets": [
                        {"key": "MTBLS100", "doc_count": 8},
                    ]
                }
            }
        }

        result = await gateway.find_study_ids_by_compounds(
            metabolite_identifications=["Lithocholic acid 3-O-glucuronide"]
        )

        assert result == ["MTBLS100"]

    @pytest.mark.asyncio
    async def test_find_study_ids_with_both_filters(self, gateway, mock_client):
        mock_client.search.return_value = {
            "aggregations": {
                "unique_studies": {
                    "buckets": [
                        {"key": "MTBLS1", "doc_count": 15},
                        {"key": "MTBLS2", "doc_count": 7},
                    ]
                }
            }
        }

        result = await gateway.find_study_ids_by_compounds(
            database_identifiers=["HMDB0031111"],
            metabolite_identifications=["Aspirin"],
        )

        assert result == ["MTBLS1", "MTBLS2"]

    @pytest.mark.asyncio
    async def test_no_matches_returns_empty_list(self, gateway, mock_client):
        mock_client.search.return_value = {"aggregations": {"unique_studies": {"buckets": []}}}

        result = await gateway.find_study_ids_by_compounds(database_identifiers=["NONEXISTENT123"])

        assert result == []

    @pytest.mark.asyncio
    async def test_missing_aggregations_returns_empty_list(self, gateway, mock_client):
        mock_client.search.return_value = {}

        result = await gateway.find_study_ids_by_compounds(database_identifiers=["HMDB0031111"])

        assert result == []


class TestBuildQuery:
    """Tests for _build_query method to verify DSL structure."""

    @pytest.fixture
    def gateway(self):
        mock_client = MagicMock()
        return ElasticsearchAssignmentGateway(client=mock_client, config=None)

    def test_query_structure_database_identifiers_only(self, gateway):
        query = gateway._build_query(
            database_identifiers=["HMDB0031111", "HMDB0000001"],
            metabolite_identifications=[],
            database_identifiers_operator="any",
            metabolite_identifications_operator="any",
        )

        assert query["size"] == 0
        assert query["timeout"] == "10s"
        assert "query" in query
        assert "aggs" in query

        bool_query = query["query"]["bool"]
        assert bool_query["minimum_should_match"] == 1
        assert len(bool_query["should"]) == 1
        assert bool_query["should"][0] == {
            "terms": {
                "fields.database_identifier.value.keyword": [
                    "HMDB0031111",
                    "HMDB0000001",
                ]
            }
        }

        aggs = query["aggs"]["unique_studies"]
        assert aggs["terms"]["field"] == "studyId.keyword"
        assert aggs["terms"]["size"] == 10000

    def test_query_structure_metabolite_identifications_only(self, gateway):
        query = gateway._build_query(
            database_identifiers=[],
            metabolite_identifications=["Lithocholic acid 3-O-glucuronide", "Aspirin"],
            database_identifiers_operator="any",
            metabolite_identifications_operator="any",
        )

        bool_query = query["query"]["bool"]
        assert len(bool_query["should"]) == 1
        assert bool_query["should"][0] == {
            "terms": {
                "fields.metabolite_identification.value.keyword": [
                    "Lithocholic acid 3-O-glucuronide",
                    "Aspirin",
                ]
            }
        }

    def test_query_structure_both_filters(self, gateway):
        query = gateway._build_query(
            database_identifiers=["HMDB0031111"],
            metabolite_identifications=["Aspirin"],
            database_identifiers_operator="any",
            metabolite_identifications_operator="any",
        )

        bool_query = query["query"]["bool"]
        assert bool_query["minimum_should_match"] == 1
        assert len(bool_query["should"]) == 2

        # Order may vary, so check both are present
        should_clauses = bool_query["should"]
        db_clause = {"terms": {"fields.database_identifier.value.keyword": ["HMDB0031111"]}}
        met_clause = {"terms": {"fields.metabolite_identification.value.keyword": ["Aspirin"]}}
        assert db_clause in should_clauses
        assert met_clause in should_clauses

    def test_query_uses_custom_max_study_ids(self):
        mock_client = MagicMock()
        config = AssignmentElasticSearchConfiguration(max_study_ids=500)
        gateway = ElasticsearchAssignmentGateway(client=mock_client, config=config)

        query = gateway._build_query(
            database_identifiers=["HMDB0031111"],
            metabolite_identifications=[],
            database_identifiers_operator="any",
            metabolite_identifications_operator="any",
        )

        assert query["aggs"]["unique_studies"]["terms"]["size"] == 500


class TestExtractStudyIds:
    """Tests for _extract_study_ids static method."""

    def test_extract_study_ids_from_buckets(self):
        es_resp = {
            "aggregations": {
                "unique_studies": {
                    "buckets": [
                        {"key": "MTBLS1", "doc_count": 10},
                        {"key": "MTBLS2", "doc_count": 5},
                        {"key": "MTBLS3", "doc_count": 1},
                    ]
                }
            }
        }

        result = ElasticsearchAssignmentGateway._extract_study_ids(es_resp)

        assert result == ["MTBLS1", "MTBLS2", "MTBLS3"]

    def test_extract_study_ids_empty_buckets(self):
        es_resp = {"aggregations": {"unique_studies": {"buckets": []}}}

        result = ElasticsearchAssignmentGateway._extract_study_ids(es_resp)

        assert result == []

    def test_extract_study_ids_missing_aggregations(self):
        es_resp = {}

        result = ElasticsearchAssignmentGateway._extract_study_ids(es_resp)

        assert result == []

    def test_extract_study_ids_missing_unique_studies(self):
        es_resp = {"aggregations": {}}

        result = ElasticsearchAssignmentGateway._extract_study_ids(es_resp)

        assert result == []


class TestSearchCallParameters:
    """Tests to verify correct parameters are passed to ES client."""

    @pytest.fixture
    def mock_client(self):
        client = MagicMock()
        client.search = AsyncMock(
            return_value={"aggregations": {"unique_studies": {"buckets": [{"key": "MTBLS1", "doc_count": 1}]}}}
        )
        return client

    @pytest.mark.asyncio
    async def test_search_called_with_correct_index(self, mock_client):
        gateway = ElasticsearchAssignmentGateway(client=mock_client, config=None)

        await gateway.find_study_ids_by_compounds(database_identifiers=["HMDB0031111"])

        call_kwargs = mock_client.search.call_args.kwargs
        assert call_kwargs["index"] == "assignment"
        assert call_kwargs["api_key_name"] == "assignment"

    @pytest.mark.asyncio
    async def test_search_called_with_custom_config(self, mock_client):
        config = AssignmentElasticSearchConfiguration(
            index_name="custom_assignment_index",
            api_key_name="custom_api_key",
        )
        gateway = ElasticsearchAssignmentGateway(client=mock_client, config=config)

        await gateway.find_study_ids_by_compounds(database_identifiers=["HMDB0031111"])

        call_kwargs = mock_client.search.call_args.kwargs
        assert call_kwargs["index"] == "custom_assignment_index"
        assert call_kwargs["api_key_name"] == "custom_api_key"
