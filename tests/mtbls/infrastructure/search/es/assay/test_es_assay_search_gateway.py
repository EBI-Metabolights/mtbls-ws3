from unittest.mock import AsyncMock, MagicMock

import pytest

from mtbls.infrastructure.search.es.assay.es_assay_configuration import (
    AssayElasticSearchConfiguration,
)
from mtbls.infrastructure.search.es.assay.es_assay_search_gateway import (
    MS_FIELD_MAP,
    ElasticsearchAssayGateway,
)


class TestElasticsearchAssayGatewayInit:
    """Tests for gateway initialization."""

    def test_init_with_none_config_uses_defaults(self):
        mock_client = MagicMock()
        gateway = ElasticsearchAssayGateway(client=mock_client, config=None)

        assert gateway.config.api_key_name == "assay"
        assert gateway.config.index_name == "assay"
        assert gateway.config.max_study_ids == 10000

    def test_init_with_dict_config(self):
        mock_client = MagicMock()
        config_dict = {
            "api_key_name": "custom_key",
            "index_name": "custom_index",
            "max_study_ids": 5000,
        }
        gateway = ElasticsearchAssayGateway(client=mock_client, config=config_dict)

        assert gateway.config.api_key_name == "custom_key"
        assert gateway.config.index_name == "custom_index"
        assert gateway.config.max_study_ids == 5000

    def test_init_with_config_object(self):
        mock_client = MagicMock()
        config = AssayElasticSearchConfiguration(
            api_key_name="test_key",
            index_name="test_index",
            max_study_ids=1000,
        )
        gateway = ElasticsearchAssayGateway(client=mock_client, config=config)

        assert gateway.config.api_key_name == "test_key"
        assert gateway.config.index_name == "test_index"
        assert gateway.config.max_study_ids == 1000


class TestFindStudyIdsByAssayFilters:
    """Tests for find_study_ids_by_assay_filters method."""

    @pytest.fixture
    def mock_client(self):
        client = MagicMock()
        client.search = AsyncMock()
        return client

    @pytest.fixture
    def gateway(self, mock_client):
        return ElasticsearchAssayGateway(client=mock_client, config=None)

    @pytest.mark.asyncio
    async def test_empty_filters_returns_empty_list(self, gateway, mock_client):
        result = await gateway.find_study_ids_by_assay_filters(ms_filters={})

        assert result == []
        mock_client.search.assert_not_called()

    @pytest.mark.asyncio
    async def test_filters_with_empty_values_returns_empty_list(
        self, gateway, mock_client
    ):
        result = await gateway.find_study_ids_by_assay_filters(
            ms_filters={"column_type": [], "instrument": []}
        )

        assert result == []
        mock_client.search.assert_not_called()

    @pytest.mark.asyncio
    async def test_unknown_filter_key_ignored(self, gateway, mock_client):
        result = await gateway.find_study_ids_by_assay_filters(
            ms_filters={"unknown_field": ["value"]}
        )

        assert result == []
        mock_client.search.assert_not_called()

    @pytest.mark.asyncio
    async def test_find_study_ids_with_column_type(self, gateway, mock_client):
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

        result = await gateway.find_study_ids_by_assay_filters(
            ms_filters={"column_type": ["reverse phase"]}
        )

        assert result == ["MTBLS1", "MTBLS42"]
        mock_client.search.assert_called_once()

    @pytest.mark.asyncio
    async def test_find_study_ids_with_multiple_filters_and_operator(
        self, gateway, mock_client
    ):
        mock_client.search.return_value = {
            "aggregations": {
                "unique_studies": {
                    "buckets": [
                        {"key": "MTBLS106", "doc_count": 10},
                    ]
                }
            }
        }

        result = await gateway.find_study_ids_by_assay_filters(
            ms_filters={
                "column_type": ["reverse phase"],
                "instrument": ["Q Exactive"],
            },
            operator="and",
        )

        assert result == ["MTBLS106"]

    @pytest.mark.asyncio
    async def test_no_matches_returns_empty_list(self, gateway, mock_client):
        mock_client.search.return_value = {
            "aggregations": {"unique_studies": {"buckets": []}}
        }

        result = await gateway.find_study_ids_by_assay_filters(
            ms_filters={"instrument": ["NONEXISTENT"]}
        )

        assert result == []

    @pytest.mark.asyncio
    async def test_missing_aggregations_returns_empty_list(self, gateway, mock_client):
        mock_client.search.return_value = {}

        result = await gateway.find_study_ids_by_assay_filters(
            ms_filters={"instrument": ["Q Exactive"]}
        )

        assert result == []


class TestBuildQuery:
    """Tests for _build_query to verify DSL structure."""

    @pytest.fixture
    def gateway(self):
        mock_client = MagicMock()
        return ElasticsearchAssayGateway(client=mock_client, config=None)

    def test_and_operator_uses_must(self, gateway):
        clauses = [
            {"terms": {MS_FIELD_MAP["column_type"]: ["reverse phase"]}},
            {"terms": {MS_FIELD_MAP["instrument"]: ["Q Exactive"]}},
        ]
        dsl = gateway._build_query(clauses, operator="and")

        assert dsl["size"] == 0
        assert dsl["timeout"] == "10s"
        assert "must" in dsl["query"]["bool"]
        assert len(dsl["query"]["bool"]["must"]) == 2

    def test_or_operator_uses_should(self, gateway):
        clauses = [
            {"terms": {MS_FIELD_MAP["column_type"]: ["reverse phase"]}},
            {"terms": {MS_FIELD_MAP["instrument"]: ["Q Exactive"]}},
        ]
        dsl = gateway._build_query(clauses, operator="or")

        assert "should" in dsl["query"]["bool"]
        assert dsl["query"]["bool"]["minimum_should_match"] == 1

    def test_aggregation_structure(self, gateway):
        clauses = [{"terms": {MS_FIELD_MAP["column_type"]: ["reverse phase"]}}]
        dsl = gateway._build_query(clauses, operator="and")

        aggs = dsl["aggs"]["unique_studies"]
        assert aggs["terms"]["field"] == "studyId.keyword"
        assert aggs["terms"]["size"] == 10000

    def test_custom_max_study_ids(self):
        mock_client = MagicMock()
        config = AssayElasticSearchConfiguration(max_study_ids=500)
        gateway = ElasticsearchAssayGateway(client=mock_client, config=config)

        clauses = [{"terms": {MS_FIELD_MAP["column_type"]: ["reverse phase"]}}]
        dsl = gateway._build_query(clauses, operator="and")

        assert dsl["aggs"]["unique_studies"]["terms"]["size"] == 500


class TestExtractStudyIds:
    """Tests for _extract_study_ids static method."""

    def test_extract_from_buckets(self):
        es_resp = {
            "aggregations": {
                "unique_studies": {
                    "buckets": [
                        {"key": "MTBLS1", "doc_count": 10},
                        {"key": "MTBLS2", "doc_count": 5},
                    ]
                }
            }
        }
        assert ElasticsearchAssayGateway._extract_study_ids(es_resp) == [
            "MTBLS1",
            "MTBLS2",
        ]

    def test_extract_empty_buckets(self):
        es_resp = {"aggregations": {"unique_studies": {"buckets": []}}}
        assert ElasticsearchAssayGateway._extract_study_ids(es_resp) == []

    def test_extract_missing_aggregations(self):
        assert ElasticsearchAssayGateway._extract_study_ids({}) == []


class TestSearchCallParameters:
    """Tests to verify correct parameters are passed to ES client."""

    @pytest.fixture
    def mock_client(self):
        client = MagicMock()
        client.search = AsyncMock(
            return_value={
                "aggregations": {
                    "unique_studies": {"buckets": [{"key": "MTBLS1", "doc_count": 1}]}
                }
            }
        )
        return client

    @pytest.mark.asyncio
    async def test_search_called_with_correct_index(self, mock_client):
        gateway = ElasticsearchAssayGateway(client=mock_client, config=None)

        await gateway.find_study_ids_by_assay_filters(
            ms_filters={"column_type": ["reverse phase"]}
        )

        call_kwargs = mock_client.search.call_args.kwargs
        assert call_kwargs["index"] == "assay"
        assert call_kwargs["api_key_name"] == "assay"

    @pytest.mark.asyncio
    async def test_search_called_with_custom_config(self, mock_client):
        config = AssayElasticSearchConfiguration(
            index_name="custom_assay_index",
            api_key_name="custom_api_key",
        )
        gateway = ElasticsearchAssayGateway(client=mock_client, config=config)

        await gateway.find_study_ids_by_assay_filters(
            ms_filters={"instrument": ["Q Exactive"]}
        )

        call_kwargs = mock_client.search.call_args.kwargs
        assert call_kwargs["index"] == "custom_assay_index"
        assert call_kwargs["api_key_name"] == "custom_api_key"
