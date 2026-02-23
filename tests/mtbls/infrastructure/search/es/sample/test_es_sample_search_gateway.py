from unittest.mock import AsyncMock, MagicMock

import pytest

from mtbls.infrastructure.search.es.sample.es_sample_configuration import (
    SampleElasticSearchConfiguration,
)
from mtbls.infrastructure.search.es.sample.es_sample_search_gateway import (
    FACTOR_HEADER_FIELDS,
    ElasticsearchSampleGateway,
)


class TestElasticsearchSampleGatewayInit:
    """Tests for gateway initialization."""

    def test_init_with_none_config_uses_defaults(self):
        mock_client = MagicMock()
        gateway = ElasticsearchSampleGateway(client=mock_client, config=None)

        assert gateway.config.api_key_name == "sample"
        assert gateway.config.index_name == "sample"
        assert gateway.config.max_study_ids == 10000

    def test_init_with_dict_config(self):
        mock_client = MagicMock()
        config_dict = {
            "api_key_name": "custom_key",
            "index_name": "custom_index",
            "max_study_ids": 5000,
        }
        gateway = ElasticsearchSampleGateway(client=mock_client, config=config_dict)

        assert gateway.config.api_key_name == "custom_key"
        assert gateway.config.index_name == "custom_index"
        assert gateway.config.max_study_ids == 5000

    def test_init_with_config_object(self):
        mock_client = MagicMock()
        config = SampleElasticSearchConfiguration(
            api_key_name="test_key",
            index_name="test_index",
            max_study_ids=1000,
        )
        gateway = ElasticsearchSampleGateway(client=mock_client, config=config)

        assert gateway.config.api_key_name == "test_key"
        assert gateway.config.index_name == "test_index"
        assert gateway.config.max_study_ids == 1000


class TestFindStudyIdsByFactorHeaders:
    """Tests for find_study_ids_by_factor_headers method."""

    @pytest.fixture
    def mock_client(self):
        client = MagicMock()
        client.search = AsyncMock()
        return client

    @pytest.fixture
    def gateway(self, mock_client):
        return ElasticsearchSampleGateway(client=mock_client, config=None)

    @pytest.mark.asyncio
    async def test_empty_values_returns_empty_list(self, gateway, mock_client):
        result = await gateway.find_study_ids_by_factor_headers(values=[])

        assert result == []
        mock_client.search.assert_not_called()

    @pytest.mark.asyncio
    async def test_single_value_returns_study_ids(self, gateway, mock_client):
        mock_client.search.return_value = {
            "aggregations": {
                "unique_studies": {
                    "buckets": [
                        {"key": "MTBLS79", "doc_count": 5},
                        {"key": "MTBLS100", "doc_count": 3},
                    ]
                }
            }
        }

        result = await gateway.find_study_ids_by_factor_headers(values=["Batch"])

        assert result == ["MTBLS79", "MTBLS100"]
        mock_client.search.assert_called_once()

    @pytest.mark.asyncio
    async def test_multiple_values_with_and_operator(self, gateway, mock_client):
        mock_client.search.return_value = {
            "aggregations": {
                "unique_studies": {"buckets": [{"key": "MTBLS1", "doc_count": 10}]}
            }
        }

        result = await gateway.find_study_ids_by_factor_headers(
            values=["Batch", "Gender"], operator="and"
        )

        assert result == ["MTBLS1"]

    @pytest.mark.asyncio
    async def test_multiple_values_with_or_operator(self, gateway, mock_client):
        mock_client.search.return_value = {
            "aggregations": {
                "unique_studies": {
                    "buckets": [
                        {"key": "MTBLS1", "doc_count": 10},
                        {"key": "MTBLS2", "doc_count": 5},
                    ]
                }
            }
        }

        result = await gateway.find_study_ids_by_factor_headers(
            values=["Batch", "Gender"], operator="or"
        )

        assert result == ["MTBLS1", "MTBLS2"]

    @pytest.mark.asyncio
    async def test_no_matches_returns_empty_list(self, gateway, mock_client):
        mock_client.search.return_value = {
            "aggregations": {"unique_studies": {"buckets": []}}
        }

        result = await gateway.find_study_ids_by_factor_headers(values=["NONEXISTENT"])

        assert result == []

    @pytest.mark.asyncio
    async def test_missing_aggregations_returns_empty_list(self, gateway, mock_client):
        mock_client.search.return_value = {}

        result = await gateway.find_study_ids_by_factor_headers(values=["Batch"])

        assert result == []


class TestBuildQuery:
    """Tests for _build_query to verify DSL structure."""

    @pytest.fixture
    def gateway(self):
        mock_client = MagicMock()
        return ElasticsearchSampleGateway(client=mock_client, config=None)

    def test_and_operator_uses_must(self, gateway):
        dsl = gateway._build_query(["Batch", "Gender"], operator="and")

        assert dsl["size"] == 0
        assert dsl["timeout"] == "10s"
        assert "must" in dsl["query"]["bool"]
        assert len(dsl["query"]["bool"]["must"]) == 2

    def test_or_operator_uses_should(self, gateway):
        dsl = gateway._build_query(["Batch", "Gender"], operator="or")

        assert "should" in dsl["query"]["bool"]
        assert dsl["query"]["bool"]["minimum_should_match"] == 1

    def test_each_value_queries_both_fields(self, gateway):
        dsl = gateway._build_query(["Batch"], operator="and")

        clause = dsl["query"]["bool"]["must"][0]
        assert "should" in clause["bool"]
        should_terms = clause["bool"]["should"]
        assert len(should_terms) == len(FACTOR_HEADER_FIELDS)
        queried_fields = {list(t["term"].keys())[0] for t in should_terms}
        assert queried_fields == set(FACTOR_HEADER_FIELDS)

    def test_aggregation_structure(self, gateway):
        dsl = gateway._build_query(["Batch"], operator="and")

        aggs = dsl["aggs"]["unique_studies"]
        assert aggs["terms"]["field"] == "studyId.keyword"
        assert aggs["terms"]["size"] == 10000

    def test_custom_max_study_ids(self):
        mock_client = MagicMock()
        config = SampleElasticSearchConfiguration(max_study_ids=500)
        gateway = ElasticsearchSampleGateway(client=mock_client, config=config)

        dsl = gateway._build_query(["Batch"], operator="and")

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
        assert ElasticsearchSampleGateway._extract_study_ids(es_resp) == [
            "MTBLS1",
            "MTBLS2",
        ]

    def test_extract_empty_buckets(self):
        es_resp = {"aggregations": {"unique_studies": {"buckets": []}}}
        assert ElasticsearchSampleGateway._extract_study_ids(es_resp) == []

    def test_extract_missing_aggregations(self):
        assert ElasticsearchSampleGateway._extract_study_ids({}) == []


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
        gateway = ElasticsearchSampleGateway(client=mock_client, config=None)

        await gateway.find_study_ids_by_factor_headers(values=["Batch"])

        call_kwargs = mock_client.search.call_args.kwargs
        assert call_kwargs["index"] == "sample"
        assert call_kwargs["api_key_name"] == "sample"

    @pytest.mark.asyncio
    async def test_search_called_with_custom_config(self, mock_client):
        config = SampleElasticSearchConfiguration(
            index_name="custom_sample_index",
            api_key_name="custom_api_key",
        )
        gateway = ElasticsearchSampleGateway(client=mock_client, config=config)

        await gateway.find_study_ids_by_factor_headers(values=["Batch"])

        call_kwargs = mock_client.search.call_args.kwargs
        assert call_kwargs["index"] == "custom_sample_index"
        assert call_kwargs["api_key_name"] == "custom_api_key"
