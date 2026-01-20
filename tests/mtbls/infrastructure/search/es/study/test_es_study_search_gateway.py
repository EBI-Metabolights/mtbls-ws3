import pytest
from unittest.mock import AsyncMock, MagicMock

from mtbls.domain.entities.search.index_search import (
    IndexSearchInput,
    PageModel,
    FilterModel,
)
from mtbls.infrastructure.search.es.study.es_study_search_gateway import (
    ElasticsearchStudyGateway,
    ALL_IDS_MAX_SIZE,
)


class TestHasQueryOrFilters:
    """Tests for _has_query_or_filters helper method."""

    @pytest.fixture
    def gateway(self):
        mock_client = MagicMock()
        return ElasticsearchStudyGateway(client=mock_client, config=None)

    @pytest.fixture
    def base_request(self) -> IndexSearchInput:
        return IndexSearchInput(
            query=None,
            page=PageModel(current=1, size=25),
            sort=None,
            filters=[],
            facets={},
        )

    def test_returns_false_for_empty_request(self, gateway, base_request):
        assert gateway._has_query_or_filters(base_request) is False

    def test_returns_false_for_whitespace_query(self, gateway, base_request):
        base_request.query = "   "
        assert gateway._has_query_or_filters(base_request) is False

    def test_returns_true_for_text_query(self, gateway, base_request):
        base_request.query = "lipidomics"
        assert gateway._has_query_or_filters(base_request) is True

    def test_returns_true_for_filter_with_values(self, gateway, base_request):
        base_request.filters = [
            FilterModel(field="organisms", values=["Homo sapiens"], operator="any")
        ]
        assert gateway._has_query_or_filters(base_request) is True

    def test_returns_false_for_filter_with_empty_values(self, gateway, base_request):
        base_request.filters = [
            FilterModel(field="organisms", values=[], operator="any")
        ]
        assert gateway._has_query_or_filters(base_request) is False

    def test_returns_true_for_multiple_filters_one_with_values(self, gateway, base_request):
        base_request.filters = [
            FilterModel(field="organisms", values=[], operator="any"),
            FilterModel(field="technology", values=["NMR"], operator="any"),
        ]
        assert gateway._has_query_or_filters(base_request) is True


class TestBuildIdsOnlyPayload:
    """Tests for _build_ids_only_payload method."""

    @pytest.fixture
    def gateway(self):
        mock_client = MagicMock()
        return ElasticsearchStudyGateway(client=mock_client, config=None)

    @pytest.fixture
    def base_request(self) -> IndexSearchInput:
        return IndexSearchInput(
            query=None,
            page=PageModel(current=1, size=25),
            sort=None,
            filters=[],
            facets={},
        )

    def test_payload_uses_studyid_source_only(self, gateway, base_request):
        base_request.query = "cancer"
        payload = gateway._build_ids_only_payload(base_request)
        assert payload["_source"] == ["studyId"]

    def test_payload_uses_max_size_limit(self, gateway, base_request):
        base_request.query = "cancer"
        payload = gateway._build_ids_only_payload(base_request)
        assert payload["size"] == ALL_IDS_MAX_SIZE

    def test_payload_has_no_aggregations(self, gateway, base_request):
        base_request.query = "cancer"
        payload = gateway._build_ids_only_payload(base_request)
        assert "aggs" not in payload

    def test_payload_does_not_track_total_hits(self, gateway, base_request):
        base_request.query = "cancer"
        payload = gateway._build_ids_only_payload(base_request)
        assert payload["track_total_hits"] is False

    def test_payload_sorts_by_study_id(self, gateway, base_request):
        base_request.query = "cancer"
        payload = gateway._build_ids_only_payload(base_request)
        assert payload["sort"] == [{"studyId": "asc"}]

    def test_payload_includes_text_query(self, gateway, base_request):
        base_request.query = "lipidomics"
        payload = gateway._build_ids_only_payload(base_request)

        bool_query = payload["query"]["bool"]
        must_clause = bool_query["must"][0]
        assert "bool" in must_clause
        assert "should" in must_clause["bool"]

    def test_payload_includes_filters(self, gateway, base_request):
        base_request.filters = [
            FilterModel(field="organisms", values=["Homo sapiens"], operator="any")
        ]
        payload = gateway._build_ids_only_payload(base_request)

        bool_query = payload["query"]["bool"]
        # Filter clause should be present
        assert len(bool_query["filter"]) > 0


class TestSearchWithIncludeAllIds:
    """Tests for search method with include_all_ids parameter."""

    @pytest.fixture
    def mock_client(self):
        client = MagicMock()
        client.search = AsyncMock()
        return client

    @pytest.fixture
    def gateway(self, mock_client):
        return ElasticsearchStudyGateway(client=mock_client, config=None)

    @pytest.fixture
    def base_request(self) -> IndexSearchInput:
        return IndexSearchInput(
            query=None,
            page=PageModel(current=1, size=25),
            sort=None,
            filters=[],
            facets={},
        )

    @pytest.mark.asyncio
    async def test_no_second_query_when_include_all_ids_false(self, mock_client, gateway, base_request):
        base_request.query = "cancer"
        mock_client.search.return_value = {
            "hits": {"hits": [], "total": {"value": 0}},
            "aggregations": {},
        }

        result = await gateway.search(base_request, include_all_ids=False)

        # Should only be called once (main search)
        assert mock_client.search.call_count == 1
        assert result.all_study_ids is None

    @pytest.mark.asyncio
    async def test_no_second_query_when_no_query_or_filters(self, mock_client, gateway, base_request):
        mock_client.search.return_value = {
            "hits": {"hits": [], "total": {"value": 0}},
            "aggregations": {},
        }

        result = await gateway.search(base_request, include_all_ids=True)

        # Should only be called once (main search), skip IDs query
        assert mock_client.search.call_count == 1
        assert result.all_study_ids is None

    @pytest.mark.asyncio
    async def test_second_query_when_include_all_ids_true_with_query(self, mock_client, gateway, base_request):
        base_request.query = "cancer"
        mock_client.search.side_effect = [
            # First call: main search
            {
                "hits": {"hits": [{"_source": {"studyId": "MTBLS1"}}], "total": {"value": 5}},
                "aggregations": {},
            },
            # Second call: all IDs query
            {
                "hits": {"hits": [
                    {"_source": {"studyId": "MTBLS1"}},
                    {"_source": {"studyId": "MTBLS2"}},
                    {"_source": {"studyId": "MTBLS3"}},
                    {"_source": {"studyId": "MTBLS4"}},
                    {"_source": {"studyId": "MTBLS5"}},
                ]},
            },
        ]

        result = await gateway.search(base_request, include_all_ids=True)

        # Should be called twice
        assert mock_client.search.call_count == 2
        assert result.all_study_ids == ["MTBLS1", "MTBLS2", "MTBLS3", "MTBLS4", "MTBLS5"]

    @pytest.mark.asyncio
    async def test_second_query_when_include_all_ids_true_with_filters(self, mock_client, gateway, base_request):
        base_request.filters = [
            FilterModel(field="organisms", values=["Homo sapiens"], operator="any")
        ]
        mock_client.search.side_effect = [
            # First call: main search
            {
                "hits": {"hits": [], "total": {"value": 2}},
                "aggregations": {},
            },
            # Second call: all IDs query
            {
                "hits": {"hits": [
                    {"_source": {"studyId": "MTBLS10"}},
                    {"_source": {"studyId": "MTBLS20"}},
                ]},
            },
        ]

        result = await gateway.search(base_request, include_all_ids=True)

        # Should be called twice
        assert mock_client.search.call_count == 2
        assert result.all_study_ids == ["MTBLS10", "MTBLS20"]

    @pytest.mark.asyncio
    async def test_all_study_ids_handles_missing_study_id_in_source(self, mock_client, gateway, base_request):
        base_request.query = "cancer"
        mock_client.search.side_effect = [
            # First call: main search
            {
                "hits": {"hits": [], "total": {"value": 0}},
                "aggregations": {},
            },
            # Second call: all IDs query with some docs missing studyId
            {
                "hits": {"hits": [
                    {"_source": {"studyId": "MTBLS1"}},
                    {"_source": {}},  # Missing studyId
                    {"_source": {"studyId": "MTBLS3"}},
                ]},
            },
        ]

        result = await gateway.search(base_request, include_all_ids=True)

        # Should skip entries without studyId
        assert result.all_study_ids == ["MTBLS1", "MTBLS3"]
