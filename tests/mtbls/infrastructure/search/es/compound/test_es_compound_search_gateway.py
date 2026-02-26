from unittest.mock import AsyncMock, MagicMock

import pytest

from mtbls.domain.entities.search.index_search import (
    CompoundSearchInput,
    FilterModel,
    PageModel,
    SortModel,
)
from mtbls.infrastructure.search.es.compound.es_compound_search_gateway import (
    MTBLS_STUDY_ID_PATTERN,
    ElasticsearchCompoundGateway,
)


class TestMtblsStudyIdPattern:
    """Tests for the MTBLS study ID regex pattern."""

    @pytest.mark.parametrize(
        "query,should_match",
        [
            ("MTBLS123", True),
            ("MTBLS1", True),
            ("MTBLS99999", True),
            ("mtbls123", True),  # case insensitive
            ("Mtbls456", True),  # mixed case
            ("MTBLS", False),  # no number
            ("MTBLS123abc", False),  # trailing chars
            ("preMTBLS123", False),  # leading chars
            ("aspirin", False),  # regular text
            ("123", False),  # just number
            ("", False),  # empty
        ],
    )
    def test_pattern_matching(self, query: str, should_match: bool):
        match = MTBLS_STUDY_ID_PATTERN.match(query)
        assert bool(match) == should_match


class TestExtractStudyIdsFromQuery:
    """Tests for __extract_study_ids_from_query helper method."""

    def test_none_query_returns_none(self):
        detected, remaining = (
            ElasticsearchCompoundGateway._extract_study_ids_from_query(None)  # noqa SLF001
        )
        assert detected is None
        assert remaining is None

    def test_empty_query_returns_none(self):
        detected, remaining = (
            ElasticsearchCompoundGateway._extract_study_ids_from_query("")  # noqa SLF001
        )
        assert detected is None
        assert remaining == ""

    def test_regular_text_query_returns_none_detected(self):
        detected, remaining = (
            ElasticsearchCompoundGateway._extract_study_ids_from_query("aspirin")  # noqa SLF001
        )
        assert detected is None
        assert remaining == "aspirin"

    def test_study_id_query_returns_detected_and_none_remaining(self):
        detected, remaining = (
            ElasticsearchCompoundGateway._extract_study_ids_from_query("MTBLS123")  # noqa SLF001
        )
        assert detected == ["MTBLS123"]
        assert remaining is None

    def test_lowercase_study_id_normalized_to_uppercase(self):
        detected, remaining = (
            ElasticsearchCompoundGateway._extract_study_ids_from_query("mtbls456")  # noqa SLF001
        )
        assert detected == ["MTBLS456"]
        assert remaining is None

    def test_study_id_with_whitespace_trimmed(self):
        detected, remaining = (
            ElasticsearchCompoundGateway._extract_study_ids_from_query("  MTBLS789  ")  # noqa SLF001
        )
        assert detected == ["MTBLS789"]
        assert remaining is None


class TestBuildSearchPayloadStudyIds:
    """Tests for _build_search_payload with study_ids parameter."""

    @pytest.fixture
    def gateway(self):
        mock_client = MagicMock()
        return ElasticsearchCompoundGateway(client=mock_client, config=None)

    @pytest.fixture
    def base_request(self) -> CompoundSearchInput:
        return CompoundSearchInput(
            query=None,
            page=PageModel(current=1, size=25),
            sort=None,
            filters=[],
            facets={},
            study_ids=None,
        )

    def test_no_study_ids_no_filter(self, gateway, base_request):
        payload = gateway._build_search_payload(base_request)  # noqa SLF001
        # Should have match_all in must, no study filter in filter clauses
        bool_query = payload["query"]["bool"]
        assert bool_query["must"] == [{"match_all": {}}]
        assert bool_query["filter"] == []

    def test_explicit_study_ids_adds_terms_filter(self, gateway, base_request):
        base_request.study_ids = ["MTBLS1", "MTBLS2"]
        payload = gateway._build_search_payload(base_request)  # noqa SLF001

        bool_query = payload["query"]["bool"]
        assert {"terms": {"studyIds": ["MTBLS1", "MTBLS2"]}} in bool_query["filter"]

    def test_study_ids_operator_all_adds_must_terms(self, gateway, base_request):
        base_request.study_ids = ["MTBLS1", "MTBLS2"]
        base_request.study_ids_operator = "all"
        payload = gateway._build_search_payload(base_request)  # noqa SLF001

        bool_query = payload["query"]["bool"]
        assert {
            "bool": {
                "must": [
                    {"term": {"studyIds": "MTBLS1"}},
                    {"term": {"studyIds": "MTBLS2"}},
                ]
            }
        } in bool_query["filter"]

    def test_explicit_study_ids_normalized_to_uppercase(self, gateway, base_request):
        base_request.study_ids = ["mtbls1", "Mtbls2"]
        payload = gateway._build_search_payload(base_request)  # noqa SLF001

        bool_query = payload["query"]["bool"]
        assert {"terms": {"studyIds": ["MTBLS1", "MTBLS2"]}} in bool_query["filter"]

    def test_detected_study_id_from_query(self, gateway, base_request):
        base_request.query = "MTBLS123"
        payload = gateway._build_search_payload(base_request)  # noqa SLF001

        bool_query = payload["query"]["bool"]
        # Should have study filter
        assert {"terms": {"studyIds": ["MTBLS123"]}} in bool_query["filter"]
        # Should NOT have text search (detected study ID consumes the query)
        assert bool_query["must"] == [{"match_all": {}}]

    def test_explicit_study_ids_takes_precedence_over_detected(
        self, gateway, base_request
    ):
        base_request.query = "MTBLS123"  # Would be detected
        base_request.study_ids = ["MTBLS456", "MTBLS789"]  # Explicit takes precedence
        payload = gateway._build_search_payload(base_request)  # noqa SLF001

        bool_query = payload["query"]["bool"]
        # Explicit param should be used
        assert {"terms": {"studyIds": ["MTBLS456", "MTBLS789"]}} in bool_query["filter"]
        # Query should be treated as text search since explicit study_ids provided
        # Actually, when explicit study_ids is provided, the query "MTBLS123" is still used
        # as a text query - let me check the logic again...
        # The logic is: if explicit study_ids, use them, and query stays as-is for text search
        must_clause = bool_query["must"]
        assert len(must_clause) == 1
        assert "bool" in must_clause[0]  # Text search clause

    def test_text_query_with_explicit_study_ids_combines_with_and(
        self, gateway, base_request
    ):
        base_request.query = "aspirin"
        base_request.study_ids = ["MTBLS1", "MTBLS2"]
        payload = gateway._build_search_payload(base_request)  # noqa SLF001

        bool_query = payload["query"]["bool"]
        # Should have study filter
        assert {"terms": {"studyIds": ["MTBLS1", "MTBLS2"]}} in bool_query["filter"]
        # Should have text search in must
        must_clause = bool_query["must"][0]
        assert "bool" in must_clause
        assert "should" in must_clause["bool"]
        # First should clause should be multi_match with "aspirin"
        multi_match = must_clause["bool"]["should"][0]
        assert multi_match["multi_match"]["query"] == "aspirin"

    def test_numeric_query_with_study_ids(self, gateway, base_request):
        base_request.query = "150.5"
        base_request.study_ids = ["MTBLS1"]
        payload = gateway._build_search_payload(base_request)  # noqa SLF001

        # Should have function_score for numeric query
        assert "function_score" in payload["query"]
        # Study filter should be in the bool query inside function_score
        bool_query = payload["query"]["function_score"]["query"]["bool"]
        assert {"terms": {"studyIds": ["MTBLS1"]}} in bool_query["filter"]

    def test_empty_study_ids_list_treated_as_none(self, gateway, base_request):
        base_request.study_ids = []
        payload = gateway._build_search_payload(base_request)  # noqa SLF001

        bool_query = payload["query"]["bool"]
        # Empty list should not add any filter
        assert bool_query["filter"] == []


class TestBuildSearchPayloadIntegration:
    """Integration tests for _build_search_payload with various combinations."""

    @pytest.fixture
    def gateway(self):
        mock_client = MagicMock()
        return ElasticsearchCompoundGateway(client=mock_client, config=None)

    def test_study_ids_with_facet_filters(self, gateway):
        request = CompoundSearchInput(
            query="aspirin",
            page=PageModel(current=1, size=25),
            sort=None,
            filters=[
                FilterModel(field="organisms", values=["Homo sapiens"], operator="any"),
            ],
            facets={},
            study_ids=["MTBLS1"],
        )
        payload = gateway._build_search_payload(request)  # noqa SLF001

        bool_query = payload["query"]["bool"]
        # Should have both study_ids filter and organisms filter
        assert {"terms": {"studyIds": ["MTBLS1"]}} in bool_query["filter"]
        assert {"terms": {"organisms": ["Homo sapiens"]}} in bool_query["filter"]

    def test_study_ids_with_sorting_and_pagination(self, gateway):
        request = CompoundSearchInput(
            query=None,
            page=PageModel(current=2, size=10),
            sort=SortModel(field="name", direction="asc"),
            filters=[],
            facets={},
            study_ids=["MTBLS1", "MTBLS2", "MTBLS3"],
        )
        payload = gateway._build_search_payload(request)  # noqa SLF001

        # Check pagination
        assert payload["from"] == 10  # (2-1) * 10
        assert payload["size"] == 10
        # Check sorting
        assert payload["sort"] == [{"name": {"order": "asc"}}]
        # Check study filter
        bool_query = payload["query"]["bool"]
        assert {"terms": {"studyIds": ["MTBLS1", "MTBLS2", "MTBLS3"]}} in bool_query[
            "filter"
        ]


class TestExportResults:
    """Tests for the export_results async generator."""

    @pytest.fixture
    def mock_client(self):
        client = MagicMock()
        client.search = AsyncMock()
        return client

    @pytest.fixture
    def gateway(self, mock_client):
        return ElasticsearchCompoundGateway(client=mock_client, config=None)

    @pytest.fixture
    def base_request(self) -> CompoundSearchInput:
        return CompoundSearchInput(
            query=None,
            page=PageModel(current=1, size=25),
            sort=None,
            filters=[],
            facets={},
        )

    @pytest.mark.asyncio
    async def test_export_yields_all_hits(self, mock_client, gateway, base_request):
        mock_client.search.side_effect = [
            {
                "hits": {
                    "hits": [
                        {"_source": {"id": "MTBLC1", "name": "Aspirin"}, "sort": ["1"]},
                        {
                            "_source": {"id": "MTBLC2", "name": "Caffeine"},
                            "sort": ["2"],
                        },
                    ],
                    "total": {"value": 2},
                },
            },
            {
                "hits": {"hits": [], "total": {"value": 2}},
            },
        ]

        results = []
        async for item in gateway.export_results(base_request):
            results.append(item)

        assert len(results) == 2
        assert results[0]["name"] == "Aspirin"

    @pytest.mark.asyncio
    async def test_export_pages_with_search_after(
        self, mock_client, gateway, base_request
    ):
        mock_client.search.side_effect = [
            {
                "hits": {
                    "hits": [{"_source": {"id": "MTBLC1"}, "sort": ["a"]}],
                    "total": {"value": 2},
                },
            },
            {
                "hits": {
                    "hits": [{"_source": {"id": "MTBLC2"}, "sort": ["b"]}],
                    "total": {"value": 2},
                },
            },
            {"hits": {"hits": [], "total": {"value": 2}}},
        ]

        results = []
        async for item in gateway.export_results(base_request, batch_size=1):
            results.append(item)

        assert len(results) == 2
        assert mock_client.search.call_count == 3

    @pytest.mark.asyncio
    async def test_export_respects_max_results(
        self, mock_client, gateway, base_request
    ):
        mock_client.search.return_value = {
            "hits": {
                "hits": [
                    {"_source": {"id": f"MTBLC{i}"}, "sort": [str(i)]} for i in range(5)
                ],
                "total": {"value": 100},
            },
        }

        results = []
        async for item in gateway.export_results(base_request, max_results=3):
            results.append(item)

        assert len(results) == 3

    @pytest.mark.asyncio
    async def test_export_strips_aggs_and_pagination(
        self, mock_client, gateway, base_request
    ):
        base_request.query = "aspirin"
        mock_client.search.return_value = {
            "hits": {"hits": [], "total": {"value": 0}},
        }

        async for _ in gateway.export_results(base_request):
            pass

        call_body = mock_client.search.call_args.kwargs["body"]
        assert "aggs" not in call_body
        assert "from" not in call_body
        assert call_body["sort"] == [{"_doc": "asc"}]
        assert "_source" not in call_body

    @pytest.mark.asyncio
    async def test_export_stops_on_empty_hits(self, mock_client, gateway, base_request):
        mock_client.search.return_value = {
            "hits": {"hits": [], "total": {"value": 0}},
        }

        results = []
        async for item in gateway.export_results(base_request):
            results.append(item)

        assert len(results) == 0
        assert mock_client.search.call_count == 1
