import pytest
from unittest.mock import AsyncMock, MagicMock

from mtbls.domain.entities.search.index_search import (
    StudySearchInput,
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
    def base_request(self) -> StudySearchInput:
        return StudySearchInput(
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

    def test_returns_true_for_study_ids(self, gateway, base_request):
        base_request.study_ids = ["MTBLS1", "MTBLS2"]
        assert gateway._has_query_or_filters(base_request) is True

    def test_returns_false_for_empty_study_ids(self, gateway, base_request):
        base_request.study_ids = []
        assert gateway._has_query_or_filters(base_request) is False


class TestBuildIdsOnlyPayload:
    """Tests for _build_ids_only_payload method."""

    @pytest.fixture
    def gateway(self):
        mock_client = MagicMock()
        return ElasticsearchStudyGateway(client=mock_client, config=None)

    @pytest.fixture
    def base_request(self) -> StudySearchInput:
        return StudySearchInput(
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
    def base_request(self) -> StudySearchInput:
        return StudySearchInput(
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


class TestHasChemicalFilters:
    """Tests for _has_chemical_filters helper method."""

    @pytest.fixture
    def gateway(self):
        mock_client = MagicMock()
        return ElasticsearchStudyGateway(client=mock_client, config=None)

    @pytest.fixture
    def base_request(self) -> StudySearchInput:
        return StudySearchInput(
            query=None,
            page=PageModel(current=1, size=25),
            sort=None,
            filters=[],
            facets={},
        )

    def test_returns_false_for_no_chemical_filters(self, gateway, base_request):
        assert gateway._has_chemical_filters(base_request) is False

    def test_returns_true_for_database_identifiers(self, gateway, base_request):
        base_request.database_identifiers = ["HMDB0031111"]
        assert gateway._has_chemical_filters(base_request) is True

    def test_returns_true_for_metabolite_identifications(self, gateway, base_request):
        base_request.metabolite_identifications = ["Aspirin"]
        assert gateway._has_chemical_filters(base_request) is True

    def test_returns_true_for_both_filters(self, gateway, base_request):
        base_request.database_identifiers = ["HMDB0031111"]
        base_request.metabolite_identifications = ["Aspirin"]
        assert gateway._has_chemical_filters(base_request) is True

    def test_returns_false_for_empty_lists(self, gateway, base_request):
        base_request.database_identifiers = []
        base_request.metabolite_identifications = []
        assert gateway._has_chemical_filters(base_request) is False


class TestResolveChemicalFilters:
    """Tests for _resolve_chemical_filters method."""

    @pytest.fixture
    def mock_client(self):
        client = MagicMock()
        client.search = AsyncMock()
        return client

    @pytest.fixture
    def mock_assignment_gateway(self):
        gateway = MagicMock()
        gateway.find_study_ids_by_compounds = AsyncMock()
        return gateway

    @pytest.fixture
    def gateway(self, mock_client, mock_assignment_gateway):
        return ElasticsearchStudyGateway(
            client=mock_client,
            config=None,
            assignment_gateway=mock_assignment_gateway,
        )

    @pytest.fixture
    def gateway_without_assignment(self, mock_client):
        return ElasticsearchStudyGateway(client=mock_client, config=None)

    @pytest.fixture
    def base_request(self) -> StudySearchInput:
        return StudySearchInput(
            query=None,
            page=PageModel(current=1, size=25),
            sort=None,
            filters=[],
            facets={},
        )

    @pytest.mark.asyncio
    async def test_no_chemical_filters_returns_unchanged(self, gateway, base_request):
        result = await gateway._resolve_chemical_filters(base_request)
        assert result is base_request

    @pytest.mark.asyncio
    async def test_no_assignment_gateway_returns_unchanged(
        self, gateway_without_assignment, base_request
    ):
        base_request.database_identifiers = ["HMDB0031111"]
        result = await gateway_without_assignment._resolve_chemical_filters(base_request)
        assert result is base_request

    @pytest.mark.asyncio
    async def test_resolves_database_identifiers_to_study_ids(
        self, gateway, mock_assignment_gateway, base_request
    ):
        base_request.database_identifiers = ["HMDB0031111"]
        mock_assignment_gateway.find_study_ids_by_compounds.return_value = [
            "MTBLS1",
            "MTBLS2",
        ]

        result = await gateway._resolve_chemical_filters(base_request)

        mock_assignment_gateway.find_study_ids_by_compounds.assert_called_once_with(
            database_identifiers=["HMDB0031111"],
            metabolite_identifications=None,
        )
        assert result.study_ids == ["MTBLS1", "MTBLS2"]
        assert result.database_identifiers is None
        assert result.metabolite_identifications is None

    @pytest.mark.asyncio
    async def test_resolves_metabolite_identifications_to_study_ids(
        self, gateway, mock_assignment_gateway, base_request
    ):
        base_request.metabolite_identifications = ["Aspirin", "Ibuprofen"]
        mock_assignment_gateway.find_study_ids_by_compounds.return_value = ["MTBLS100"]

        result = await gateway._resolve_chemical_filters(base_request)

        assert result.study_ids == ["MTBLS100"]
        assert result.metabolite_identifications is None

    @pytest.mark.asyncio
    async def test_no_matching_studies_returns_impossible_filter(
        self, gateway, mock_assignment_gateway, base_request
    ):
        base_request.database_identifiers = ["NONEXISTENT"]
        mock_assignment_gateway.find_study_ids_by_compounds.return_value = []

        result = await gateway._resolve_chemical_filters(base_request)

        assert result.study_ids == ["__NO_MATCHING_STUDIES__"]

    @pytest.mark.asyncio
    async def test_intersects_with_existing_study_ids(
        self, gateway, mock_assignment_gateway, base_request
    ):
        base_request.database_identifiers = ["HMDB0031111"]
        base_request.study_ids = ["MTBLS1", "MTBLS3", "MTBLS5"]
        mock_assignment_gateway.find_study_ids_by_compounds.return_value = [
            "MTBLS1",
            "MTBLS2",
            "MTBLS3",
        ]

        result = await gateway._resolve_chemical_filters(base_request)

        # Intersection of [MTBLS1, MTBLS3, MTBLS5] and [MTBLS1, MTBLS2, MTBLS3]
        assert set(result.study_ids) == {"MTBLS1", "MTBLS3"}

    @pytest.mark.asyncio
    async def test_empty_intersection_returns_impossible_filter(
        self, gateway, mock_assignment_gateway, base_request
    ):
        base_request.database_identifiers = ["HMDB0031111"]
        base_request.study_ids = ["MTBLS100", "MTBLS200"]
        mock_assignment_gateway.find_study_ids_by_compounds.return_value = [
            "MTBLS1",
            "MTBLS2",
        ]

        result = await gateway._resolve_chemical_filters(base_request)

        assert result.study_ids == ["__NO_MATCHING_STUDIES__"]

    @pytest.mark.asyncio
    async def test_preserves_other_request_fields(
        self, gateway, mock_assignment_gateway, base_request
    ):
        base_request.query = "lipidomics"
        base_request.database_identifiers = ["HMDB0031111"]
        base_request.filters = [
            FilterModel(field="organisms", values=["Homo sapiens"], operator="any")
        ]
        mock_assignment_gateway.find_study_ids_by_compounds.return_value = ["MTBLS1"]

        result = await gateway._resolve_chemical_filters(base_request)

        assert result.query == "lipidomics"
        assert result.filters == base_request.filters
        assert result.page == base_request.page


class TestSearchWithChemicalFilters:
    """Integration tests for search with chemical filters."""

    @pytest.fixture
    def mock_client(self):
        client = MagicMock()
        client.search = AsyncMock()
        return client

    @pytest.fixture
    def mock_assignment_gateway(self):
        gateway = MagicMock()
        gateway.find_study_ids_by_compounds = AsyncMock()
        return gateway

    @pytest.fixture
    def gateway(self, mock_client, mock_assignment_gateway):
        return ElasticsearchStudyGateway(
            client=mock_client,
            config=None,
            assignment_gateway=mock_assignment_gateway,
        )

    @pytest.fixture
    def base_request(self) -> StudySearchInput:
        return StudySearchInput(
            query=None,
            page=PageModel(current=1, size=25),
            sort=None,
            filters=[],
            facets={},
        )

    @pytest.mark.asyncio
    async def test_search_with_database_identifiers_calls_assignment_gateway(
        self, mock_client, mock_assignment_gateway, gateway, base_request
    ):
        base_request.database_identifiers = ["HMDB0031111"]
        mock_assignment_gateway.find_study_ids_by_compounds.return_value = [
            "MTBLS1",
            "MTBLS2",
        ]
        mock_client.search.return_value = {
            "hits": {"hits": [], "total": {"value": 0}},
            "aggregations": {},
        }

        await gateway.search(base_request)

        mock_assignment_gateway.find_study_ids_by_compounds.assert_called_once()

    @pytest.mark.asyncio
    async def test_search_adds_study_ids_filter_from_chemical_filters(
        self, mock_client, mock_assignment_gateway, gateway, base_request
    ):
        base_request.database_identifiers = ["HMDB0031111"]
        mock_assignment_gateway.find_study_ids_by_compounds.return_value = [
            "MTBLS1",
            "MTBLS2",
        ]
        mock_client.search.return_value = {
            "hits": {"hits": [], "total": {"value": 0}},
            "aggregations": {},
        }

        await gateway.search(base_request)

        # Verify the search was called with study_ids filter
        call_args = mock_client.search.call_args
        dsl = call_args.kwargs["body"]
        filter_clauses = dsl["query"]["bool"]["filter"]
        assert {"terms": {"studyId": ["MTBLS1", "MTBLS2"]}} in filter_clauses

    @pytest.mark.asyncio
    async def test_search_without_chemical_filters_does_not_call_assignment_gateway(
        self, mock_client, mock_assignment_gateway, gateway, base_request
    ):
        mock_client.search.return_value = {
            "hits": {"hits": [], "total": {"value": 0}},
            "aggregations": {},
        }

        await gateway.search(base_request)

        mock_assignment_gateway.find_study_ids_by_compounds.assert_not_called()

    @pytest.mark.asyncio
    async def test_search_with_chemical_filters_returns_all_study_ids_when_requested(
        self, mock_client, mock_assignment_gateway, gateway, base_request
    ):
        """Test that all_study_ids is populated when using chemical filters with include_all_ids=True."""
        base_request.database_identifiers = ["HMDB0031111"]
        mock_assignment_gateway.find_study_ids_by_compounds.return_value = [
            "MTBLS1",
            "MTBLS2",
            "MTBLS3",
        ]
        mock_client.search.side_effect = [
            # First call: main search
            {
                "hits": {"hits": [{"_source": {"studyId": "MTBLS1"}}], "total": {"value": 3}},
                "aggregations": {},
            },
            # Second call: all IDs query
            {
                "hits": {"hits": [
                    {"_source": {"studyId": "MTBLS1"}},
                    {"_source": {"studyId": "MTBLS2"}},
                    {"_source": {"studyId": "MTBLS3"}},
                ]},
            },
        ]

        result = await gateway.search(base_request, include_all_ids=True)

        # Should be called twice (main search + IDs query)
        assert mock_client.search.call_count == 2
        assert result.all_study_ids == ["MTBLS1", "MTBLS2", "MTBLS3"]

    @pytest.mark.asyncio
    async def test_search_with_only_chemical_filters_returns_all_study_ids(
        self, mock_client, mock_assignment_gateway, gateway, base_request
    ):
        """Regression test: all_study_ids should be populated even when only chemical filters are used (no text query or facet filters)."""
        # Only chemical filters, no text query or filters
        base_request.database_identifiers = ["HMDB0031111"]
        base_request.query = None
        base_request.filters = []

        mock_assignment_gateway.find_study_ids_by_compounds.return_value = [
            "MTBLS10",
            "MTBLS20",
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

        # Should be called twice because study_ids from chemical filter resolution counts as a meaningful filter
        assert mock_client.search.call_count == 2
        assert result.all_study_ids == ["MTBLS10", "MTBLS20"]


class TestBuildSearchPayloadWithStudyIds:
    """Tests for _build_search_payload with study_ids."""

    @pytest.fixture
    def gateway(self):
        mock_client = MagicMock()
        return ElasticsearchStudyGateway(client=mock_client, config=None)

    @pytest.fixture
    def base_request(self) -> StudySearchInput:
        return StudySearchInput(
            query=None,
            page=PageModel(current=1, size=25),
            sort=None,
            filters=[],
            facets={},
        )

    def test_study_ids_adds_terms_filter(self, gateway, base_request):
        base_request.study_ids = ["MTBLS1", "MTBLS2"]
        payload = gateway._build_search_payload(base_request)

        bool_query = payload["query"]["bool"]
        assert {"terms": {"studyId": ["MTBLS1", "MTBLS2"]}} in bool_query["filter"]

    def test_no_study_ids_no_filter(self, gateway, base_request):
        payload = gateway._build_search_payload(base_request)

        bool_query = payload["query"]["bool"]
        # Filter should be empty or not contain studyId filter
        study_id_filters = [f for f in bool_query["filter"] if "studyId" in str(f)]
        assert len(study_id_filters) == 0

    def test_study_ids_combined_with_other_filters(self, gateway, base_request):
        base_request.study_ids = ["MTBLS1"]
        base_request.filters = [
            FilterModel(field="organisms", values=["Homo sapiens"], operator="any")
        ]
        payload = gateway._build_search_payload(base_request)

        bool_query = payload["query"]["bool"]
        assert {"terms": {"studyId": ["MTBLS1"]}} in bool_query["filter"]
        # Should have at least 2 filter clauses
        assert len(bool_query["filter"]) >= 2
