import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional
from uuid import uuid4

from mtbls.application.services.interfaces.search_port import BaseElasticSearchGateway, SearchPort
from mtbls.domain.entities.search.study.facet_configuration import STUDY_FACET_CONFIG
from mtbls.domain.entities.search.index_search import (
    IndexSearchResult,
    StudySearchInput,
)
from mtbls.infrastructure.search.es.es_client import ElasticsearchClient
from mtbls.infrastructure.search.es.es_configuration import (
    StudyElasticSearchConfiguration,
)

if TYPE_CHECKING:
    from mtbls.infrastructure.search.es.assignment.es_assignment_search_gateway import (
        ElasticsearchAssignmentGateway,
    )

# Maximum number of study IDs to return in all_study_ids
ALL_IDS_MAX_SIZE = 100
logger = logging.getLogger(__name__)


class ElasticsearchStudyGateway(BaseElasticSearchGateway):
    def __init__(
        self,
        client: ElasticsearchClient,
        config: None | StudyElasticSearchConfiguration | dict[str, Any],
        assignment_gateway: Optional["ElasticsearchAssignmentGateway"] = None,
    ):
        self._client = client
        self._config = config
        self._assignment_gateway = assignment_gateway
        if not self._config:
            self._config = StudyElasticSearchConfiguration()
        elif isinstance(self._config, dict):
            self._config = StudyElasticSearchConfiguration.model_validate(config)
        super().__init__()

    @property
    def config(self) -> StudyElasticSearchConfiguration:
        return self._config

    async def get_index_mapping(self) -> Dict[str, Any]:
        """
        Return the ES mapping for the configured index.
        """
        mapping = await self._client.get_mapping(
            self.config.index_name, api_key_name=self.config.api_key_name
        )
        return mapping.get(self.config.index_name, mapping)

    async def search(
        self,
        query: StudySearchInput,
        raw: bool = False,
        include_all_ids: bool = False,
    ) -> IndexSearchResult | Dict[str, Any]:
        # Track if we can reuse chemical filter study_ids for all_study_ids response
        # (optimization to skip redundant ES query when only chemical filters are used)
        chemical_filter_study_ids: Optional[List[str]] = None
        had_chemical_filters = self._has_chemical_filters(query)

        # Resolve chemical filters to study IDs if present
        resolved_query = await self._resolve_chemical_filters(query)

        # Capture the resolved study_ids if chemical filters were applied
        if had_chemical_filters and resolved_query.study_ids:
            # Filter out the sentinel value used for "no matches"
            if resolved_query.study_ids != ["__NO_MATCHING_STUDIES__"]:
                chemical_filter_study_ids = resolved_query.study_ids

        dsl = self._build_search_payload(resolved_query)
        logger.debug("Study search main query starting")
        es_resp = await self._client.search(
            index=self.config.index_name,
            body=dsl,
            api_key_name=self.config.api_key_name,
        )
        logger.debug("Study search main query completed")

        if raw:
            return es_resp

        results = [self._map_hit(h) for h in es_resp.get("hits", {}).get("hits", [])]
        total = self._extract_total(es_resp)
        facets = self._map_aggs_to_searchui(es_resp.get("aggregations") or {}, STUDY_FACET_CONFIG)

        # Fetch all matching study IDs if requested and there's a meaningful query/filter
        all_study_ids: Optional[List[str]] = None
        if include_all_ids and self._has_query_or_filters(resolved_query):
            # Optimization: if ONLY chemical filters were used (no text query, no facet filters),
            # we can reuse the study_ids from assignment lookup instead of querying ES again
            if chemical_filter_study_ids and self._only_has_chemical_filters(query):
                logger.debug(
                    "Study search all_ids query skipped - reusing %d IDs from assignment lookup",
                    len(chemical_filter_study_ids),
                )
                all_study_ids = chemical_filter_study_ids[:ALL_IDS_MAX_SIZE]
            else:
                logger.debug("Study search all_ids query starting")
                all_study_ids = await self._fetch_all_matching_ids(resolved_query)
                logger.debug("Study search all_ids query completed")

        return IndexSearchResult(
            results=results,
            totalResults=total,
            facets=facets,
            requestId=str(uuid4()),  # useless currently
            all_study_ids=all_study_ids,
        )

    def _has_chemical_filters(self, req: StudySearchInput) -> bool:
        """Check if the request has chemical filters (database identifiers or metabolite identifications)."""
        if req.database_identifiers:
            return True
        if req.metabolite_identifications:
            return True
        return False

    def _only_has_chemical_filters(self, req: StudySearchInput) -> bool:
        """
        Check if the request ONLY has chemical filters - no text query and no facet filters.

        When this is true and chemical filters resolved to study_ids, we can skip
        the all_study_ids ES query and reuse the assignment lookup results directly.
        """
        # Has text query? Then we need ES to filter further
        if req.query and req.query.strip():
            return False

        # Has facet filters with values? Then we need ES to filter further
        if req.filters:
            for f in req.filters:
                if f.values:
                    return False

        # Has explicit study_ids constraint (not from chemical filter resolution)?
        # This would mean intersection is needed
        if req.study_ids:
            return False

        # Only chemical filters remain
        return self._has_chemical_filters(req)

    async def _resolve_chemical_filters(self, req: StudySearchInput) -> StudySearchInput:
        """
        If chemical filters are present, query the assignment gateway to get matching study IDs,
        then add them as study_ids filter on the request.

        Returns a new StudySearchInput with resolved study_ids if chemical filters were present,
        or the original request unchanged if not.
        """
        if not self._has_chemical_filters(req):
            return req

        if not self._assignment_gateway:
            # No assignment gateway configured - skip chemical filter resolution
            return req

        logger.debug("Study search assignment lookup starting")
        # Query assignment index for study IDs matching the chemical filters
        matching_study_ids = await self._assignment_gateway.find_study_ids_by_compounds(
            database_identifiers=req.database_identifiers,
            metabolite_identifications=req.metabolite_identifications,
            database_identifiers_operator=req.database_identifiers_operator,
            metabolite_identifications_operator=req.metabolite_identifications_operator,
        )
        logger.debug(
            "Study search assignment lookup completed (matches=%s)",
            len(matching_study_ids) if matching_study_ids is not None else 0,
        )

        if not matching_study_ids:
            # No studies match the chemical filters - return impossible filter
            # by setting study_ids to a non-existent ID to ensure zero results
            return req.model_copy(
                update={
                    "study_ids": ["__NO_MATCHING_STUDIES__"],
                    "database_identifiers": None,  # Already resolved
                    "metabolite_identifications": None,  # Already resolved
                }
            )

        # Combine with any existing study_ids filter (intersection)
        if req.study_ids:
            # Intersect: only keep study IDs that are in both lists
            existing_set = set(req.study_ids)
            combined_study_ids = [sid for sid in matching_study_ids if sid in existing_set]
            if not combined_study_ids:
                # No intersection - return impossible filter
                combined_study_ids = ["__NO_MATCHING_STUDIES__"]
        else:
            combined_study_ids = matching_study_ids

        return req.model_copy(
            update={
                "study_ids": combined_study_ids,
                "database_identifiers": None,  # Already resolved
                "metabolite_identifications": None,  # Already resolved
            }
        )

    def _build_search_payload(self, req: StudySearchInput) -> Dict[str, Any]:
        must: List[Dict[str, Any]] = []
        filter_clauses: List[Dict[str, Any]] = []
        must_not: List[Dict[str, Any]] = []
        if req.query:
            should_clauses: List[Dict[str, Any]] = []
            should_clauses.append(
                {
                    "multi_match": {
                        "query": req.query,
                        "fields": list(self.config.search_fields),
                        "operator": "and",
                    }
                }
            )
            # prefix match (fast) on non-nested fields
            should_clauses.extend(
                [{"prefix": {field: req.query.lower()}} for field in self.config.search_fields]
            )
            # nested-aware search to keep simple queries working across nested docs
            should_clauses.extend(self._nested_search_clauses(req.query))
            must.append(
                {
                    "bool": {
                        "should": should_clauses,
                        "minimum_should_match": 1,
                    }
                }
            )

        # Filters (simple value filters; operator all/any/none)
        for f in req.filters:
            if not f.values:
                continue
            spec = self._find_facet_spec(f.field)
            # Range facets need to be mapped back to range queries (buckets are keyed by name or range key)
            if spec and spec.get("type") == "range":
                ranges = spec.get("ranges") or []
                target_field = spec.get("field") or f.field
                range_queries: List[Dict[str, Any]] = []
                for v in f.values:
                    rq = self._range_query_from_value(target_field, ranges, v)
                    if rq:
                        range_queries.append(rq)
                if not range_queries:
                    continue
                if f.operator == "none":
                    must_not.extend(range_queries)
                elif f.operator == "any":
                    filter_clauses.append({"bool": {"should": range_queries, "minimum_should_match": 1}})
                else:  # "all"
                    filter_clauses.extend(range_queries)
                continue

            field, nested_path = self._resolve_filter_field(f.field)
            if not field:
                continue
            operator = f.operator or "all"
            if operator == "none":
                must_not.append(self._build_filter_clause(field, f.values, nested_path, operator))
            elif operator == "any":
                filter_clauses.append(self._build_filter_clause(field, f.values, nested_path, operator))
            else:  # "all"
                # For nested fields we deliberately pool across nested docs by adding one clause per value.
                clauses = self._build_filter_clause(field, f.values, nested_path, operator)
                if isinstance(clauses, list):
                    filter_clauses.extend(clauses)
                else:
                    filter_clauses.append(clauses)

        # Study IDs filter (from resolved chemical filters or explicit study_ids parameter)
        if req.study_ids:
            filter_clauses.append({"terms": {"studyId": req.study_ids}})

        # Sort
        sort_clause: List[Any] = []
        if req.sort:
            sort_clause.append({req.sort.field: {"order": req.sort.direction}})
        else:
            # optional deterministic tiebreaker
            sort_clause.append({"_score": "desc"})

        # Pagination (from/size; simple & fine for first iteration)
        size = req.page.size
        from_ = (req.page.current - 1) * size

        # Aggregations (value + range based on UI facets config)
        aggs = self._build_aggs()

        dsl: Dict[str, Any] = {
            "track_total_hits": True,
            "from": from_,
            "size": size,
            "query": {
                "bool": {
                    "must": must or [{"match_all": {}}],
                    "filter": filter_clauses,
                    "must_not": must_not,
                }
            },
            "sort": sort_clause,
        }

        if aggs:
            dsl["aggs"] = aggs

        if self.config.source_includes:
            dsl["_source"] = {"includes": list(self._config.source_includes)}

        dsl.setdefault("timeout", "3s")

        return dsl

    def _has_query_or_filters(self, req: StudySearchInput) -> bool:
        """Check if the request has a text query, filters, or study_ids applied."""
        if req.query and req.query.strip():
            return True
        if req.filters:
            for f in req.filters:
                if f.values:
                    return True
        if req.study_ids:
            return True
        return False

    async def _fetch_all_matching_ids(self, req: StudySearchInput) -> List[str]:
        """
        Fetch all study IDs matching the query/filters (up to ALL_IDS_MAX_SIZE).
        Uses the same query logic but with minimal _source and no aggregations.
        """
        dsl = self._build_ids_only_payload(req)
        es_resp = await self._client.search(
            index=self.config.index_name,
            body=dsl,
            api_key_name=self.config.api_key_name,
        )

        study_ids: List[str] = []
        for hit in es_resp.get("hits", {}).get("hits", []):
            source = hit.get("_source", {})
            study_id = source.get("studyId")
            if study_id:
                study_ids.append(study_id)

        return study_ids

    def _build_ids_only_payload(self, req: StudySearchInput) -> Dict[str, Any]:
        """
        Build a payload for fetching only study IDs (no aggregations, minimal source).
        Uses the same query/filter logic as _build_search_payload.
        """
        must: List[Dict[str, Any]] = []
        filter_clauses: List[Dict[str, Any]] = []
        must_not: List[Dict[str, Any]] = []

        if req.query:
            should_clauses: List[Dict[str, Any]] = []
            should_clauses.append(
                {
                    "multi_match": {
                        "query": req.query,
                        "fields": list(self.config.search_fields),
                        "operator": "and",
                    }
                }
            )
            should_clauses.extend(
                [{"prefix": {field: req.query.lower()}} for field in self.config.search_fields]
            )
            should_clauses.extend(self._nested_search_clauses(req.query))
            must.append(
                {
                    "bool": {
                        "should": should_clauses,
                        "minimum_should_match": 1,
                    }
                }
            )

        for f in req.filters:
            if not f.values:
                continue
            spec = self._find_facet_spec(f.field)
            if spec and spec.get("type") == "range":
                ranges = spec.get("ranges") or []
                target_field = spec.get("field") or f.field
                range_queries: List[Dict[str, Any]] = []
                for v in f.values:
                    rq = self._range_query_from_value(target_field, ranges, v)
                    if rq:
                        range_queries.append(rq)
                if not range_queries:
                    continue
                if f.operator == "none":
                    must_not.extend(range_queries)
                elif f.operator == "any":
                    filter_clauses.append({"bool": {"should": range_queries, "minimum_should_match": 1}})
                else:
                    filter_clauses.extend(range_queries)
                continue

            field, nested_path = self._resolve_filter_field(f.field)
            if not field:
                continue
            operator = f.operator or "all"
            if operator == "none":
                must_not.append(self._build_filter_clause(field, f.values, nested_path, operator))
            elif operator == "any":
                filter_clauses.append(self._build_filter_clause(field, f.values, nested_path, operator))
            else:
                clauses = self._build_filter_clause(field, f.values, nested_path, operator)
                if isinstance(clauses, list):
                    filter_clauses.extend(clauses)
                else:
                    filter_clauses.append(clauses)

        # Study IDs filter (from resolved chemical filters or explicit study_ids parameter)
        if req.study_ids:
            filter_clauses.append({"terms": {"studyId": req.study_ids}})

        dsl: Dict[str, Any] = {
            "track_total_hits": False,  # Not needed for ID fetch
            "from": 0,
            "size": ALL_IDS_MAX_SIZE,
            "_source": ["studyId"],
            "query": {
                "bool": {
                    "must": must or [{"match_all": {}}],
                    "filter": filter_clauses,
                    "must_not": must_not,
                }
            },
            "sort": [{"studyId": "asc"}],  # Deterministic ordering
        }

        dsl.setdefault("timeout", "10s")  # Allow more time for large result set

        return dsl

    def _nested_search_clauses(self, query: str) -> List[Dict[str, Any]]:
        clauses: List[Dict[str, Any]] = []
        for path, fields in self.config.nested_search_fields:
            clauses.append(
                {
                    "nested": {
                        "path": path,
                        "score_mode": "avg",
                        "query": {
                            "multi_match": {
                                "query": query,
                                "fields": list(fields),
                                "operator": "and",
                            }
                        },
                    }
                }
            )
        return clauses

    def _resolve_filter_field(self, field: str) -> tuple[str | None, str | None]:
        """
        Given a UI filter field, resolve it to an ES field path and whether it is nested.
        """
        spec = self._find_facet_spec(field)
        if spec:
            return spec.get("field") or field, spec.get("nested_path")
        return field, None

    @staticmethod
    def _find_facet_spec(field: str, facet_config: Dict[str, Any] | None = None) -> Dict[str, Any] | None:
        """Return the facet spec for a given UI field or underlying field."""
        config = facet_config or STUDY_FACET_CONFIG
        if field in config:
            return config[field] or {}
        for _, spec in config.items():
            if not spec:
                continue
            if spec.get("field") == field:
                return spec
        return None

    def _build_filter_clause(
        self, field: str, values: List[Any], nested_path: str | None, operator: str
    ) -> Dict[str, Any] | List[Dict[str, Any]]:
        if not nested_path:
            if operator == "none":
                return {"terms": {field: values}}
            if operator == "any":
                return {"terms": {field: values}}
            return [{"term": {field: v}} for v in values]

        # Nested filters: pooled across nested docs. Each value gets its own nested clause for "all",
        # so a doc matches if the values are present anywhere within the nested set (not necessarily the same item).
        if operator == "none":
            return {
                "nested": {
                    "path": nested_path,
                    "query": {"terms": {field: values}},
                    "score_mode": "avg",
                }
            }
        if operator == "any":
            return {
                "nested": {
                    "path": nested_path,
                    "query": {"terms": {field: values}},
                    "score_mode": "avg",
                }
            }
        return [
            {
                "nested": {
                    "path": nested_path,
                    "query": {"term": {field: v}},
                    "score_mode": "avg",
                }
            }
            for v in values
        ]

    def _build_aggs(self) -> Dict[str, Any]:
        """
        Supports server-side facet config:
        FACET_CONFIG = {
            "organisms": {
                "type": "value",
                "field": "organisms.term",
                "nested_path": "organisms",
                "size": 20,
            },
            ...
        }
        """

        aggs: Dict[str, Any] = {}
        nested_bucket_name = "values"

        for facet_name, spec in STUDY_FACET_CONFIG.items():
            if not spec:
                continue

            ftype = spec.get("type")
            # allow config to alias an underlying field; fall back to facet_name
            field = spec.get("field") or facet_name
            nested_path = spec.get("nested_path")

            if ftype == "value":
                terms_body = {
                    "field": field,
                    "size": spec.get("size") or self.config.facet_size,
                    "order": {"_count": "desc"},
                }
                if nested_path:
                    aggs[facet_name] = {
                        "nested": {"path": nested_path},
                        "aggs": {nested_bucket_name: {"terms": terms_body}},
                    }
                else:
                    aggs[facet_name] = {"terms": terms_body}

            elif ftype == "range":
                ranges = spec.get("ranges") or []
                aggs[facet_name] = {
                    "filters": {
                        "filters": {
                            self._range_key(r): self._range_query(field, r)
                            for r in ranges
                        }
                    }
                }

            else:
                continue

        return aggs



    @staticmethod
    def _range_key(r: Dict[str, Any]) -> str:
        name = r.get("name")
        if name:
            return str(name)
        start = r.get("from")
        end = r.get("to")
        if start is not None and end is not None:
            return f"{start}..{end}"
        if start is not None:
            return f"{start}+"
        if end is not None:
            return f"..{end}"
        return "range"

    @staticmethod
    def _range_query(field: str, r: Dict[str, Any]) -> Dict[str, Any]:
        q: Dict[str, Any] = {"range": {field: {}}}
        if "from" in r and r["from"] is not None:
            q["range"][field]["gte"] = r["from"]
        if "to" in r and r["to"] is not None:
            q["range"][field]["lte"] = r["to"]
        return q

    @staticmethod
    def _range_query_from_value(field: str, ranges: List[Dict[str, Any]], value: Any) -> Dict[str, Any] | None:
        """
        Map a selected bucket name back to its range query.
        Matches either the explicit 'name' or the derived _range_key form.
        """
        for r in ranges:
            name = r.get("name")
            if value == name or value == BaseElasticSearchGateway._range_key(r):
                return BaseElasticSearchGateway._range_query(field, r)
        return None

    def _map_aggs_to_searchui(self, aggs: Dict[str, Any], config: Dict[str, Any] | None = None) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        nested_bucket_name = "values"
        facet_config = config or STUDY_FACET_CONFIG

        for facet_name, spec in facet_config.items():
            ftype = spec.get("type")
            agg_resp = aggs.get(facet_name)
            if not agg_resp:
                continue
            if spec.get("nested_path"):
                agg_resp = agg_resp.get(nested_bucket_name)
                if not agg_resp:
                    continue

            buckets: List[Dict[str, Any]] = []

            if ftype == "value":
                for b in agg_resp.get("buckets", []):
                    buckets.append(
                        {
                            "value": b.get("key"),
                            "count": int(b.get("doc_count", 0)),
                        }
                    )
            elif ftype == "range":
                for key, b in (agg_resp.get("buckets") or {}).items():
                    buckets.append(
                        {
                            "value": key,
                            "count": int(b.get("doc_count", 0)),
                        }
                    )
            else:
                continue

            out[facet_name] = [
                {
                    "type": ftype,
                    "data": buckets,
                }
            ]

        return out
