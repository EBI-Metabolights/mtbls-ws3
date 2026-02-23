import logging
import re
from collections.abc import AsyncIterator
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

from mtbls.application.services.interfaces.search_port import (
    BaseElasticSearchGateway,
)
from mtbls.domain.entities.search.compound.facet_configuration import (
    COMPOUND_FACET_CONFIG,
)
from mtbls.domain.entities.search.index_search import (
    CompoundSearchInput,
    IndexSearchResult,
)
from mtbls.infrastructure.search.es.compound.es_compound_configuration import (
    CompoundElasticSearchConfiguration,
)
from mtbls.infrastructure.search.es.es_client import ElasticsearchClient

# Pattern to detect MetaboLights study IDs in query strings
MTBLS_STUDY_ID_PATTERN = re.compile(r"^MTBLS\d+$", re.IGNORECASE)

logger = logging.getLogger(__name__)

# Export defaults
EXPORT_MAX_RESULTS = 10_000
EXPORT_BATCH_SIZE = 500


class ElasticsearchCompoundGateway(BaseElasticSearchGateway):
    def __init__(
        self,
        client: ElasticsearchClient,
        config: None | CompoundElasticSearchConfiguration | dict[str, Any],
    ):
        self._client = client
        self._config = config
        if not self._config:
            self._config = CompoundElasticSearchConfiguration()
        elif isinstance(self._config, dict):
            self._config = CompoundElasticSearchConfiguration.model_validate(config)
        super().__init__()

    @property
    def config(self) -> CompoundElasticSearchConfiguration:
        return self._config

    @staticmethod
    def _extract_study_ids_from_query(
        query: Optional[str],
    ) -> Tuple[Optional[List[str]], Optional[str]]:
        """
        Detect if query is a MetaboLights study ID pattern.

        Returns:
            Tuple of (detected_study_ids, remaining_query).
            - If query matches MTBLS pattern: ([study_id], None)
            - Otherwise: (None, original_query)
        """
        if not query:
            return None, query
        query_stripped = query.strip()
        if MTBLS_STUDY_ID_PATTERN.match(query_stripped):
            # Normalize to uppercase for consistent matching
            return [query_stripped.upper()], None
        return None, query

    async def get_index_mapping(self) -> dict[str, Any]:
        """
        Return the ES mapping for the configured index.
        """
        mapping = await self._client.get_mapping(
            self.config.index_name, api_key_name=self.config.api_key_name
        )
        return mapping.get(self.config.index_name, mapping)

    async def search(
        self,
        query: CompoundSearchInput,
        raw: bool = False,
    ) -> Any:
        dsl = self._build_search_payload(query)
        es_resp = await self._client.search(
            index=self.config.index_name,
            body=dsl,
            api_key_name=self.config.api_key_name,
        )

        if raw:
            return es_resp

        results = [self._map_hit(h) for h in es_resp.get("hits", {}).get("hits", [])]
        total = self._extract_total(es_resp)
        facets = self._map_aggs_to_searchui(es_resp.get("aggregations") or {})

        return IndexSearchResult(
            results=results,
            totalResults=total,
            facets=facets,
            requestId=str(uuid4()),  # useless currently
        )

    async def export_results(
        self,
        query: CompoundSearchInput,
        max_results: int = EXPORT_MAX_RESULTS,
        batch_size: int = EXPORT_BATCH_SIZE,
    ) -> AsyncIterator[Dict[str, Any]]:
        """
        Yield all matching compound documents using search_after pagination.
        """
        dsl = self._build_search_payload(query)
        dsl.pop("aggs", None)
        dsl.pop("from", None)
        dsl.pop("_source", None)
        dsl["size"] = min(batch_size, max_results)
        dsl["sort"] = [{"_doc": "asc"}]
        dsl["track_total_hits"] = True
        dsl["timeout"] = "30s"

        yielded = 0
        search_after = None

        while yielded < max_results:
            if search_after is not None:
                dsl["search_after"] = search_after

            remaining = max_results - yielded
            dsl["size"] = min(batch_size, remaining)

            es_resp = await self._client.search(
                index=self.config.index_name,
                body=dsl,
                api_key_name=self.config.api_key_name,
            )

            hits = es_resp.get("hits", {}).get("hits", [])
            if not hits:
                break

            for hit in hits:
                yield self._map_hit(hit)
                yielded += 1
                if yielded >= max_results:
                    break

            search_after = hits[-1].get("sort")
            if not search_after:
                break

    def _build_search_payload(self, req: CompoundSearchInput) -> Dict[str, Any]:
        """Builds the Elasticsearch DSL query payload.

        Supports:
        - Text search across configured fields
        - Numeric query scoring (Gaussian decay on mass fields)
        - Study ID filtering via explicit `study_ids` param or auto-detection of MTBLS pattern
        - Facet filtering with all/any/none operators

        Args:
            req (CompoundSearchInput): Search input, term, pagination, sorting, filters.

        Returns:
            Dict[str, Any]: Elasticsearch DSL payload.
        """
        must: List[Dict[str, Any]] = []
        filter_clauses: List[Dict[str, Any]] = []
        must_not: List[Dict[str, Any]] = []
        numeric_query: float | None = None

        # --- Study ID handling ---
        # Priority: explicit study_ids param > auto-detected pattern in query
        effective_study_ids: Optional[List[str]] = None
        effective_query: Optional[str] = req.query

        if req.study_ids:
            # Explicit param provided - normalize to uppercase
            effective_study_ids = [sid.upper() for sid in req.study_ids]
        else:
            # Check if query looks like a study ID
            detected_ids, remaining_query = self._extract_study_ids_from_query(
                req.query
            )
            if detected_ids:
                effective_study_ids = detected_ids
                effective_query = remaining_query  # None if query was just a study ID

        # Add study_ids filter if we have any
        if effective_study_ids:
            # Respect study_ids_operator: "any" uses terms; "all" requires every ID
            operator = req.study_ids_operator or "any"
            if operator == "all":
                filter_clauses.append(
                    {
                        "bool": {
                            "must": [
                                {"term": {"studyIds": sid}}
                                for sid in effective_study_ids
                            ]
                        }
                    }
                )
            else:
                filter_clauses.append({"terms": {"studyIds": effective_study_ids}})

        # --- Text/numeric query handling ---
        if effective_query:
            try:
                numeric_query = float(effective_query)
            except (TypeError, ValueError):
                numeric_query = None
            # Only run text matching if this isn't purely numeric; numeric searches rely on scoring functions.
            if numeric_query is None:
                must.append(
                    {
                        "bool": {
                            "should": [
                                {
                                    "multi_match": {
                                        "query": effective_query,
                                        "fields": list(self.config.search_fields),
                                        "operator": "and",
                                    }
                                },
                                # prefix match (fast)
                                *[
                                    {"prefix": {field: effective_query.lower()}}
                                    for field in self.config.search_fields
                                ],
                            ],
                            "minimum_should_match": 1,
                        }
                    }
                )

        # Filters (simple value filters; operator all/any/none)
        for f in req.filters:
            if not f.values:
                continue
            spec = COMPOUND_FACET_CONFIG.get(f.field) or {}
            if spec.get("type") == "range":
                ranges = spec.get("ranges") or []
                range_queries: List[Dict[str, Any]] = []
                for v in f.values:
                    rq = self._range_query_from_value(
                        spec.get("field") or f.field, ranges, v
                    )
                    if rq:
                        range_queries.append(rq)
                if not range_queries:
                    continue
                if f.operator == "none":
                    must_not.extend(range_queries)
                elif f.operator == "any":
                    filter_clauses.append(
                        {"bool": {"should": range_queries, "minimum_should_match": 1}}
                    )
                else:  # "all"
                    filter_clauses.extend(range_queries)
            else:
                target_field = spec.get("field") or f.field
                # Only coerce boolean-like values for facets that are explicitly boolean (only_true flag).
                if spec.get("only_true"):
                    normalized_values = [self._normalize_bool(v) for v in f.values]
                else:
                    normalized_values = list(f.values)
                if f.operator == "none":
                    must_not.append({"terms": {target_field: normalized_values}})
                elif f.operator == "any":
                    filter_clauses.append({"terms": {target_field: normalized_values}})
                else:  # "all"
                    for v in normalized_values:
                        filter_clauses.append({"term": {target_field: v}})

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

        base_query: Dict[str, Any] = {
            "track_total_hits": True,
            "from": from_,
            "size": size,
            "sort": sort_clause,
        }

        bool_query = {
            "bool": {
                "must": must or [{"match_all": {}}],
                "filter": filter_clauses,
                "must_not": must_not,
            }
        }

        if numeric_query is not None:
            base_query["query"] = {
                "function_score": {
                    "query": bool_query,
                    "functions": [
                        {
                            "gauss": {
                                "exactmass": {
                                    "origin": numeric_query,
                                    "scale": "1",
                                    "offset": 0,
                                }
                            },
                            "weight": 2,
                        },
                        {
                            "gauss": {
                                "averagemass": {
                                    "origin": numeric_query,
                                    "scale": "1",
                                    "offset": 0,
                                }
                            },
                            "weight": 1.5,
                        },
                    ],
                    "score_mode": "sum",
                    "boost_mode": "sum",
                }
            }
        else:
            base_query["query"] = bool_query

        if aggs:
            base_query["aggs"] = aggs

        if self.config.source_includes:
            base_query["_source"] = {"includes": list(self._config.source_includes)}

        base_query.setdefault("timeout", "3s")

        return base_query

    @staticmethod
    def _range_query_from_value(
        field: str, ranges: List[Dict[str, Any]], value: Any
    ) -> Dict[str, Any] | None:
        """
        Map a selected bucket name back to its range query.
        Matches either the explicit 'name' or the derived _range_key form.
        """
        for r in ranges:
            name = r.get("name")
            if value == name or value == BaseElasticSearchGateway._range_key(r):
                return BaseElasticSearchGateway._range_query(field, r)
        return None

    @staticmethod
    def _normalize_bool(value: Any) -> Any:
        """
        Coerce common truthy/falsy representations to booleans; otherwise return original.
        """
        if isinstance(value, bool):
            return value
        if isinstance(value, int):
            if value == 1:
                return True
            if value == 0:
                return False
        if isinstance(value, str):
            v = value.strip().lower()
            if v in ("1", "true", "yes", "y"):
                return True
            if v in ("0", "false", "no", "n"):
                return False
        return value

    def _build_aggs(self) -> Dict[str, Any]:
        """
        Supports server-side facet config:
        FACET_CONFIG = {
            "organisms": {
                "type": "value",
                "field": "organisms.keyword",
                "size": 20,
            },
            "species": {
                "type": "value",
                "field": "species_hits.species",
                "nested_path": "species_hits",
            },
            ...
        }
        """

        aggs: Dict[str, Any] = {}

        for facet_name, spec in COMPOUND_FACET_CONFIG.items():
            if not spec:
                continue

            ftype = spec.get("type")
            field = spec.get("field") or facet_name
            nested_path = spec.get("nested_path")

            # ---- VALUE FACETS -------------------------------------------------
            if ftype == "value":
                terms_agg = {
                    "terms": {
                        "field": field,
                        "size": spec.get("size") or self.config.facet_size,
                        "order": {"_count": "desc"},
                    }
                }

                if nested_path:
                    aggs[facet_name] = {
                        "nested": {"path": nested_path},
                        "aggs": {
                            "values": terms_agg,  # 👈 inner name used in mapper
                        },
                    }
                else:
                    aggs[facet_name] = terms_agg

            # ---- RANGE FACETS -------------------------------------------------
            elif ftype == "range":
                ranges = spec.get("ranges") or []
                filters_agg = {
                    "filters": {
                        "filters": {
                            self._range_key(r): self._range_query(field, r)
                            for r in ranges
                        }
                    }
                }

                if nested_path:
                    aggs[facet_name] = {
                        "nested": {"path": nested_path},
                        "aggs": {
                            "ranges": filters_agg,  # 👈 inner name used in mapper
                        },
                    }
                else:
                    aggs[facet_name] = filters_agg

            else:
                continue

        return aggs

    def _map_aggs_to_searchui(self, aggs: Dict[str, Any]) -> Dict[str, Any]:
        out: Dict[str, Any] = {}

        for facet_name, spec in COMPOUND_FACET_CONFIG.items():
            ftype = spec.get("type")
            nested_path = spec.get("nested_path")
            agg_resp = aggs.get(facet_name)
            if not agg_resp:
                continue

            # If this facet was nested, the actual buckets are under the inner agg
            if nested_path:
                if ftype == "value":
                    agg_resp = agg_resp.get("values", {})
                elif ftype == "range":
                    agg_resp = agg_resp.get("ranges", {})
                # if we still have nothing, skip
                if not agg_resp:
                    continue

            buckets: List[Dict[str, Any]] = []

            if ftype == "value":
                only_true = spec.get("only_true", False)
                for b in agg_resp.get("buckets", []):
                    key = b.get("key")
                    if only_true and not self._is_true_bucket(key):
                        continue
                    buckets.append({"value": key, "count": int(b.get("doc_count", 0))})

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
