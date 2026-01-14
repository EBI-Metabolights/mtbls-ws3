from typing import Any, Dict, List
from uuid import uuid4

from mtbls.application.services.interfaces.search_port import BaseElasticSearchGateway, SearchPort
from mtbls.domain.entities.search.study.facet_configuration import STUDY_FACET_CONFIG
from mtbls.domain.entities.search.index_search import (
    IndexSearchInput,
    IndexSearchResult,
)
from mtbls.infrastructure.search.es.es_client import ElasticsearchClient
from mtbls.infrastructure.search.es.es_configuration import (
    StudyElasticSearchConfiguration,
)


class ElasticsearchStudyGateway(BaseElasticSearchGateway):
    def __init__(
        self,
        client: ElasticsearchClient,
        config: None | StudyElasticSearchConfiguration | dict[str, Any],
    ):
        self._client = client
        self._config = config
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
        query: IndexSearchInput,
        raw: bool = False,
    ) -> IndexSearchResult | Dict[str, Any]:
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
        facets = self._map_aggs_to_searchui(es_resp.get("aggregations") or {}, STUDY_FACET_CONFIG)

        return IndexSearchResult(
            results=results,
            totalResults=total,
            facets=facets,
            requestId=str(uuid4()),  # useless currently
        )

    def _build_search_payload(self, req: IndexSearchInput) -> Dict[str, Any]:
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
        if field in FACET_CONFIG:
            spec = FACET_CONFIG[field] or {}
            return spec.get("field") or field, spec.get("nested_path")

        for fname, spec in FACET_CONFIG.items():
            if not spec:
                continue
            if spec.get("field") == field:
                return field, spec.get("nested_path")

        return field, None

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

    def _map_aggs_to_searchui(self, aggs: Dict[str, Any]) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        nested_bucket_name = "values"

        for facet_name, spec in FACET_CONFIG.items():
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
