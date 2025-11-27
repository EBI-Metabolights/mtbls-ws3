from typing import Any, Dict, List
from uuid import uuid4

from mtbls.application.services.interfaces.search_port import SearchPort
from mtbls.domain.entities.search.study.facet_configuration import FACET_CONFIG
from mtbls.domain.entities.search.study.index_search import (
    IndexSearchInput,
    IndexSearchResult,
)
from mtbls.infrastructure.search.es.es_client import ElasticsearchClient
from mtbls.infrastructure.search.es.es_configuration import (
    StudyElasticSearchConfiguration,
)


class ElasticsearchStudyGateway(SearchPort):
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
        mapping = await self._client.get_mapping(self.config.index_name)
        return mapping.get(self.config.index_name, mapping)

    async def search(
        self,
        query: IndexSearchInput,
        raw: bool = False,
    ) -> IndexSearchResult | Dict[str, Any]:
        dsl = self._build_search_payload(query)
        es_resp = await self._client.search(index=self.config.index_name, body=dsl)

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

    def _build_search_payload(self, req: IndexSearchInput) -> Dict[str, Any]:
        must: List[Dict[str, Any]] = []
        filter_clauses: List[Dict[str, Any]] = []
        must_not: List[Dict[str, Any]] = []
        if req.query:
            must.append(
                {
                    "bool": {
                        "should": [
                            {
                                "multi_match": {
                                    "query": req.query,
                                    "fields": list(self.config.search_fields),
                                    "operator": "and",
                                }
                            },
                            # prefix match (fast)
                            *[
                                {"prefix": {field: req.query.lower()}}
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
            if f.operator == "none":
                must_not.append({"terms": {f.field: f.values}})
            elif f.operator == "any":
                filter_clauses.append({"terms": {f.field: f.values}})
            else:  # "all"
                for v in f.values:
                    filter_clauses.append({"term": {f.field: v}})

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

    def _build_aggs(self) -> Dict[str, Any]:
        """
        Supports server-side facet config:
        FACET_CONFIG = {
            "organisms": {
                "type": "value",
                "field": "organisms.keyword",
                "size": 20,
            },
            ...
        }
        """

        aggs: Dict[str, Any] = {}

        for facet_name, spec in FACET_CONFIG.items():
            if not spec:
                continue

            ftype = spec.get("type")
            # allow config to alias an underlying field; fall back to facet_name
            field = spec.get("field") or facet_name

            if ftype == "value":
                aggs[facet_name] = {
                    "terms": {
                        "field": field,  # 👈 WAS facet_name
                        "size": spec.get("size") or self.config.facet_size,
                        "order": {"_count": "desc"},
                    }
                }

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
    def _extract_total(es_resp: Dict[str, Any]) -> int:
        total = es_resp.get("hits", {}).get("total", 0)
        if isinstance(total, dict):
            return int(total.get("value", 0))
        return int(total)

    @staticmethod
    def _map_hit(hit: Dict[str, Any]) -> Dict[str, Any]:
        # Return _source; you can enrich with highlight/_id if desired
        src = hit.get("_source") or {}
        # Keep a stable URL/id if helpful
        if "_id" in hit and "id" not in src:
            src["id"] = hit["_id"]
        return src

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

        for facet_name, spec in FACET_CONFIG.items():
            ftype = spec.get("type")
            agg_resp = aggs.get(facet_name)
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
