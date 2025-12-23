

from typing import Any, Dict, List
from uuid import uuid4
from mtbls.application.services.interfaces.search_port import BaseElasticSearchGateway, SearchPort
from mtbls.domain.entities.search.compound.facet_configuration import COMPOUND_FACET_CONFIG
from mtbls.domain.entities.search.index_search import IndexSearchInput, IndexSearchResult
from mtbls.infrastructure.search.es.compound.es_compound_configuration import CompoundElasticSearchConfiguration
from mtbls.infrastructure.search.es.es_client import ElasticsearchClient



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
        query: IndexSearchInput,
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


    def _build_search_payload(self, req: IndexSearchInput) -> Dict[str, Any]:
        """" This is a carbon copy of the method from the search gateway for studies.
        At some point the logic for searching compounds may change. We could maybe refactor this
        logic to be available in the base class, to be later overridden.

        Args:
            req (IndexSearchInput): Search input, term, pagination, sorting, filters.

        Returns:
            Dict[str, Any]: Elasticsearch DSL payload.
        """
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
