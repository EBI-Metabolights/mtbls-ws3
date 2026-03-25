import abc
from typing import Any, Dict, List
from uuid import uuid4

from mtbls.domain.entities.search.index_search import (
    BaseSearchInput,
    IndexSearchInput,
    IndexSearchResult,
)


class SearchPort(abc.ABC):
    @abc.abstractmethod
    async def search(
        self,
        query: BaseSearchInput,
        raw: bool = False,
    ) -> IndexSearchResult | Dict[str, Any]: ...


class BaseElasticSearchGateway(SearchPort):
    async def search(
        self,
        query: IndexSearchInput,
        raw: bool = False,
    ) -> IndexSearchResult | Dict[str, Any]:
        dsl = self._build_search_payload(query)
        api_key_name = getattr(self.config, "api_key_name", None)
        es_resp = await self._client.search(
            index=self.config.index_name,
            body=dsl,
            api_key_name=api_key_name,
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
            requestId=str(uuid4()),
        )

    # --- Hook methods that subclasses can override later ------------------

    def _build_search_payload(self, req: IndexSearchInput) -> Dict[str, Any]:
        return {
            "track_total_hits": True,
            **self._build_pagination(req),
            "query": self._build_query(req),
            "sort": self._build_sort(req),
            "aggs": self._build_aggs(),
            **self._build_source(),
            "timeout": "3s",
        }

    def _build_query(self, req: IndexSearchInput) -> Dict[str, Any]:
        # your current must/filter/must_not logic here
        ...

    def _build_pagination(self, req: IndexSearchInput) -> Dict[str, Any]: ...

    def _build_sort(self, req: IndexSearchInput) -> List[Any]: ...

    def _build_aggs(self) -> Dict[str, Any]: ...

    def _build_source(self) -> Dict[str, Any]:
        if self.config.source_includes:
            return {"_source": {"includes": list(self.config.source_includes)}}
        return {}

    def _map_aggs_to_searchui(
        self, aggs: Dict[str, Any], config: Dict[str, Any]
    ) -> Dict[str, Any]:
        out: Dict[str, Any] = {}

        for facet_name, spec in config.items():
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

    @staticmethod
    def _is_true_bucket(key: object) -> bool:
        return key is True or key == 1 or key == "1" or str(key).lower() == "true"
