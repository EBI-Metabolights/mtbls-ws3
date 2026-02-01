import logging
from typing import Any

from mtbls.infrastructure.search.es.assay.es_assay_configuration import (
    AssayElasticSearchConfiguration,
)
from mtbls.infrastructure.search.es.es_client import ElasticsearchClient

logger = logging.getLogger(__name__)

# Mapping from request field names to Elasticsearch field paths in the assay index
MS_FIELD_MAP: dict[str, str] = {
    "column_type": "fields.Parameter Value[Column type].value.keyword",
    "chromatography_instrument": "fields.Parameter Value[Chromatography Instrument].value.keyword",
    "instrument": "fields.Parameter Value[Instrument].value.keyword",
}


class ElasticsearchAssayGateway:
    """Gateway for querying the assay index to find study IDs by MS assay filters."""

    def __init__(
        self,
        client: ElasticsearchClient,
        config: None | AssayElasticSearchConfiguration | dict[str, Any],
    ):
        self._client = client
        self._config = config
        if not self._config:
            self._config = AssayElasticSearchConfiguration()
        elif isinstance(self._config, dict):
            self._config = AssayElasticSearchConfiguration.model_validate(config)

    @property
    def config(self) -> AssayElasticSearchConfiguration:
        return self._config

    async def find_study_ids_by_assay_filters(
        self,
        ms_filters: dict[str, list[str]],
        operator: str = "and",
    ) -> list[str]:
        """
        Query the assay index and return unique study IDs matching the given MS filters.

        Args:
            ms_filters: Mapping of filter field names to value lists.
                        Keys must be from MS_FIELD_MAP (column_type, chromatography_instrument, instrument).
            operator: 'and' requires all filter fields to match; 'or' requires any.

        Returns:
            List of unique study IDs.
        """
        # Build clauses from provided filters
        clauses: list[dict[str, Any]] = []
        for field_name, values in ms_filters.items():
            if not values:
                continue
            es_field = MS_FIELD_MAP.get(field_name)
            if not es_field:
                continue
            clauses.append({"terms": {es_field: values}})

        if not clauses:
            return []

        logger.debug(
            "Assay lookup starting (filters=%s, operator=%s)",
            list(ms_filters.keys()),
            operator,
        )

        dsl = self._build_query(clauses, operator)

        es_resp = await self._client.search(
            index=self.config.index_name,
            body=dsl,
            api_key_name=self.config.api_key_name,
        )

        logger.debug("Assay lookup completed")
        return self._extract_study_ids(es_resp)

    def _build_query(
        self,
        clauses: list[dict[str, Any]],
        operator: str,
    ) -> dict[str, Any]:
        """Build Elasticsearch DSL for finding study IDs from assay filters."""
        if operator == "or":
            query: dict[str, Any] = {
                "bool": {
                    "should": clauses,
                    "minimum_should_match": 1,
                }
            }
        else:
            # "and" — all clauses must match
            query = {
                "bool": {
                    "must": clauses,
                }
            }

        return {
            "size": 0,
            "query": query,
            "aggs": {
                "unique_studies": {
                    "terms": {
                        "field": "studyId.keyword",
                        "size": self.config.max_study_ids,
                    }
                }
            },
            "timeout": "10s",
        }

    @staticmethod
    def _extract_study_ids(es_resp: dict[str, Any]) -> list[str]:
        """Extract study IDs from the aggregation response."""
        aggs = es_resp.get("aggregations", {})
        unique_studies = aggs.get("unique_studies", {})
        buckets = unique_studies.get("buckets", [])
        return [bucket["key"] for bucket in buckets]
