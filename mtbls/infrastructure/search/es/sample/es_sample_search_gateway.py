import logging
from typing import Any

from mtbls.infrastructure.search.es.sample.es_sample_configuration import (
    SampleElasticSearchConfiguration,
)
from mtbls.infrastructure.search.es.es_client import ElasticsearchClient

logger = logging.getLogger(__name__)

# ES field paths for factor header name matching
FACTOR_HEADER_FIELDS: list[str] = [
    "factorHeaderNames.keyword",
    "modifiedFactorHeaderNames",
]


class ElasticsearchSampleGateway:
    """Gateway for querying the sample index to find study IDs by factor header name filters."""

    def __init__(
        self,
        client: ElasticsearchClient,
        config: None | SampleElasticSearchConfiguration | dict[str, Any],
    ):
        self._client = client
        self._config = config
        if not self._config:
            self._config = SampleElasticSearchConfiguration()
        elif isinstance(self._config, dict):
            self._config = SampleElasticSearchConfiguration.model_validate(config)

    @property
    def config(self) -> SampleElasticSearchConfiguration:
        return self._config

    async def find_study_ids_by_factor_headers(
        self,
        values: list[str],
        operator: str = "and",
    ) -> list[str]:
        """
        Query the sample index and return unique study IDs matching the given factor header names.

        Each value is matched against both factorHeaderNames.keyword and modifiedFactorHeaderNames
        using a bool/should (OR across the two fields).

        Args:
            values: Factor header name values to filter by.
            operator: 'and' requires all values to match; 'or' requires any.

        Returns:
            List of unique study IDs.
        """
        if not values:
            return []

        logger.debug(
            "Sample lookup starting (values=%s, operator=%s)",
            values,
            operator,
        )

        dsl = self._build_query(values, operator)

        es_resp = await self._client.search(
            index=self.config.index_name,
            body=dsl,
            api_key_name=self.config.api_key_name,
        )

        logger.debug("Sample lookup completed")
        return self._extract_study_ids(es_resp)

    def _build_query(
        self,
        values: list[str],
        operator: str,
    ) -> dict[str, Any]:
        """Build Elasticsearch DSL for finding study IDs from factor header name filters."""
        # Each value becomes a bool/should across both fields
        per_value_clauses: list[dict[str, Any]] = []
        for value in values:
            per_value_clauses.append(
                {
                    "bool": {
                        "should": [
                            {"term": {field: value}} for field in FACTOR_HEADER_FIELDS
                        ],
                        "minimum_should_match": 1,
                    }
                }
            )

        if operator == "or":
            query: dict[str, Any] = {
                "bool": {
                    "should": per_value_clauses,
                    "minimum_should_match": 1,
                }
            }
        else:
            # "and" — all values must match
            query = {
                "bool": {
                    "must": per_value_clauses,
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
