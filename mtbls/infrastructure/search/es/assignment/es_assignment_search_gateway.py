from typing import Any

from mtbls.infrastructure.search.es.assignment.es_assignment_configuration import (
    AssignmentElasticSearchConfiguration,
)
from mtbls.infrastructure.search.es.es_client import ElasticsearchClient


class ElasticsearchAssignmentGateway:
    """Gateway for querying the assignment index to find study IDs by compound identifiers."""

    def __init__(
        self,
        client: ElasticsearchClient,
        config: None | AssignmentElasticSearchConfiguration | dict[str, Any],
    ):
        self._client = client
        self._config = config
        if not self._config:
            self._config = AssignmentElasticSearchConfiguration()
        elif isinstance(self._config, dict):
            self._config = AssignmentElasticSearchConfiguration.model_validate(config)

    @property
    def config(self) -> AssignmentElasticSearchConfiguration:
        return self._config

    async def find_study_ids_by_compounds(
        self,
        database_identifiers: list[str] | None = None,
        metabolite_identifications: list[str] | None = None,
    ) -> list[str]:
        """
        Query assignment index and return unique study IDs where any of the
        provided compounds are found.

        Args:
            database_identifiers: List of identifiers (e.g., HMDB0031111)
            metabolite_identifications: List of names (e.g., Lithocholic acid...)

        Returns:
            List of unique study IDs (e.g., ['MTBLS1', 'MTBLS42', ...])
        """
        # Return empty if no filters provided
        if not database_identifiers and not metabolite_identifications:
            return []

        dsl = self._build_query(database_identifiers, metabolite_identifications)

        es_resp = await self._client.search(
            index=self.config.index_name,
            body=dsl,
            api_key_name=self.config.api_key_name,
        )

        return self._extract_study_ids(es_resp)

    def _build_query(
        self,
        database_identifiers: list[str] | None,
        metabolite_identifications: list[str] | None,
    ) -> dict[str, Any]:
        """Build the Elasticsearch DSL query for finding study IDs."""
        should_clauses: list[dict[str, Any]] = []

        if database_identifiers:
            should_clauses.append(
                {"terms": {"fields.database_identifier.value.keyword": database_identifiers}}
            )

        if metabolite_identifications:
            should_clauses.append(
                {"terms": {"fields.metabolite_identification.value.keyword": metabolite_identifications}}
            )

        return {
            "size": 0,  # No documents needed, just aggregation
            "query": {
                "bool": {
                    "should": should_clauses,
                    "minimum_should_match": 1,
                }
            },
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
