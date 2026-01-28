import logging
from typing import Any

from mtbls.infrastructure.search.es.assignment.es_assignment_configuration import (
    AssignmentElasticSearchConfiguration,
)
from mtbls.infrastructure.search.es.es_client import ElasticsearchClient

logger = logging.getLogger(__name__)


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
        database_identifiers_operator: str | None = "any",
        metabolite_identifications_operator: str | None = "any",
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

        logger.debug(
            "Assignment lookup starting (db_ids=%s, met_ids=%s, db_op=%s, met_op=%s)",
            len(database_identifiers or []),
            len(metabolite_identifications or []),
            database_identifiers_operator or "any",
            metabolite_identifications_operator or "any",
        )

        dsl = self._build_query(
            database_identifiers=database_identifiers or [],
            metabolite_identifications=metabolite_identifications or [],
            database_identifiers_operator=database_identifiers_operator or "any",
            metabolite_identifications_operator=metabolite_identifications_operator or "any",
        )

        es_resp = await self._client.search(
            index=self.config.index_name,
            body=dsl,
            api_key_name=self.config.api_key_name,
        )

        logger.debug("Assignment lookup completed")
        return self._extract_study_ids(es_resp)

    def _build_query(
        self,
        database_identifiers: list[str],
        metabolite_identifications: list[str],
        database_identifiers_operator: str,
        metabolite_identifications_operator: str,
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

        query: dict[str, Any] = {"match_all": {}}
        if should_clauses:
            query = {"bool": {"should": should_clauses, "minimum_should_match": 1}}

        aggs: dict[str, Any] = {
            "unique_studies": {
                "terms": {
                    "field": "studyId.keyword",
                    "size": self.config.max_study_ids,
                },
                "aggs": {},
            }
        }

        buckets_path: dict[str, str] = {}
        script_parts: list[str] = []

        # Database identifiers: require all when operator == "all"; optionally require any if we need to enforce presence
        if database_identifiers:
            if database_identifiers_operator == "all":
                for idx, value in enumerate(database_identifiers):
                    agg_name = f"db_all_{idx}"
                    aggs["unique_studies"]["aggs"][agg_name] = {
                        "filter": {
                            "term": {
                                "fields.database_identifier.value.keyword": value
                            }
                        }
                    }
                    buckets_path[agg_name] = f"{agg_name}>_count"
                    script_parts.append(f"params.{agg_name} > 0")
            else:
                # Only enforce "any" if this is the sole list or the other list uses "all".
                if not metabolite_identifications or metabolite_identifications_operator == "all":
                    aggs["unique_studies"]["aggs"]["db_any"] = {
                        "filter": {
                            "terms": {"fields.database_identifier.value.keyword": database_identifiers}
                        }
                    }
                    buckets_path["db_any"] = "db_any>_count"
                    script_parts.append("params.db_any > 0")

        # Metabolite identifications: require all when operator == "all"; optionally require any if we need to enforce presence
        if metabolite_identifications:
            if metabolite_identifications_operator == "all":
                for idx, value in enumerate(metabolite_identifications):
                    agg_name = f"met_all_{idx}"
                    aggs["unique_studies"]["aggs"][agg_name] = {
                        "filter": {
                            "term": {
                                "fields.metabolite_identification.value.keyword": value
                            }
                        }
                    }
                    buckets_path[agg_name] = f"{agg_name}>_count"
                    script_parts.append(f"params.{agg_name} > 0")
            else:
                if not database_identifiers or database_identifiers_operator == "all":
                    aggs["unique_studies"]["aggs"]["met_any"] = {
                        "filter": {
                            "terms": {
                                "fields.metabolite_identification.value.keyword": metabolite_identifications
                            }
                        }
                    }
                    buckets_path["met_any"] = "met_any>_count"
                    script_parts.append("params.met_any > 0")

        if buckets_path:
            aggs["unique_studies"]["aggs"]["require_all"] = {
                "bucket_selector": {
                    "buckets_path": buckets_path,
                    "script": " && ".join(script_parts) if script_parts else "true",
                }
            }

        return {
            "size": 0,  # No documents needed, just aggregation
            "query": query,
            "aggs": aggs,
            "timeout": "10s",
        }

    @staticmethod
    def _extract_study_ids(es_resp: dict[str, Any]) -> list[str]:
        """Extract study IDs from the aggregation response."""
        aggs = es_resp.get("aggregations", {})
        unique_studies = aggs.get("unique_studies", {})
        buckets = unique_studies.get("buckets", [])

        return [bucket["key"] for bucket in buckets]
