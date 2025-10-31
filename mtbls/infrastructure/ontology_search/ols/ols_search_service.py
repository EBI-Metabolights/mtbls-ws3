from typing import Any
from urllib.parse import quote

from mtbls.application.services.interfaces.http_client import HttpClient
from mtbls.application.services.interfaces.ontology_search_service import (
    OntologySearchService,
)
from mtbls.domain.entities.http_response import HttpResponse
from mtbls.domain.entities.ontology.ontology_search import (
    OntologyTermHit,
    OntologyTermSearchResult,
)
from mtbls.domain.entities.validation.validation_configuration import (
    BaseOntologyValidation,
    OntologyValidationType,
)
from mtbls.domain.enums.http_request_type import HttpRequestType
from mtbls.infrastructure.ontology_search.ols.ols_configuration import OlsConfiguration
from mtbls.infrastructure.ontology_search.ols.schemas import OlsSearchResultItem


class OlsOntologySearchService(OntologySearchService):
    def __init__(
        self,
        http_client: HttpClient,
        config: None | OlsConfiguration | dict[str, Any] = None,
    ):
        self.http_client = http_client
        self.config = config
        if not self.config:
            self.config = OlsConfiguration()
        elif isinstance(self.config, dict):
            self.config = OlsConfiguration.model_validate(config)

    async def search(
        self,
        keyword: str,
        rule: BaseOntologyValidation,
        page: None | int = 0,
        size: None | int = 50,
        exact_match: bool = False,
    ) -> OntologyTermSearchResult:
        if not rule or not rule.ontology_validation_type:
            return OntologyTermSearchResult(
                success=False, message="Invalid rule definition"
            )
        if (
            not rule.ontologies
            and rule.ontology_validation_type
            == OntologyValidationType.SELECTED_ONTOLOGY
        ):
            return OntologyTermSearchResult(
                success=False,
                message=f"Ontology list is not defined for "
                f"{OntologyValidationType.SELECTED_ONTOLOGY}",
            )

        if rule.ontologies is None:
            rule.ontologies = []
        if (
            not rule.allowed_parent_ontology_terms
            and rule.ontology_validation_type
            == OntologyValidationType.CHILD_ONTOLOGY_TERM
        ):
            return OntologyTermSearchResult(
                success=False,
                message=f"Parent ontology terms are not defined for "
                f"{OntologyValidationType.CHILD_ONTOLOGY_TERM}",
            )
        if rule.allowed_parent_ontology_terms is None:
            rule.allowed_parent_ontology_terms = []

        validation_type = rule.ontology_validation_type
        if validation_type == OntologyValidationType.SELECTED_ONTOLOGY_TERM:
            validation_type_name = OntologyValidationType.SELECTED_ONTOLOGY_TERM.name
            return OntologyTermSearchResult(
                success=False,
                message=f"{validation_type_name} is not supported",
            )
        exact_match_response, exact_match_result = await self.search_by_validation_type(
            validation_type, keyword, rule, page, size, True
        )
        if exact_match:
            response = exact_match_response
            result = exact_match_result
        else:
            response, result = await self.search_by_validation_type(
                validation_type, keyword, rule, page, size, False
            )
        if not response:
            return OntologyTermSearchResult(
                success=False, message="No response. Check validation type.", result=[]
            )
        if not response or response.error:
            return OntologyTermSearchResult(
                success=False, message=response.error_message or "", result=[]
            )
        if not exact_match and exact_match_result:
            terms = {(x.term_source_ref, x.term_accession_number) for x in result}
            for item in exact_match_result:
                if (item.term_source_ref, item.term_accession_number) not in terms:
                    result.append(item)

        ontologies = rule.ontologies or []

        rank = {value.upper(): i for i, value in enumerate(ontologies)}
        result.sort(
            key=lambda x: (
                self.position_rank(
                    x.term.lower(),
                    keyword.lower(),
                    set([x.lower() for x in x.synonym if x]),
                ),
                x.term,
                rank.get(x.term_source_ref.upper(), len(ontologies)),
            ),
        )
        return OntologyTermSearchResult(success=True, result=result, page=page or 0)

    async def search_by_validation_type(
        self,
        validation_type: OntologyValidationType,
        keyword: str,
        rule: BaseOntologyValidation,
        page: int,
        size: int,
        exact_match: bool,
    ):
        response = result = None
        if validation_type == OntologyValidationType.ANY_ONTOLOGY_TERM:
            response, result = await self.search_term(
                keyword, page=page, size=size, exact_match_only=exact_match
            )
        elif validation_type == OntologyValidationType.SELECTED_ONTOLOGY:
            response, result = await self.search_term(
                keyword,
                ontology_filter=rule.ontologies,
                page=page,
                size=size,
                exact_match_only=exact_match,
            )
        elif validation_type == OntologyValidationType.CHILD_ONTOLOGY_TERM:
            parents = [
                x.term_accession_number
                for x in rule.allowed_parent_ontology_terms.parents
            ]
            response, result = await self.search_term(
                keyword,
                ontology_filter=rule.ontologies,
                parents=parents,
                page=page,
                size=size,
                exact_match_only=exact_match,
            )

        return response, result

    async def find_ontology_term(
        self, term: str, ontologies: None | str | list[str]
    ) -> OntologyTermSearchResult:
        if not ontologies:
            ontologies = []
        if isinstance(ontologies, str):
            ontologies = [ontologies]

        response, result = await self.search_term(
            term,
            ontology_filter=ontologies,
            page=0,
            size=self.config.default_search_result_size,
            exact_match_only=True,
        )

        if response.error:
            return OntologyTermSearchResult(
                success=False, message=response.error_message or "", result=[]
            )
        ontologies = ontologies or []

        rank = {value.upper(): i for i, value in enumerate(ontologies)}
        result.sort(
            key=lambda x: (
                self.position_rank(
                    x.term, term, set([x.lower() for x in x.synonym if x])
                ),
                x.term,
                rank.get(x.term_source_ref.upper(), len(ontologies)),
            ),
        )
        return OntologyTermSearchResult(success=True, result=result, page=0)

    async def find_by_accession(
        self, acession: str, ontology: str
    ) -> OntologyTermSearchResult:
        if not acession or not ontology:
            return OntologyTermSearchResult(
                success=False, message="Invalid input" or "", result=[]
            )

        response, term = await self.search_accession(acession, ontology=ontology)

        if response.error or not term:
            return OntologyTermSearchResult(
                success=False, message=response.error_message or "", result=[]
            )

        return OntologyTermSearchResult(success=True, result=[term], page=0)

    def position_rank(
        self, term: str, keyword: str, synonym_set: set[str] = None
    ) -> int:
        """Return rank based on position of search term in string."""
        if not synonym_set:
            synonym_set = set()
        if term == keyword:
            return 0
        if synonym_set and keyword in synonym_set:
            return 1
        if term.startswith(keyword):
            return 2  # highest priority
        elif term.endswith(keyword):
            return 4  # lowest priority
        elif keyword in term:
            return 3  # middle occurrence
        else:
            return 5  # doesn't contain at all

    async def search_term(
        self,
        keyword: str,
        ontology_filter: None | list[str] = None,
        parents: None | list[str] = None,
        exact_match_only: bool = False,
        query_fields: None | list[str] = None,
        page: None | int = None,
        size: None | int = None,
    ) -> tuple[HttpResponse, list[OntologyTermHit]]:
        if not size or size <= 0:
            size = self.config.default_search_result_size
        url = f"{self.config.origin_url}/api/search"
        if not query_fields:
            query_fields_str = "label,synonym"
        else:
            query_fields_str = ",".join(query_fields)

        params = {
            "q": keyword,
            "fieldList": "iri,label,ontology_prefix,obo_id,description,type,synonym",
            "queryFields": query_fields_str,
            "exact": exact_match_only,
            "obsoletes": False,
            "rows": size,
            "format": "json",
            "lang": "en",
            "type": "class,individual",
        }
        if page and page > 0:
            params.update({"start": page * size})
        ontology_filter_set = set()
        if ontology_filter:
            ontology_filter_set = {
                x.lower() for x in ontology_filter if x and x.strip()
            }

        if parents:
            params.update({"childrenOf": ",".join([x for x in parents if x])})
        elif ontology_filter:
            params.update(
                {"ontology": ",".join([x.lower() for x in ontology_filter if x])}
            )

        result: HttpResponse = await self.http_client.send_request(
            HttpRequestType.GET,
            url,
            params=params,
            timeout=self.config.timeout_in_seconds,
            follow_redirects=True,
        )
        if result.error:
            return result, []
        matches = result.json_data.get("response", {}).get("docs", [])

        if not matches:
            return result, []
        hits = [
            OlsSearchResultItem.model_validate(x).convert_to_ontology_term_hit(
                self.config.origin, self.config.origin_url
            )
            for x in matches
        ]
        if ontology_filter_set:
            return result, list(
                filter(
                    lambda x: x.term_source_ref.lower() in ontology_filter_set,
                    hits,
                )
            )
        return result, hits

    async def search_accession(
        self, accession: str, ontology: str
    ) -> tuple[HttpResponse, OlsSearchResultItem]:
        if not ontology or not accession:
            return HttpResponse(
                error=True,
                error_message="Invalid accession or ontology",
                status_code=400,
            ), []

        iri = quote(quote(accession, safe=""), safe="")

        url = f"{self.config.origin_url}/api/ontologies/{ontology}/terms/{iri}"
        params = {"lang": "en"}
        headers = {"Accept": "application/json"}
        result: HttpResponse = await self.http_client.send_request(
            HttpRequestType.GET,
            url,
            params=params,
            headers=headers,
            timeout=self.config.timeout_in_seconds,
            follow_redirects=True,
        )
        if result.error or result.json_data.get("status", 200) != 200:
            return result, []

        if not result.json_data:
            return result, []

        return result, OlsSearchResultItem.model_validate(
            result.json_data
        ).convert_to_ontology_term_hit(self.config.origin, self.config.origin_url)
