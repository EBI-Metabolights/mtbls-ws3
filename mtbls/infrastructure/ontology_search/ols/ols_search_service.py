import hashlib
import json
import logging
import re
from typing import Any, OrderedDict
from urllib.parse import quote

from pydantic import HttpUrl, ValidationError

from mtbls.application.services.interfaces.cache_service import CacheService
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
    ParentOntologyTerms,
)
from mtbls.domain.enums.http_request_type import HttpRequestType
from mtbls.infrastructure.ontology_search.ols.ols_configuration import OlsConfiguration
from mtbls.infrastructure.ontology_search.ols.schemas import OlsSearchResultItem

logger = logging.getLogger(__name__)


class OlsOntologySearchService(OntologySearchService):
    def __init__(
        self,
        http_client: HttpClient,
        cache_service: CacheService,
        config: None | OlsConfiguration | dict[str, Any] = None,
    ):
        self.http_client = http_client
        self.cache_service = cache_service
        if not config:
            self.config = OlsConfiguration()
        elif isinstance(config, dict):
            self.config = OlsConfiguration.model_validate(config)
        elif isinstance(config, OlsConfiguration):
            self.config = config
        else:
            raise Exception("OLS configuration is not valid.")

    async def search(
        self,
        keyword: str,
        rule: BaseOntologyValidation,
        page: None | int = 0,
        size: None | int = 50,
        exact_match: bool = False,
    ) -> OntologyTermSearchResult:
        if not rule or not rule.validation_type:
            return OntologyTermSearchResult(
                success=False, message="Invalid rule definition"
            )
        if (
            not rule.ontologies
            and rule.validation_type == OntologyValidationType.SELECTED_ONTOLOGY
        ):
            return OntologyTermSearchResult(
                success=False,
                message=f"Ontology list is not defined for "
                f"{OntologyValidationType.SELECTED_ONTOLOGY}",
            )

        if not rule.allowed_parent_ontology_terms:
            rule.allowed_parent_ontology_terms = ParentOntologyTerms(
                parents=[], exclude_by_label_pattern=[], exclude_by_accession=[]
            )
        if not rule.allowed_parent_ontology_terms.exclude_by_accession:
            rule.allowed_parent_ontology_terms.exclude_by_accession = []
        if not rule.allowed_parent_ontology_terms.exclude_by_label_pattern:
            rule.allowed_parent_ontology_terms.exclude_by_label_pattern = []
        is_uri = False
        try:
            is_uri = HttpUrl(url=keyword) is not None
        except ValidationError:
            pass

        if rule.ontologies is None:
            rule.ontologies = []
        curie_match = re.match(r"^([A-Za-z0-9]+):([A-Za-z0-9_]+)$", keyword)
        result = []
        exact_match_result = []
        is_child_ontology_search = False
        if is_uri:
            exact_match = True
            logger.debug("Searching by IRI with exact match: %s", keyword)
            response, exact_match_result = await self.search_term(
                keyword,
                page=page,
                size=size,
                exact_match_only=exact_match,
                query_fields=["iri"],
            )
            result = exact_match_result
        elif curie_match:
            exact_match = True
            logger.debug("Searching by Compact URI with exact match: %s", keyword)
            ontology, _ = curie_match.groups()

            response, exact_match_result = await self.search_term(
                keyword,
                ontology_filter=[ontology],
                page=page,
                size=size,
                exact_match_only=exact_match,
                query_fields=["obo_id"],
            )
            result = exact_match_result
        else:
            validation_type = rule.validation_type
            is_child_ontology_search = (
                validation_type == OntologyValidationType.CHILD_ONTOLOGY_TERM
            )
            is_selected_ontology_search = (
                validation_type == OntologyValidationType.SELECTED_ONTOLOGY
            )
            is_selected_ontology_terms = (
                validation_type == OntologyValidationType.SELECTED_ONTOLOGY_TERM
            )
            has_parent_terms = (
                rule.allowed_parent_ontology_terms
                and rule.allowed_parent_ontology_terms.parents
            )
            if (
                not rule.allowed_parent_ontology_terms
                or not rule.allowed_parent_ontology_terms.parents
            ) and is_child_ontology_search:
                message = (
                    f"Parent ontology terms are not defined for "
                    f"{OntologyValidationType.CHILD_ONTOLOGY_TERM}: {keyword}"
                )
                logger.debug(message)
                return OntologyTermSearchResult(success=False, message=message)

            validation_type = rule.validation_type
            if not rule.ontologies and is_selected_ontology_search:
                message = (
                    f"Ontologies are not defined for "
                    f"{OntologyValidationType.SELECTED_ONTOLOGY}: {keyword}"
                )
                logger.debug(message)
                return OntologyTermSearchResult(success=False, message=message)

            if is_selected_ontology_terms:
                name = OntologyValidationType.SELECTED_ONTOLOGY_TERM.name
                message = f"{name} is not supported for searching: '{keyword}'"
                return OntologyTermSearchResult(success=False, message=message)

            rule.ontologies = rule.ontologies or []
            if has_parent_terms:
                parent_sources = list(
                    OrderedDict.fromkeys(
                        [
                            x.term_source_ref
                            for x in rule.allowed_parent_ontology_terms.parents
                        ]
                    )
                )
                for item in parent_sources:
                    if item not in rule.ontologies:
                        rule.ontologies.append(item)
            (
                exact_match_response,
                exact_match_result,
            ) = await self.search_by_validation_type(
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
        result = result or []
        if not exact_match and exact_match_result:
            terms = {(x.term_source_ref, x.term_accession_number) for x in result}
            for item in exact_match_result:
                if (item.term_source_ref, item.term_accession_number) not in terms:
                    result.append(item)

        excluded_patterns = rule.allowed_parent_ontology_terms.exclude_by_label_pattern
        excluded_iri_list = rule.allowed_parent_ontology_terms.exclude_by_accession
        if is_child_ontology_search and (excluded_patterns or excluded_iri_list):

            def is_included(term: OntologyTermHit):
                if not term:
                    False
                if excluded_iri_list:
                    for iri in excluded_iri_list:
                        match = term.term_accession_number == iri
                        if match:
                            return False
                if excluded_patterns:
                    for pattern in excluded_patterns:
                        match = re.match(pattern, term.term)
                        if match:
                            return False
                return True

            result = list(filter(is_included, result))

        ontologies = rule.ontologies
        rank = {value.upper(): i for i, value in enumerate(ontologies)}
        result.sort(
            key=lambda x: (
                self.position_rank(
                    x.term.lower(),
                    keyword.lower(),
                    set([x.lower() for x in x.synonym if x]),
                ),
                x.term.lower(),
                rank.get(x.term_source_ref.upper(), len(ontologies)),
                x.term_source_ref.upper(),
            ),
        )
        return OntologyTermSearchResult(success=True, result=result, page=page or 0)

    async def search_by_validation_type(
        self,
        validation_type: OntologyValidationType,
        keyword: str,
        rule: BaseOntologyValidation,
        page: None | int,
        size: None | int,
        exact_match: bool,
    ):
        response = result = parents = None
        if (
            rule.allowed_parent_ontology_terms
            and rule.allowed_parent_ontology_terms.parents
        ):
            parents = rule.allowed_parent_ontology_terms.parents
        parents = parents or []
        parent_iri_list = [x.term_accession_number for x in parents]

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
            response, result = await self.search_term(
                keyword,
                ontology_filter=rule.ontologies,
                parents=parent_iri_list,
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

        return OntologyTermSearchResult(success=True, result=term, page=0)

    def position_rank(
        self, term: str, keyword: str, synonym_set: None | set[str] = None
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
        field_list: None | list[str] = None,
    ) -> tuple[HttpResponse, list[OntologyTermHit]]:
        if not size or size <= 0:
            size = self.config.default_search_result_size
        url = f"{self.config.origin_url}/api/search"
        if not query_fields:
            query_fields_str = "label,synonym"
        else:
            query_fields_str = ",".join(query_fields)
        if not field_list:
            field_list = [
                "iri",
                "label",
                "ontology_prefix",
                "obo_id",
                "description",
                "type",
                "synonym",
            ]
        params = {
            "q": keyword,
            "fieldList": ",".join(field_list),
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

        # params = {"lang": "en"}
        headers = {"Accept": "application/json"}
        cache_key, result = await self.find_in_cache(url, params, headers)

        if not result:
            result = await self.http_client.send_request(
                HttpRequestType.GET,
                url,
                params=params,
                timeout=self.config.timeout_in_seconds,
                follow_redirects=True,
            )
            if result.error or not result.json_data or result.status_code != 200:
                return result, []
            await self.set_cache_value(cache_key, result)

        matches = None
        if result and result.json_data:
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

    async def find_in_cache(
        self, url, params, headers
    ) -> tuple[str, None | HttpResponse]:
        cache_key = self.url_cache_key(url, params, headers)
        cache_result = await self.cache_service.get_value(cache_key)
        result: None | HttpResponse = None
        if cache_result:
            try:
                result = HttpResponse.model_validate_json(
                    cache_result, by_alias=True, strict=True
                )
            except Exception as ex:
                logger.exception(ex)
        return (
            cache_key,
            result,
        )

    async def set_cache_value(self, cache_key: str, result: HttpResponse):
        valid_response = None
        hits = None
        if not result or not result.json_data or result.status_code != 200:
            return
        if result and result.json_data:
            response = result.json_data.get("response", {})
            if response:
                valid_response = True if "docs" in response else False
                hits = response.get("docs", [])
        if valid_response:
            timeout = (
                self.config.success_result_cache_timeout_in_seconds
                if hits
                else self.config.empty_result_cache_timeout_in_seconds
            )

            result_str = result.model_dump_json(by_alias=True)
            await self.cache_service.set_value(
                cache_key, result_str, expiration_time_in_seconds=timeout
            )

    def url_cache_key(
        self, url: str, params: dict[str, str], headers: dict[str, str]
    ) -> str:
        key_data = {"url": url, "params": params, "headers": headers}
        key_str = json.dumps(key_data, sort_keys=True)
        return hashlib.sha256(key_str.encode()).hexdigest()

    async def search_accession(
        self, accession: str, ontology: None | str = None
    ) -> tuple[HttpResponse, list[OntologyTermHit]]:
        if not ontology or not accession:
            return HttpResponse(
                error=True,
                error_message="Invalid accession or ontology",
                status_code=400,
            ), []

        iri = quote(quote(accession, safe=""), safe="")
        if ontology:
            url = f"{self.config.origin_url}/api/ontologies/{ontology}/terms/{iri}"
        else:
            url = f"{self.config.origin_url}/api/terms/{iri}"

        params = {"lang": "en"}
        headers = {"Accept": "application/json"}
        cache_key, result = await self.find_in_cache(url, params, headers)

        if not result:
            result = await self.http_client.send_request(
                HttpRequestType.GET,
                url,
                params=params,
                headers=headers,
                timeout=self.config.timeout_in_seconds,
                follow_redirects=True,
            )
            if result.error or not result.json_data or result.status_code != 200:
                return result, []
            await self.set_cache_value(cache_key, result)
        if not result.json_data:
            return result, []

        return result, [
            OlsSearchResultItem.model_validate(
                result.json_data
            ).convert_to_ontology_term_hit(self.config.origin, self.config.origin_url)
        ]
