import abc

from mtbls.domain.entities.ontology.ontology_search import OntologyTermSearchResult
from mtbls.domain.entities.validation.validation_configuration import (
    BaseOntologyValidation,
)


class OntologySearchService(abc.ABC):
    @abc.abstractmethod
    async def search(
        self,
        keyword: str,
        rule: BaseOntologyValidation,
        page: None | int = 0,
        size: None | int = 50,
        exact_match: bool = False,
    ) -> OntologyTermSearchResult: ...

    async def find_ontology_term(
        self, term: str, ontologies: None | str | list[str]
    ) -> OntologyTermSearchResult: ...

    async def find_by_accession(
        self, acession: str, ontology: str
    ) -> OntologyTermSearchResult: ...
