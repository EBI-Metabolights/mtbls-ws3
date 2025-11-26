from typing import Annotated

from pydantic import Field

from mtbls.domain.entities.ontology.ontology_term import OntologyTerm, StudyBaseModel


class OntologyTermDescription(OntologyTerm):
    description: Annotated[str, Field(description="Ontology term description")]
    curie: Annotated[
        str, Field(description="Compact URI of ontology term. e.g. MS:1000073")
    ]
    synonym: list[str] = []


class OntologyTermHit(OntologyTermDescription):
    origin: Annotated[
        str, Field(description="Origin of ontology search result. e.g. OLS, BIOPORTAL")
    ]
    origin_url: Annotated[str, Field(description="URL of ontology search service.")]


class OntologyTermSearchResult(StudyBaseModel):
    result: Annotated[
        None | list[OntologyTermHit], Field(description="search result")
    ] = None
    success: Annotated[bool, Field(description="Search execution status.")] = False
    message: Annotated[
        None | str, Field(description="Message about search execution.")
    ] = None
    page: Annotated[int, Field(description="Current search result page.")] = 0
