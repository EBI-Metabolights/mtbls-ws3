from logging import getLogger
from typing import Annotated

from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Body, Depends, Header, Path, Query, Response, status

from mtbls.application.services.interfaces.ontology_search_service import (
    OntologySearchService,
)
from mtbls.domain.entities.ontology.ontology_search import OntologyTermSearchResult
from mtbls.domain.entities.validation.validation_configuration import (
    BaseOntologyValidation,
    OntologyValidationType,
)
from mtbls.presentation.rest_api.core.responses import (
    APIErrorResponse,
    APIResponse,
)
from mtbls.presentation.rest_api.groups.public.v1.routers.ontology_search.examples import (  # noqa E501
    ONTOLOGY_SEARCH_BODY_EXAMPLES,
)

logger = getLogger(__name__)

router = APIRouter(tags=["Public"], prefix="/public/v2")


@router.post(
    "/ontology-terms/search",
    summary="Search ontology terms.",
    description="Search ontology terms using input",
    response_model=APIResponse[OntologyTermSearchResult],
)
@inject
async def search_ontologies(
    response: Response,
    q: Annotated[str, Query(min_length=3, title="keyword to find an ontology term")],
    rule: Annotated[
        None | BaseOntologyValidation,
        Body(
            title="search ontology terms based on validation rules",
            description="search ontology terms based on validation rules",
            openapi_examples=ONTOLOGY_SEARCH_BODY_EXAMPLES,
        ),
    ] = None,
    exact_match: Annotated[
        bool, Query(title="Exact matched terms only (case insensitive)")
    ] = False,
    ontology_search_service: OntologySearchService = Depends(  # noqa: FAST002
        Provide["services.ontology_search_service"]
    ),
):
    if not q or len(q) < 3:
        response.status_code = status.HTTP_400_BAD_REQUEST
        response_message = APIErrorResponse(error="query is not valid.")
        return response_message
    if not rule:
        rule = BaseOntologyValidation(
            ontology_validation_type=OntologyValidationType.ANY_ONTOLOGY_TERM,
            ontologies=None,
            allowed_parent_ontology_terms=None,
        )
    result = await ontology_search_service.search(q, rule=rule, exact_match=exact_match)
    response: APIResponse[OntologyTermSearchResult] = APIResponse[
        OntologyTermSearchResult
    ](content=result)
    return response


@router.get(
    "/ontology-terms/{ontology}/{label}",
    summary="Find ontology term by label. "
    "It is exact match search in the selected ontology",
    description="Find ontology term by label",
    response_model=APIResponse[OntologyTermSearchResult],
)
@inject
async def find_term_by_label(
    response: Response,
    ontology: Annotated[
        str, Path(min_length=1, title="ontology prefix. e.g., MS, NCBITAXON")
    ],
    label: Annotated[str, Path(min_length=3, title="keyword to find an ontology term")],
    ontology_search_service: OntologySearchService = Depends(  # noqa: FAST002
        Provide["services.ontology_search_service"]
    ),
):
    result = await ontology_search_service.find_ontology_term(label, ontology.lower())
    response: APIResponse[OntologyTermSearchResult] = APIResponse[
        OntologyTermSearchResult
    ](content=result)
    return response


@router.get(
    "/ontology-terms/{ontology}",
    summary="Find ontology term by label. "
    "It is exact match search in the selected ontology",
    description="Find ontology term by label",
    response_model=APIResponse[OntologyTermSearchResult],
)
@inject
async def find_term_by_accession(
    response: Response,
    ontology: Annotated[
        str, Path(min_length=1, title="ontology prefix. e.g., MS, NCBITAXON")
    ],
    accession: Annotated[
        str, Header(min_length=3, title="Accession number of an ontology term")
    ],
    ontology_search_service: OntologySearchService = Depends(  # noqa: FAST002
        Provide["services.ontology_search_service"]
    ),
):
    result = await ontology_search_service.find_by_accession(
        accession, ontology.lower()
    )
    response: APIResponse[OntologyTermSearchResult] = APIResponse[
        OntologyTermSearchResult
    ](content=result)
    return response
