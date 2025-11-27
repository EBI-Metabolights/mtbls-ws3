from logging import getLogger
from typing import Annotated, Any

from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Body, Depends, Response, status

from mtbls.domain.entities.search.study.index_search import (
    IndexSearchInput,
    IndexSearchResult,
)
from mtbls.presentation.rest_api.core.responses import APIErrorResponse, APIResponse

logger = getLogger(__name__)

router = APIRouter(tags=["Public"], prefix="/public/v2/public-study-index")


@router.post(
    "/search",
    summary="MetaboLights Study Search from public study index.",
    description="MetaboLights search results from public study index. Sanitised output format.",
    response_model=APIResponse[IndexSearchResult],  # IndexSearchResult or raw dict
)
@inject
async def search_study_index(
    response: Response,
    q: Annotated[IndexSearchInput, Body()],
    elasticsearch_study_search_service=Depends(
        Provide["gateways.elasticsearch_study_gateway"]
    ),
):
    if not q:
        response.status_code = status.HTTP_400_BAD_REQUEST
        response_message = APIErrorResponse(error="Search query is not valid.")
        return response_message
    result = await elasticsearch_study_search_service.search(query=q)
    response: APIResponse[IndexSearchResult] = APIResponse[IndexSearchResult](
        content=result
    )
    return response


@router.post(
    "/search/raw",
    summary="MetaboLights Study Search from public study index. (Raw ES Response)",
    description="MetaboLights Statistics from public study index. Raw Elasticsearch Response format for APIConnectors.",
    response_model=APIResponse[Any],
)
@inject
async def search_study_index_raw(
    response: Response,
    q: Annotated[IndexSearchInput, Body()],
    elasticsearch_study_search_service=Depends(
        Provide["gateways.elasticsearch_study_gateway"]
    ),
):
    if not q:
        response.status_code = status.HTTP_400_BAD_REQUEST
        response_message = APIErrorResponse(error="Search query is not valid.")
        return response_message
    result = await elasticsearch_study_search_service.search(query=q, raw=True)
    response: APIResponse[Any] = APIResponse[Any](content=result.body)
    return response


@router.get(
    "/mapping",
    summary="Get Elasticsearch mapping for the public study index.",
    description="Returns the mapping of the configured public study index.",
    response_model=APIResponse[Any],
)
@inject
async def get_study_index_mapping(
    elasticsearch_study_search_service=Depends(
        Provide["gateways.elasticsearch_study_gateway"]
    ),
):
    mapping = await elasticsearch_study_search_service.get_index_mapping()
    return APIResponse[Any](content=mapping)
