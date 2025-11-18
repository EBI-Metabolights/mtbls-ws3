from logging import getLogger
from typing import Annotated

from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends, Path, Response, status, Body

from mtbls.application.services.interfaces.http_client import HttpClient
from mtbls.application.services.interfaces.repositories.statistic.statistic_read_repository import (  # noqa E501
    StatisticReadRepository,
)
from mtbls.domain.entities.search.study.index_search import IndexSearchInput, IndexSearchResult
from mtbls.presentation.rest_api.core.responses import APIErrorResponse, APIResponse
from mtbls.presentation.rest_api.groups.public.v1.routers.statistics.schemas import (
    MetricData,
    StatisticCategory,
    StatisticData,
)

logger = getLogger(__name__)

router = APIRouter(tags=["Public"], prefix="/public/v2/public-study-index")


@router.post(
    "/search",
    summary="MetaboLights Study Search from public study index.",
    description="MetaboLights Statistics from public study index.",
    response_model=APIResponse[IndexSearchResult],
)
@inject
async def search_study_index(
    response: Response,
    q: Annotated[IndexSearchInput, Body()],
    http_client: HttpClient = Depends(
        Provide["gateways.http_client"]
    ),
    elasticsearch_study_search_service = Depends(
        Provide["gateways.elasticsearch_study_gateway"]
    ),

):
    if not q:
        response.status_code = status.HTTP_400_BAD_REQUEST
        response_message = APIErrorResponse(error="Search query is not valid.")
        return response_message
    result = await elasticsearch_study_search_service.search(query=q)
    response: APIResponse[IndexSearchResult] = APIResponse[IndexSearchResult](content=result)
    return response
