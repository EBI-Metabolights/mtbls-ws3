from logging import getLogger
from typing import Annotated

from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Body, Depends, Response

from mtbls.application.services.interfaces.http_client import HttpClient
from mtbls.application.services.interfaces.repositories.statistic.statistic_read_repository import (  # noqa E501
    StatisticReadRepository,
)
from mtbls.presentation.rest_api.core.responses import APIResponse
from mtbls.presentation.rest_api.groups.public.v1.routers.statistics.schemas import (
    StatisticData,
)
from mtbls.presentation.rest_api.groups.public.v1.routers.study_search.schemas import (
    IndexSearchInput,
)

logger = getLogger(__name__)

router = APIRouter(tags=["Public"], prefix="/public/v2/public-study-index")


@router.post(
    "/search",
    summary="MetaboLights study search from public study index.",
    description="MetaboLights study search from public study index.",
    response_model=APIResponse[StatisticData],
)
@inject
async def search_study_index(
    response: Response,
    query: Annotated[IndexSearchInput, Body(description="query")],
    http_client: HttpClient = Depends(  # noqa: FAST002
        Provide["gateways.http_client"]
    ),
):
    return response
