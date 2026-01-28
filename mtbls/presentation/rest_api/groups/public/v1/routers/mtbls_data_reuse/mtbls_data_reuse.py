from logging import getLogger

from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends, Response
from fastapi.responses import PlainTextResponse

import mtbls.application.services.interfaces.repositories.mtbls_data_reuse.mtbls_data_reuse_read_repository as data_reuse_repo  # noqa: E501
from mtbls.presentation.rest_api.groups.public.v1.routers.mtbls_data_reuse.schemas import (  # noqa: E501
    StatisticTitle,
)

MtblsDataReuseReadRepository = data_reuse_repo.MtblsDataReuseReadRepository

logger = getLogger(__name__)

router = APIRouter(tags=["Public"], prefix="/public/v2/mtbls-data-reuse")


@router.get(
    "/archive/stats/submissions-monthly-tsv",
    summary="MetaboLights Monthly submission count.",
    description="MetaboLights Monthly submission count.",
)
@inject
async def get_stats_monthly_count(
    response: Response,
    mtbls_data_reuse_read_repository: MtblsDataReuseReadRepository = Depends(  # noqa: FAST002
        Provide["repositories.mtbls_data_reuse_read_repository"]
    ),
):
    data = await mtbls_data_reuse_read_repository.get_latest_data_by_title(
        title=StatisticTitle.SUBMISSIONS_MONTHLY_COUNT.value
    )
    response: Response = PlainTextResponse(content=data.content)
    return response


@router.get(
    "/archive/stats/submitted-data",
    summary="MetaboLights Monthly submitted data volume.",
    description="MetaboLights Monthly submitted data volume.",
)
@inject
async def get_stats_monthly_data_volume(
    response: Response,
    mtbls_data_reuse_read_repository: MtblsDataReuseReadRepository = Depends(  # noqa: FAST002
        Provide["repositories.mtbls_data_reuse_read_repository"]
    ),
):
    data = await mtbls_data_reuse_read_repository.get_latest_data_by_title(
        title=StatisticTitle.SUBMISSIONS_MONTHLY_VOLUME.value
    )
    if data is None:
        response.status_code = 404
        return {"error": "Data not found"}
    response: Response = PlainTextResponse(content=data.content)
    return response
