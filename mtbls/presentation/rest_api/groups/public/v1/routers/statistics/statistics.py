from logging import getLogger
from typing import Annotated

from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends, Path, Response, status

from mtbls.application.services.interfaces.repositories.statistic.statistic_read_repository import (  # noqa E501
    StatisticReadRepository,
)
from mtbls.presentation.rest_api.core.responses import APIErrorResponse, APIResponse
from mtbls.presentation.rest_api.groups.public.v1.routers.statistics.schemas import (
    MetricData,
    StatisticCategory,
    StatisticData,
)

logger = getLogger(__name__)

router = APIRouter(
    tags=["MetaboLights Statistics"], prefix="/public/v2/mtbls-statistics"
)


@router.get(
    "/general/{category}",
    summary="MetaboLights Statistics by category.",
    description="MetaboLights Statistics by category.",
    response_model=APIResponse[StatisticData],
)
@inject
async def get_statistics_by_section(
    response: Response,
    category: Annotated[StatisticCategory, Path(title="Category of statistics")],
    statistic_read_repository: StatisticReadRepository = Depends(  # noqa: FAST002
        Provide["repositories.statistic_read_repository"]
    ),
):
    if not isinstance(category, StatisticCategory):
        response.status_code = status.HTTP_404_NOT_FOUND
        response_message = APIErrorResponse(error="Category is not defined.")
        return response_message
    category_value = category.get_db_value()
    metrics = await statistic_read_repository.get_metrics_by_section(
        section=category_value
    )
    title = await statistic_read_repository.get_metric_by_section_and_name(
        section="title", name=category_value
    )
    response: APIResponse[StatisticData] = APIResponse[StatisticData]()
    response.content = StatisticData(title=title)
    response.content.key_values = [
        MetricData(key=x.name, value=x.value) for x in metrics
    ]
    return response
