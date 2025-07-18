from logging import getLogger
from typing import Annotated, Any

from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends

from mtbls.domain.entities.user import UserOutput
from mtbls.presentation.rest_api.core.responses import APIResponse
from mtbls.presentation.rest_api.groups.auth.v1.routers.dependencies import (
    check_curator_role,
)

logger = getLogger(__name__)

router = APIRouter(tags=["System"], prefix="/system/v2/configuration")


@router.get(
    "",
    summary="Get current system configuration",
    description="Get current system configuration",
    response_model=APIResponse[dict[str, Any]],
)
@inject
async def get_status(
    user: Annotated[UserOutput, Depends(check_curator_role)],
    config: dict[str, Any] = Depends(  # noqa: FAST002
        Provide["config"]
    ),
) -> APIResponse[dict[str, Any]]:
    logger.info("%s user requested configuration", user.id_)
    return APIResponse[dict[str, Any]](status="success", content=config)
