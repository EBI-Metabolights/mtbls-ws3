from logging import getLogger

from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends, Response

from mtbls.application.services.interfaces.health_check_service import (
    SystemHealthCheckService,
)
from mtbls.domain.shared.health_check.transfer_status import (
    ProtocolServerStatus,
    TransferStatus,
)
from mtbls.presentation.rest_api.core.responses import APIResponse, Status
from mtbls.presentation.rest_api.groups.system.v1.routers.health.schemas import (
    TransferHealthCheckResponse,
)

logger = getLogger(__name__)

router = APIRouter(tags=["System"], prefix="/system/v2/transfer-status")


@router.get(
    "",
    summary="Get current status of FTP and Aspera",
    description="Attempt to reach the FASP and FTP servers to test whether they are online and responsive, and report the results",
    response_model=APIResponse[TransferHealthCheckResponse],
)
@inject
async def get_status(
    response: Response,
    system_health_check_service: SystemHealthCheckService = Depends(  # noqa: FAST002
        Provide["services.system_health_check_service"]
    ),
) -> APIResponse[TransferHealthCheckResponse]:
    try:
        status = await system_health_check_service.check_transfer_services()

        return APIResponse[TransferHealthCheckResponse](
            content=TransferHealthCheckResponse(transfer_status=status, message=""),
        )
    except Exception as ex:
        return APIResponse[TransferHealthCheckResponse](
            status=Status.ERROR,
            errorMessage=f"Health service failed {str(ex)}",
            errors=[str(ex)],
            content=TransferHealthCheckResponse(
                transfer_status=TransferStatus(
                    aspera=ProtocolServerStatus(online=False),
                    private_ftp=ProtocolServerStatus(online=False),
                    public_ftp=ProtocolServerStatus(online=False),
                ),
                message="Could not fetch remote health status",
            ),
        )
