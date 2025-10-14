from typing import Annotated

from pydantic import Field

from mtbls.domain.shared.health_check.transfer_status import TransferStatus
from mtbls.presentation.rest_api.core.base import APIBaseModel


class TransferHealthCheckResponse(APIBaseModel):
    transfer_status: Annotated[
        TransferStatus,
        Field(default="", description="Current status of each upload server"),
    ]
    message: Annotated[
        str,
        Field(default="", description="Message related to the task."),
    ]
