from typing import Annotated

from pydantic import BaseModel, Field

from mtbls.domain.shared.health_check.transfer_status import TransferStatus


class TransferHealthCheckResponse(BaseModel):
    transfer_status: Annotated[
        TransferStatus,
        Field(default="", description="Current status of each upload server"),
    ]
    message: Annotated[
        str,
        Field(default="", description="Message related to the task."),
    ]
