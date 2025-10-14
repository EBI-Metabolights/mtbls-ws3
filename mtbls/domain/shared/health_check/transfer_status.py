from typing import Annotated

from metabolights_utils.common import CamelCaseModel
from pydantic import Field


class ProtocolServerStatus(CamelCaseModel):
    online: Annotated[
        bool, Field(description="Indicated if the server is reachable or not.")
    ]


class TransferStatus(CamelCaseModel):
    private_ftp: Annotated[
        ProtocolServerStatus, Field(description="Private FTP server status")
    ]
    public_ftp: Annotated[
        ProtocolServerStatus, Field(description="Public FTP server status")
    ]
    aspera: Annotated[ProtocolServerStatus, Field(description="Aspera server status")]
