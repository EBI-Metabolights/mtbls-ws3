from pydantic import BaseModel


class ProtocolServerStatus(BaseModel):
    online: bool


class TransferStatus(BaseModel):
    private_ftp: ProtocolServerStatus
    public_ftp: ProtocolServerStatus
    aspera: ProtocolServerStatus
