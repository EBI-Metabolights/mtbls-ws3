from pydantic import BaseModel


class TransferHealthCheckConfiguration(BaseModel):
    health_check_url: str = ""
    timeout_in_seconds: int = 5


class SystemHealthCheckConfiguration(BaseModel):
    transfer_health_check: TransferHealthCheckConfiguration = (
        TransferHealthCheckConfiguration()
    )
