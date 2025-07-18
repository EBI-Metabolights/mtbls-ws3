from pydantic import BaseModel


class TransferHealthCheckConfiguration(BaseModel):
    test: bool = False
    test_response: bool = True
    aspera_host: str = "hx-fasp-1.ebi.ac.uk"
    aspera_port: int = 33001
    private_ftp_host: str = "ftp-private.ebi.ac.uk"
    private_ftp_port: int = 21
    public_ftp_host: str = "ftp.ebi.ac.uk"
    public_ftp_port: int = 21
    timeout_in_seconds: int = 3


class StandaloneSystemHealthCheckConfiguration(BaseModel):
    transfer_health_check: TransferHealthCheckConfiguration = (
        TransferHealthCheckConfiguration()
    )
