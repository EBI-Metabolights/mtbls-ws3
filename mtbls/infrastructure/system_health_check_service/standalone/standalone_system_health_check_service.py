import socket
from logging import getLogger
from typing import Any, Union

import httpx

from mtbls.application.services.interfaces.health_check_service import (
    SystemHealthCheckService,
)
from mtbls.domain.exceptions.health_check import HealthCheckError
from mtbls.domain.shared.health_check.transfer_status import (
    ProtocolServerStatus,
    TransferStatus,
)
from mtbls.infrastructure.system_health_check_service.standalone.standalone_system_health_check_config import (  # noqa: E501
    StandaloneSystemHealthCheckConfiguration,
)

logger = getLogger(__name__)


class StandaloneSystemHealthCheckService(SystemHealthCheckService):
    def __init__(
        self,
        config: Union[StandaloneSystemHealthCheckConfiguration, dict[str, Any]],
    ):
        super().__init__()
        self.config = config
        if isinstance(self.config, dict):
            self.config = StandaloneSystemHealthCheckConfiguration.model_validate(
                config
            )

    async def check_transfer_services(self) -> TransferStatus:
        config = self.config.transfer_health_check
        if config.test:
            return TransferStatus(
                aspera=ProtocolServerStatus(online=config.test_response),
                private_ftp=ProtocolServerStatus(online=config.test_response),
                public_ftp=ProtocolServerStatus(online=config.test_response),
            )
        try:
            transfer_status = TransferStatus(
                aspera=ProtocolServerStatus(
                    online=self.is_fasp_alive(
                        host=config.aspera_host,
                        port=config.aspera_port,
                        timeout=config.timeout_in_seconds,
                    )
                ),
                private_ftp=ProtocolServerStatus(
                    online=self.is_ftp_alive_banner(
                        host=config.private_ftp_host,
                        port=config.private_ftp_port,
                        timeout=config.timeout_in_seconds,
                    )
                ),
                public_ftp=ProtocolServerStatus(
                    online=self.is_ftp_alive_banner(
                        host=config.public_ftp_host,
                        port=config.public_ftp_port,
                        timeout=config.timeout_in_seconds,
                    )
                ),
            )
            return transfer_status

        except (httpx.RequestError, httpx.HTTPStatusError, httpx.ConnectError) as exc:
            err_msg = str(exc)
            raise HealthCheckError("Could not fetch remote health status", err_msg)

    def is_fasp_alive(self, host: str, port: int = 33001, timeout: float = 3.0) -> bool:
        """
        Attempt a TCP connect to host:port and verify an SSH banner comes back.
        Returns True if we see something like "SSH-2.0-Aspera".
        """
        try:
            with socket.create_connection((host, port), timeout=timeout) as sock:
                banner = sock.recv(64)
                return banner.startswith(b"SSH-")
        except (socket.timeout, ConnectionRefusedError, OSError):
            return False

    def is_ftp_alive_banner(
        self, host: str, port: int = 21, timeout: float = 3.0
    ) -> bool:
        """
        Open a TCP socket to the FTP port and check for an "FTP" banner.
        Returns True if the server responds with something like "220 ".
        """
        try:
            with socket.create_connection((host, port), timeout=timeout) as sock:
                banner = sock.recv(64).decode(errors="ignore")
                return banner.startswith("220-Welcome")
        except (socket.timeout, ConnectionRefusedError, OSError):
            return False
