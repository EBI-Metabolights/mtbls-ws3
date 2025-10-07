from logging import getLogger
from typing import Any, Union

from mtbls.application.services.interfaces.health_check_service import (
    SystemHealthCheckService,
)
from mtbls.application.services.interfaces.http_client import HttpClient
from mtbls.domain.entities.http_response import HttpResponse
from mtbls.domain.enums.http_request_type import HttpRequestType
from mtbls.domain.exceptions.health_check import HealthCheckError
from mtbls.domain.shared.health_check.transfer_status import (
    ProtocolServerStatus,
    TransferStatus,
)
from mtbls.infrastructure.system_health_check_service.remote.remote_system_health_check_config import (  # noqa: E501
    SystemHealthCheckConfiguration,
)

logger = getLogger(__name__)


class RemoteSystemHealthCheckService(SystemHealthCheckService):
    def __init__(
        self,
        config: Union[SystemHealthCheckConfiguration, dict[str, Any]],
        http_client: HttpClient,
    ):
        super().__init__()
        self.http_client = http_client
        self.config = config
        if isinstance(self.config, dict):
            self.config = SystemHealthCheckConfiguration.model_validate(config)

    async def check_transfer_services(self) -> TransferStatus:
        config = self.config.transfer_health_check
        if not config.health_check_url:
            raise HealthCheckError("Remote service is not defined.")

        try:
            response: HttpResponse = await self.http_client.send_request(
                HttpRequestType.GET,
                url=config.health_check_url,
                timeout=config.timeout_in_seconds,
            )

            content = response.json_data.get("content", {})
            ts = content.get("transfer_status", {})

            if not ts:
                raise HealthCheckError("Remote service response is not valid.")

            transfer_status = TransferStatus(
                aspera=ProtocolServerStatus(
                    online=ts.get("aspera", {}).get("online", False)
                ),
                private_ftp=ProtocolServerStatus(
                    online=ts.get("private_ftp", {}).get("online", False)
                ),
                public_ftp=ProtocolServerStatus(
                    online=ts.get("public_ftp", {}).get("online", False)
                ),
            )
            return transfer_status

        except Exception as exc:
            # If we can’t reach the service or it returned a bad status
            err_msg = str(exc)
            raise HealthCheckError("Could not fetch remote health status", err_msg)
