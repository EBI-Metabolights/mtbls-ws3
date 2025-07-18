import abc

from mtbls.domain.shared.health_check.transfer_status import TransferStatus


class SystemHealthCheckService(abc.ABC):
    @abc.abstractmethod
    async def check_transfer_services(self) -> TransferStatus: ...
