import abc

from mtbls.application.services.interfaces.repositories.default.abstract_read_repository import (  # noqa E501
    AbstractReadRepository,
)
from mtbls.domain.entities.base_entity import MtblsDataReuseOutput


class MtblsDataReuseReadRepository(AbstractReadRepository[MtblsDataReuseOutput, str], abc.ABC):
    @abc.abstractmethod
    async def get_latest_data_by_title(self, title: str) -> MtblsDataReuseOutput: ...
