import abc

from mtbls.application.services.interfaces.repositories.default.abstract_read_repository import (  # noqa E501
    AbstractReadRepository,
)
from mtbls.domain.entities.statistic import Statistic


class StatisticReadRepository(AbstractReadRepository[Statistic, str], abc.ABC):
    @abc.abstractmethod
    async def get_metrics_by_section(self, section: str) -> list[Statistic]: ...

    @abc.abstractmethod
    async def get_metric_by_section_and_name(self, section: str, name: str) -> str: ...
