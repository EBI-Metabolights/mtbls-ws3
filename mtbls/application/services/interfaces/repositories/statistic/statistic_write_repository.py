from mtbls.application.services.interfaces.repositories.default.abstract_write_repository import (  # noqa E501
    AbstractWriteRepository,
)
from mtbls.application.services.interfaces.repositories.statistic.statistic_read_repository import (  # noqa E501
    StatisticReadRepository,
)
from mtbls.domain.entities.statistic import (
    Statistic,
    StatisticInput,
)


class StatisticWriteRepository(
    AbstractWriteRepository[StatisticInput, Statistic, str],
    StatisticReadRepository,
): ...
