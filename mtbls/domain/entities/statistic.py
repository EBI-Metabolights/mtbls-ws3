from mtbls.domain.entities.base_entity import BaseStatisticInput


class StatisticInput(BaseStatisticInput):
    section: str
    name: str
    value: str
    sort_order: int


class Statistic(StatisticInput):
    id_: int
