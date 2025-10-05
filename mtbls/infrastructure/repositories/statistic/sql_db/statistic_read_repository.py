import logging
from typing import OrderedDict

from mtbls.application.services.interfaces.repositories.statistic.statistic_read_repository import (  # noqa E501
    StatisticReadRepository,
)
from mtbls.domain.entities.statistic import Statistic
from mtbls.domain.enums.entity import Entity
from mtbls.domain.shared.repository.entity_filter import EntityFilter
from mtbls.domain.shared.repository.query_options import QueryOptions
from mtbls.domain.shared.repository.sort_option import SortOption
from mtbls.infrastructure.persistence.db.alias_generator import AliasGenerator
from mtbls.infrastructure.persistence.db.db_client import DatabaseClient
from mtbls.infrastructure.persistence.db.model.entity_mapper import EntityMapper
from mtbls.infrastructure.repositories.default.db.default_read_repository import (
    SqlDbDefaultReadRepository,
)

logger = logging.getLogger(__name__)


class SqlDbStatisticReadRepository(
    SqlDbDefaultReadRepository[Statistic, int], StatisticReadRepository
):
    def __init__(
        self,
        entity_mapper: EntityMapper,
        alias_generator: AliasGenerator,
        database_client: DatabaseClient,
    ) -> None:
        super().__init__(entity_mapper, alias_generator, database_client)
        self.user_repository = None

        self.user_type_alias_dict = OrderedDict()
        for field in Statistic.model_fields:
            entity_type: Entity = Statistic.__entity_type__
            value = alias_generator.get_alias(entity_type, field)
            self.user_type_alias_dict[field] = value

    async def get_metrics_by_section(self, section: str) -> list[Statistic]:
        query_options = QueryOptions(
            filters=[
                EntityFilter(
                    key="section",
                    value=section,
                )
            ],
            sort_options=[SortOption(key="sort_order")],
        )
        result = await self.find(query_options=query_options)
        if result and result.data:
            return result.data
        return []

    async def get_metric_by_section_and_name(self, section: str, name: str) -> str:
        query_options = QueryOptions(
            filters=[
                EntityFilter(
                    key="section",
                    value=section,
                ),
                EntityFilter(
                    key="name",
                    value=name,
                ),
            ],
            sort_options=[SortOption(key="sort_order")],
        )
        result = await self.find(query_options=query_options)
        if result and result.data:
            metric = Statistic.model_validate(result.data[0], from_attributes=True)
            return metric.value or ""

        return ""
