import logging
from typing import OrderedDict, Union

from mtbls.application.services.interfaces.repositories.mtbls_data_reuse import (
    mtbls_data_reuse_read_repository as data_reuse_repo,
)
from mtbls.domain.entities.base_entity import MtblsDataReuseOutput
from mtbls.domain.enums.entity import Entity
from mtbls.domain.enums.sort_order import SortOrder
from mtbls.domain.shared.repository.entity_filter import EntityFilter
from mtbls.domain.shared.repository.query_options import QueryOptions
from mtbls.domain.shared.repository.sort_option import SortOption
from mtbls.infrastructure.persistence.db.alias_generator import AliasGenerator
from mtbls.infrastructure.persistence.db.db_client import DatabaseClient
from mtbls.infrastructure.persistence.db.model.entity_mapper import EntityMapper
from mtbls.infrastructure.repositories.default.db.default_read_repository import (
    SqlDbDefaultReadRepository,
)

MtblsDataReuseReadRepository = data_reuse_repo.MtblsDataReuseReadRepository

logger = logging.getLogger(__name__)


class SqlDbMtblsDataReuseReadRepository(
    SqlDbDefaultReadRepository[MtblsDataReuseOutput, int], MtblsDataReuseReadRepository
):
    def __init__(
        self,
        entity_mapper: EntityMapper,
        alias_generator: AliasGenerator,
        database_client: DatabaseClient,
    ) -> None:
        super().__init__(entity_mapper, alias_generator, database_client)

        self.user_type_alias_dict = OrderedDict()
        for field in MtblsDataReuseOutput.model_fields:
            entity_type: Entity = MtblsDataReuseOutput.model_config.get("entity_type")
            value = alias_generator.get_alias(entity_type, field)
            self.user_type_alias_dict[field] = value

    async def get_latest_data_by_title(self, title: str) -> Union[str, MtblsDataReuseOutput]:
        query_options = QueryOptions(
            filters=[
                EntityFilter(
                    key="content_name",
                    value=title,
                )
            ],
            sort_options=[
                SortOption(
                    key="updated_timestamp",
                    order=SortOrder.DESC,
                )
            ],
        )
        result = await self.find(query_options=query_options)

        if result and result.data:
            return result.data[0]

        return None
