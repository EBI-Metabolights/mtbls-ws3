from mtbls.application.services.interfaces.repositories.user.user_write_repository import (
    UserWriteRepository,
)
from mtbls.domain.entities.user import UserInput, UserOutput
from mtbls.infrastructure.persistence.db.alias_generator import AliasGenerator
from mtbls.infrastructure.persistence.db.db_client import DatabaseClient
from mtbls.infrastructure.persistence.db.model.entity_mapper import EntityMapper
from mtbls.infrastructure.repositories.default.db.default_write_repository import (
    SqlDbDefaultWriteRepository,
)


class SqlDbUserWriteRepository(
    SqlDbDefaultWriteRepository[UserInput, UserOutput, int],
    UserWriteRepository,
):
    def __init__(
        self,
        entity_mapper: EntityMapper,
        alias_generator: AliasGenerator,
        database_client: DatabaseClient,
    ) -> None:
        super().__init__(entity_mapper, alias_generator, database_client)
