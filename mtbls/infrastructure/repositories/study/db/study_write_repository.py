from mtbls.application.services.interfaces.repositories.study.study_write_repository import (
    StudyWriteRepository,
)
from mtbls.domain.entities.study import StudyInput, StudyOutput
from mtbls.infrastructure.persistence.db.alias_generator import AliasGenerator
from mtbls.infrastructure.persistence.db.db_client import DatabaseClient
from mtbls.infrastructure.persistence.db.model.entity_mapper import EntityMapper
from mtbls.infrastructure.repositories.default.db.default_write_repository import (
    SqlDbDefaultWriteRepository,
)
from mtbls.infrastructure.repositories.study.db.study_read_repository import (
    SqlDbStudyReadRepository,
)


class SqlDbStudyWriteRepository(
    SqlDbDefaultWriteRepository[StudyInput, StudyOutput, int],
    SqlDbStudyReadRepository,
    StudyWriteRepository,
):
    def __init__(
        self,
        entity_mapper: EntityMapper,
        alias_generator: AliasGenerator,
        database_client: DatabaseClient,
    ) -> None:
        super().__init__(entity_mapper, alias_generator, database_client)
