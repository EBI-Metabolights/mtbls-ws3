import logging
from typing import Callable, Union

from pydantic import ConfigDict, validate_call
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from typing_extensions import OrderedDict

from mtbls.application.services.interfaces.repositories.study.study_read_repository import (  # noqa: E501
    StudyReadRepository,
)
from mtbls.domain.entities.study import StudyOutput
from mtbls.domain.entities.user import UserOutput
from mtbls.domain.enums.entity import Entity
from mtbls.domain.shared.repository.query_options import QueryOptions
from mtbls.infrastructure.persistence.db.alias_generator import AliasGenerator
from mtbls.infrastructure.persistence.db.db_client import DatabaseClient
from mtbls.infrastructure.persistence.db.model.entity_mapper import EntityMapper
from mtbls.infrastructure.persistence.db.model.study_models import Study, User
from mtbls.infrastructure.repositories.default.db.default_read_repository import (
    SqlDbDefaultReadRepository,
)

logger = logging.getLogger(__name__)


class SqlDbStudyReadRepository(
    SqlDbDefaultReadRepository[StudyOutput, int], StudyReadRepository
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
        for field in UserOutput.model_fields:
            entity_type: Entity = UserOutput.__entity_type__
            value = alias_generator.get_alias(entity_type, field)
            self.user_type_alias_dict[field] = value

    @validate_call(validate_return=True, config=ConfigDict(strict=True))
    async def get_study_by_accession(self, accession: str) -> Union[None, StudyOutput]:
        return await self._get_first_by_field_name("accession_number", accession)

    @validate_call(validate_return=True, config=ConfigDict(strict=True))
    async def get_study_by_obfuscation_code(
        self, obfuscation_code: str
    ) -> Union[None, StudyOutput]:
        return await self._get_first_by_field_name("obfuscation_code", obfuscation_code)

    async def get_study_accessions(
        self, query_options: Union[None, QueryOptions] = None
    ) -> list[str]: ...

    async def convert_to_user(self, db_object: User) -> UserOutput:
        if not db_object:
            return None
        object_dict = db_object.__dict__

        value = {
            x: object_dict[self.user_type_alias_dict[x]]
            for x in self.user_type_alias_dict
        }
        return UserOutput.model_validate(value)

    async def convert_to_users(self, db_objects: User) -> list[UserOutput]:
        if not db_objects:
            return []
        return [await self.convert_to_user(x) for x in db_objects]

    async def _get_studies_by_filter(self, filter_: Callable) -> list[StudyOutput]:
        async with self.database_client.session() as session:
            stmt = select(User).where(filter_()).options(selectinload(User.studies))

            result = await session.execute(stmt)
            result: None | Study = result.scalars().one_or_none()

            if result:
                return await self.convert_to_output_type_list(result.studies)
            return []

    async def get_studies_by_username(self, username: str) -> list[StudyOutput]:
        return await self._get_studies_by_filter(lambda: User.username == username)

    async def get_studies_by_email(self, email: str) -> list[StudyOutput]:
        return await self._get_studies_by_filter(lambda: User.email == email)

    async def get_studies_by_orcid(self, orcid: str) -> list[StudyOutput]:
        return await self._get_studies_by_filter(lambda: User.orcid == orcid)
