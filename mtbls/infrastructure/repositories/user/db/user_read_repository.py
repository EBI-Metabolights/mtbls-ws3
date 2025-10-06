import logging
from typing import Callable, Union

from pydantic import ConfigDict, validate_call
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from mtbls.application.services.interfaces.repositories.user.user_read_repository import (  # noqa: E501
    UserReadRepository,
)
from mtbls.domain.entities.study import StudyOutput
from mtbls.domain.entities.user import UserOutput
from mtbls.infrastructure.persistence.db.alias_generator import AliasGenerator
from mtbls.infrastructure.persistence.db.db_client import DatabaseClient
from mtbls.infrastructure.persistence.db.model.entity_mapper import EntityMapper
from mtbls.infrastructure.persistence.db.model.study_models import Study, User
from mtbls.infrastructure.repositories.default.db.default_read_repository import (
    SqlDbDefaultReadRepository,
)

logger = logging.getLogger(__name__)


class SqlDbUserReadRepository(
    SqlDbDefaultReadRepository[UserOutput, int], UserReadRepository
):
    def __init__(
        self,
        entity_mapper: EntityMapper,
        alias_generator: AliasGenerator,
        database_client: DatabaseClient,
    ) -> None:
        super().__init__(entity_mapper, alias_generator, database_client)
        self.user_repository = None

    @validate_call(validate_return=True, config=ConfigDict(strict=True))
    async def get_study_by_accession(self, accession: str) -> Union[None, StudyOutput]:
        return await self._get_first_by_field_name("accession_number", accession)

    async def get_user_by_orcid(self, orcid: str) -> Union[None, UserOutput]:
        return await self._get_first_by_field_name("orcid", orcid)

    async def get_user_by_email(self, email: str) -> Union[None, UserOutput]:
        return await self._get_first_by_field_name("email", email)

    async def get_user_by_username(self, username: str) -> Union[None, UserOutput]:
        return await self._get_first_by_field_name("username", username)

    async def get_user_by_api_token(self, api_token: str) -> Union[None, UserOutput]:
        return await self._get_first_by_field_name("api_token", api_token)

    async def _get_studies_by_filter(self, filter_: Callable) -> list[StudyOutput]:
        async with self.database_client.session() as session:
            stmt = select(User).where(filter_()).options(selectinload(Study.users))

            result = await session.execute(stmt)
            result: Study = result.scalars().all()
            if result:
                return [
                    UserOutput.model_validate(x)
                    for x in await result[0].awaitable_attrs.users
                ]
            return []

    async def get_studies_by_username(self, username: str) -> list[StudyOutput]:
        return await self._get_studies_by_filter(lambda: User.username == username)

    async def get_studies_by_email(self, email: str) -> list[StudyOutput]:
        return await self._get_studies_by_filter(lambda: User.email == email)

    async def get_studies_by_orcid(self, orcid: str) -> list[StudyOutput]:
        return await self._get_studies_by_filter(lambda: User.orcid == orcid)
