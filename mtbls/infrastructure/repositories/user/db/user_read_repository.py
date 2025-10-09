import logging
from typing import Callable, Union

from sqlalchemy import select
from sqlalchemy.orm import selectinload

from mtbls.application.services.interfaces.repositories.user.user_read_repository import (  # noqa: E501
    UserReadRepository,
)
from mtbls.domain.entities.study import StudyOutput
from mtbls.domain.entities.user import UserOutput
from mtbls.domain.enums.entity import Entity
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
        self.study_table_moodel = entity_mapper.get_table_model(Entity.Study)
        self.study_revision_table_moodel = entity_mapper.get_table_model(
            Entity.StudyRevision
        )

    async def _get_users_by_filter(
        self, filter_, include_studies: bool = False
    ) -> None | UserOutput:
        async with self.database_client.session() as session:
            stmt = select(User).where(filter_())
            if include_studies:
                stmt = stmt.options(selectinload(User.studies))

            result = await session.execute(stmt)
            users: None | list[User] = result.scalars().all()
            user_entities = []
            if users:
                for user in users:
                    user_entity: UserOutput = (
                        await self.entity_mapper.convert_to_output_type(
                            user, UserOutput
                        )
                    )
                    if include_studies and user.studies:
                        user_entity.studies = (
                            await self.entity_mapper.convert_to_output_type_list(
                                user_entity.studies, StudyOutput
                            )
                        )
                    user_entities.append(user_entity)

        return user_entities

    async def get_user_by_id(
        self,
        id_: int,
        include_studies: bool = False,
    ) -> Union[None, UserOutput]:
        result = await self._get_users_by_filter(
            lambda: User.id == id_, include_studies=include_studies
        )
        return result[0] if result else None

    async def get_user_by_orcid(
        self,
        orcid: str,
        include_studies: bool = False,
    ) -> Union[None, UserOutput]:
        result = await self._get_users_by_filter(
            lambda: User.orcid == orcid, include_studies=include_studies
        )
        return result[0] if result else None

    async def get_user_by_email(
        self,
        email: str,
        include_studies: bool = False,
    ) -> Union[None, UserOutput]:
        result = await self._get_users_by_filter(
            lambda: User.email == email, include_studies=include_studies
        )
        return result[0] if result else None

    async def get_user_by_username(
        self,
        username: str,
        include_studies: bool = False,
    ) -> Union[None, UserOutput]:
        result = await self._get_users_by_filter(
            lambda: User.username == username, include_studies=include_studies
        )
        return result[0] if result else None

    async def get_user_by_api_token(
        self,
        api_token: str,
        include_studies: bool = False,
    ) -> Union[None, UserOutput]:
        result = await self._get_users_by_filter(
            lambda: User.apitoken == api_token, include_studies=include_studies
        )
        return result[0] if result else None

    async def _get_study_submitters_by_filter(
        self, filter_: Callable
    ) -> list[UserOutput]:
        async with self.database_client.session() as session:
            stmt = (
                select(Study).where(filter_()).options(selectinload(Study.submitters))
            )

            result = await session.execute(stmt)
            study: None | Study = result.scalars().one_or_none()
            if study:
                return await self.entity_mapper.convert_to_output_type_list(
                    study.submitters, UserOutput
                )
            return []

    async def get_study_submitters_by_accession(
        self, accession_number: str
    ) -> list[UserOutput]:
        result = await self._get_study_submitters_by_filter(
            lambda: Study.acc == accession_number
        )
        return result

    async def get_study_submitters_by_study_table_id(
        self, id_: int
    ) -> list[UserOutput]:
        return await self._get_study_submitters_by_filter(lambda: Study.id == id_)
