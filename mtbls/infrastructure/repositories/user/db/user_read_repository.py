import logging
import re
from typing import Callable, Union

from sqlalchemy import and_, select
from sqlalchemy.orm import selectinload

from mtbls.application.services.interfaces.auth.authentication_service import (
    UserProfileService,
)
from mtbls.application.services.interfaces.repositories.user.user_read_repository import (  # noqa: E501
    UserReadRepository,
)
from mtbls.domain.entities.study import StudyOutput
from mtbls.domain.entities.user import UserOutput, UserProfile
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
        user_profile_service: None | UserProfileService = None,
    ) -> None:
        super().__init__(entity_mapper, alias_generator, database_client)
        self.user_repository = None
        self._user_profile_service = user_profile_service
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
                    if self.user_profile_service:
                        await self.update_from_user_profile(user_entity)
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
        if not orcid:
            return None
        orcid = re.sub(r"https?://orcid\.org/", "", orcid.lower())
        username = None
        if self.user_profile_service:
            users = await self.user_profile_service.get_users_by_query(
                {"q": f"orcid:{orcid}"}
            )
            username = users[0].username if users else None
        else:
            async with self.database_client.session() as session:
                stmt = select(User).where(User.orcid == orcid)
                result = await session.execute(stmt)
                users: None | list[User] = result.scalars().all()

                if users:
                    username = users[0].username
                    if len(users) > 1:
                        logger.warning(
                            "Multiple users with orcid %s. %s is selected",
                            orcid,
                            users[0].username,
                        )
        if not username:
            return None
        result = await self._get_users_by_filter(
            lambda: User.username == username, include_studies=include_studies
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
        username: str,
        api_token: str,
        include_studies: bool = False,
    ) -> Union[None, UserOutput]:
        result = await self._get_users_by_filter(
            lambda: and_(User.apitoken == api_token, User.username == username),
            include_studies=include_studies,
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
                submitters = await self.entity_mapper.convert_to_output_type_list(
                    study.submitters, UserOutput
                )
                if self.user_profile_service:
                    for submitter in submitters:
                        await self.update_from_user_profile(submitter)
                return submitters
            return []

    async def update_from_user_profile(self, submitter: UserOutput):
        user: UserProfile = await self.user_profile_service.get_user_profile(
            username=submitter.username
        )
        if not user:
            # keep current values. reset profile fields in future
            return
        for name, _ in UserOutput.model_fields.items():
            if name in UserProfile.model_fields:
                setattr(submitter, name, getattr(user, name))

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
