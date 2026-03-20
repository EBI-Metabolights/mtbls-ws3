import logging
import re
from typing import Union

from pydantic import ConfigDict, validate_call
from sqlalchemy import and_, select
from sqlalchemy.orm import selectinload
from typing_extensions import OrderedDict

from mtbls.application.services.interfaces.auth.authentication_service import (
    UserProfileService,
)
from mtbls.application.services.interfaces.repositories.study.study_read_repository import (  # noqa: E501
    StudyReadRepository,
)
from mtbls.domain.entities.study import StudyOutput
from mtbls.domain.entities.study_revision import StudyRevisionOutput
from mtbls.domain.entities.user import UserOutput, UserProfile
from mtbls.domain.enums.entity import Entity
from mtbls.domain.shared.repository.entity_filter import EntityFilter
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
        user_profile_service: None | UserProfileService = None,
    ) -> None:
        super().__init__(
            entity_mapper=entity_mapper,
            alias_generator=alias_generator,
            database_client=database_client,
        )
        self.set_user_profile_service(user_profile_service)
        self.user_repository = None
        self.user_type_alias_dict = OrderedDict()
        for field in UserOutput.model_fields:
            entity_type: Entity = UserOutput.model_config.get("entity_type")
            value = alias_generator.get_alias(entity_type, field)
            self.user_type_alias_dict[field] = value

    @validate_call(validate_return=True, config=ConfigDict(strict=True))
    async def get_study_by_id(
        self,
        id_: str,
        include_revisions: bool = False,
        include_submitters: bool = False,
    ) -> Union[None, StudyOutput]:
        return await self.get_study_by_filter(
            lambda: Study.id == id, include_revisions, include_submitters
        )

    @validate_call(validate_return=True, config=ConfigDict(strict=True))
    async def get_study_by_accession(
        self,
        accession: str,
        include_revisions: bool = False,
        include_submitters: bool = False,
    ) -> Union[None, StudyOutput]:
        return await self.get_study_by_filter(
            lambda: Study.acc == accession, include_revisions, include_submitters
        )

    async def get_study_by_filter(
        self, filter_, include_revisions, include_submitters
    ) -> None | StudyOutput:
        study_entity: None | StudyOutput = None
        async with self.database_client.session() as session:
            stmt = select(Study).where(filter_())
            if include_submitters:
                stmt = stmt.options(selectinload(Study.submitters))
            if include_revisions:
                stmt = stmt.options(selectinload(Study.revisions))
            result = await session.execute(stmt)
            study: None | Study = result.scalars().one_or_none()
            if study:
                study_entity: StudyOutput = (
                    await self.entity_mapper.convert_to_output_type(study, StudyOutput)
                )
                if include_revisions and study.revisions:
                    study_entity.revisions = (
                        await self.entity_mapper.convert_to_output_type_list(
                            study.revisions, StudyRevisionOutput
                        )
                    )
                if include_submitters and study.submitters:
                    study_entity.submitters = (
                        await self.entity_mapper.convert_to_output_type_list(
                            study.submitters, UserOutput
                        )
                    )
                    if self.user_profile_service:
                        for submitter in study_entity.submitters:
                            await self.update_from_user_profile(submitter)

        return study_entity

    async def update_from_user_profile(self, submitter: User):
        user: UserProfile = await self.user_profile_service.get_user_profile(
            username=submitter.username
        )
        if not user:
            # keep current values. reset profile fields in future
            return
        for name, _ in UserOutput.model_fields.items():
            if name in UserProfile.model_fields:
                setattr(submitter, name, getattr(user, name))

    @validate_call(validate_return=True, config=ConfigDict(strict=True))
    async def get_study_by_obfuscation_code(
        self,
        obfuscation_code: str,
        include_revisions: bool = False,
        include_submitters: bool = False,
    ) -> Union[None, StudyOutput]:
        return await self.get_study_by_filter(
            lambda: Study.obfuscationcode == obfuscation_code,
            include_revisions,
            include_submitters,
        )

    async def get_studies(
        self,
        filters: Union[None, list[EntityFilter]],
        include_revisions: bool = False,
        include_submitters: bool = False,
    ) -> list[StudyOutput]:
        query_filters = self.build_filters(Study, filters)

        if not query_filters:
            return []
        elif len(query_filters) > 1:
            filters = and_(*query_filters)
        else:
            filters = query_filters[0]

        return await self._get_studies_by_filter(
            filter_=lambda: filters,
            include_revisions=include_revisions,
            include_submitters=include_submitters,
        )

    async def get_study_accessions(
        self,
        filters: Union[None, list[EntityFilter]],
    ) -> list[str]:
        return await self.select_field(
            "accession_number", QueryOptions(filters=filters)
        )

    async def _get_studies_by_filter(
        self, filter_, include_revisions, include_submitters
    ) -> None | list[StudyOutput]:
        async with self.database_client.session() as session:
            stmt = select(Study).where(filter_())
            if include_submitters:
                stmt = stmt.options(selectinload(Study.submitters))
            if include_revisions:
                stmt = stmt.options(selectinload(Study.revisions))
            result = await session.execute(stmt)
            studies: None | Study = result.scalars().all()
            study_entities = []
            if studies:
                for study_item in studies:
                    study: Study = study_item
                    study_entity: StudyOutput = (
                        await self.entity_mapper.convert_to_output_type(
                            study, StudyOutput
                        )
                    )
                    if include_revisions and study.revisions:
                        study_entity.revisions = (
                            await self.entity_mapper.convert_to_output_type_list(
                                study.revisions, StudyRevisionOutput
                            )
                        )
                    if include_submitters and study.submitters:
                        study_entity.submitters = (
                            await self.entity_mapper.convert_to_output_type_list(
                                study.submitters, UserOutput
                            )
                        )
                        if self.user_profile_service:
                            for submitter in study_entity.submitters:
                                await self.update_from_user_profile(submitter)
                    study_entities.append(study_entity)

        return study_entities

    async def _get_submitter_studies(
        self, filter_, user_profile: None | UserProfile = None
    ) -> None | StudyOutput:
        async with self.database_client.session() as session:
            stmt = select(User).where(filter_()).options(selectinload(User.studies))

            result = await session.execute(stmt)
            user: None | User = result.scalars().one_or_none()
            if user:
                return await self.entity_mapper.convert_to_output_type_list(
                    user.studies, StudyOutput
                )
        return []

    async def get_studies_by_username(self, username: str) -> list[StudyOutput]:
        return await self._get_submitter_studies(lambda: User.username == username)

    async def get_studies_by_orcid(self, orcid: str) -> list[StudyOutput]:
        if not orcid:
            return []
        orcid = re.sub(r"https?://orcid\.org/", "", orcid.lower())
        username = None
        if self.user_profile_service:
            users = await self.user_profile_service.get_users_by_query(
                {"q", f"orcid:{orcid}"}
            )

            username = users[0].username if users else None
            if not username:
                return []
        else:
            async with self.database_client.session() as session:
                stmt = select(User).where(User.orcid == orcid)
                result = await session.execute(stmt)
                users: None | list[User] = result.scalars().all()
                if users:
                    username = users[0].username
                if not username:
                    return []

        return await self._get_submitter_studies(lambda: User.username == username)

    async def get_studies_by_user_id(self, id_: str) -> list[StudyOutput]:
        return await self._get_submitter_studies(lambda: User.id == id_)
