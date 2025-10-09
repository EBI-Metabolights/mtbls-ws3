import logging
from typing import Union

from pydantic import ConfigDict, validate_call
from sqlalchemy import and_, select
from sqlalchemy.orm import selectinload
from typing_extensions import OrderedDict

from mtbls.application.services.interfaces.repositories.study.study_read_repository import (  # noqa: E501
    StudyReadRepository,
)
from mtbls.domain.entities.study import StudyOutput
from mtbls.domain.entities.study_revision import StudyRevisionOutput
from mtbls.domain.entities.user import UserOutput
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
    ) -> None:
        super().__init__(entity_mapper, alias_generator, database_client)
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

        return study_entity

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
    ) -> list[str]:
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
    ) -> None | StudyOutput:
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
                for study in studies:
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
                    study_entities.append(study_entity)

        return study_entities

    async def _get_submitter_studies(self, filter_) -> None | StudyOutput:
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

    async def get_studies_by_email(self, email: str) -> list[StudyOutput]:
        return await self._get_submitter_studies(lambda: User.email == email)

    async def get_studies_by_orcid(self, orcid: str) -> list[StudyOutput]:
        return await self._get_submitter_studies(lambda: User.orcid == orcid)

    async def get_studies_by_user_id(self, id_: str) -> list[StudyOutput]:
        return await self._get_submitter_studies(lambda: User.id == id_)
