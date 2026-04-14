import datetime
import logging
from typing import List

from mtbls.application.services.interfaces.repositories.study.study_read_repository import (
    StudyReadRepository,
)
from mtbls.domain.entities.study import StudyOutput
from mtbls.domain.enums.filter_operand import FilterOperand
from mtbls.domain.shared.repository.entity_filter import EntityFilter

logger = logging.getLogger(__name__)


async def get_private_study_ids(
    study_read_repository: StudyReadRepository,
    min_expected_release_date: None | datetime.datetime = None,
    max_expected_release_date: None | datetime.datetime = None,
    expected_release_date_field: str = "release_date",
) -> List[StudyOutput]:
    try:
        filters = [
            EntityFilter(
                key="first_private_date",
                operand=FilterOperand.GT,
                value=datetime.datetime.fromtimestamp(0),
            ),
            EntityFilter(
                key="revision_number",
                operand=FilterOperand.EQ,
                value=0,
            ),
        ]
        for val, field, operator in [
            (min_expected_release_date, expected_release_date_field, FilterOperand.GE),
            (max_expected_release_date, expected_release_date_field, FilterOperand.LE),
        ]:
            if val is not None:
                filters.append(EntityFilter(key=field, operand=operator, value=val))
        studies: list[StudyOutput] = await study_read_repository.get_studies(
            filters=filters, include_submitters=True
        )

        def sort_by_revision(x: StudyOutput):
            return (x.release_date, x.first_private_date, x.accession_number)

        studies.sort(key=sort_by_revision, reverse=True)
        logger.info("%s studies are found.", len(studies))
        return studies
    except Exception as ex:
        raise ex


async def get_private_study_ids_by_first_private_date(
    study_read_repository: StudyReadRepository,
    min_first_private_date: None | datetime.datetime = None,
    max_first_private_date: None | datetime.datetime = None,
    min_created_at: None | datetime.datetime = None,
    max_created_at: None | datetime.datetime = None,
) -> List[StudyOutput]:
    try:
        filters = [
            EntityFilter(
                key="first_private_date",
                operand=FilterOperand.GT,
                value=datetime.datetime.fromtimestamp(0),
            ),
            EntityFilter(
                key="revision_number",
                operand=FilterOperand.EQ,
                value=0,
            ),
        ]
        for val, field, operator in [
            (min_first_private_date, "first_private_date", FilterOperand.GE),
            (max_first_private_date, "first_private_date", FilterOperand.LE),
            (min_created_at, "created_at", FilterOperand.GE),
            (max_created_at, "created_at", FilterOperand.LE),
        ]:
            if val is not None:
                filters.append(EntityFilter(key=field, operand=operator, value=val))

        studies: list[StudyOutput] = await study_read_repository.get_studies(
            filters=filters, include_submitters=True
        )

        def sort_by_revision(x: StudyOutput):
            return (x.first_private_date, x.created_at)

        studies.sort(key=sort_by_revision, reverse=True)
        logger.info("%s studies are found.", len(studies))
        return studies
    except Exception as ex:
        raise ex
