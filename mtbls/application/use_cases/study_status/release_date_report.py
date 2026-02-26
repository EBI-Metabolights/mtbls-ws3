import datetime
import logging
from pathlib import Path

from dateutil.relativedelta import relativedelta

from mtbls.application.services.interfaces.repositories.study.study_read_repository import (
    StudyReadRepository,
)
from mtbls.application.use_cases.study_status.private_studies import (
    get_private_study_ids,
)

logger = logging.getLogger(__name__)


class PrivateStudyReleaseDateReporter:
    def __init__(self, study_read_repository: StudyReadRepository):
        self.study_read_repository = study_read_repository

    async def find_release_date_in_past(self):
        today = datetime.datetime.now()
        _, end = self.get_datetime_range(today)
        return await self.find_release_date_in_range(None, end)

    async def find_release_date_today(self):
        today = datetime.datetime.now()
        start, end = self.get_datetime_range(today)
        return await self.find_release_date_in_range(start, end)

    async def find_release_date_in_future(
        self, days_later: int = 0, months_later: int = 0
    ):
        today = datetime.datetime.now()
        start, end = self.get_datetime_range(
            today, days_later=days_later, months_later=months_later
        )
        return await self.find_release_date_in_range(start, end)

    def get_datetime_range(
        self, start_date: datetime.datetime, days_later: int = 0, months_later: int = 0
    ) -> tuple[datetime.datetime, datetime.datetime]:
        target_date = datetime.datetime.fromtimestamp(start_date.timestamp())
        target_date = target_date.replace(hour=0, minute=0, second=0, microsecond=0)

        end = datetime.datetime.fromtimestamp(target_date.timestamp())

        months = relativedelta(months=months_later)
        days = relativedelta(days=days_later)
        end = end + months + days
        if end >= target_date:
            end = end + relativedelta(days=1) - datetime.timedelta(microseconds=1)
        else:
            temp = target_date
            target_date = (
                end - relativedelta(days=1) + datetime.timedelta(microseconds=1)
            )
            end = temp + relativedelta(days=1) - datetime.timedelta(microseconds=1)
        return target_date, end

    async def find_release_date_at(self, target_date: datetime.datetime):
        start, end = self.get_datetime_range(target_date)
        return await self.find_release_date_in_range(start, end)

    async def find_release_date_in_range(
        self, minimum: None | datetime.datetime, maximum: None | datetime.datetime
    ):
        studies = await get_private_study_ids(
            study_read_repository=self.study_read_repository,
            min_expected_release_date=minimum,
            max_expected_release_date=maximum,
        )
        return studies

    async def create_report(self, report_file_path: str):
        today = datetime.datetime.now()
        today = today.replace(hour=0, minute=0, second=0, microsecond=0)
        tomorrow = today + relativedelta(days=1)
        one_week_later = today + relativedelta(days=7)
        one_month_later = today + relativedelta(months=1)

        today_studies = await self.find_release_date_at(today)
        future_1 = await self.find_release_date_at(tomorrow)
        future_2 = await self.find_release_date_at(one_week_later)
        future_3 = await self.find_release_date_at(one_month_later)

        end_of_month = (
            one_month_later + relativedelta(days=1) - datetime.timedelta(microseconds=1)
        )
        in_one_month = await self.find_release_date_in_range(tomorrow, end_of_month)
        past = await self.find_release_date_in_past()
        file_path = Path(report_file_path)

        lines = ["Release Date Summary Report for Private Studies"]

        for studies, header in [
            (today_studies, f"today: {today.strftime('%Y-%m-%d')}"),
            (future_1, f"tomorrow: {tomorrow.strftime('%Y-%m-%d')}"),
            (
                future_2,
                f"one week later: {one_week_later.strftime('%Y-%m-%d')}",
            ),
            (
                future_3,
                f"one month later: {one_month_later.strftime('%Y-%m-%d')}",
            ),
            (
                in_one_month,
                "in one month: "
                f"{tomorrow.strftime('%Y-%m-%d')} - "
                f"{one_month_later.strftime('%Y-%m-%d')}",
            ),
            (past, "in past"),
        ]:
            if not studies:
                lines.append(f"Private studies with release date {header} - No studies")
            else:
                lines.append(
                    f"Private studies with release date {header} - Total {len(studies)}"
                )
                lines.append(
                    "#:\t"
                    "STUDY_ID\t"
                    "STATUS\t"
                    "CREATED_AT\t"
                    "PRIVATE_AT\t"
                    "EXPECTED_RELEASE_DATE"
                )
                for idx, study in enumerate(studies, start=1):
                    created_at = (
                        study.created_at.strftime("%Y-%m-%d")
                        if study.created_at
                        else ""
                    )
                    first_private_date = (
                        study.first_private_date.strftime("%Y-%m-%d")
                        if study.first_private_date
                        else ""
                    )
                    release_date = (
                        study.release_date.strftime("%Y-%m-%d")
                        if study.release_date
                        else ""
                    )
                    lines.append(
                        f"{idx}\t"
                        f"{study.accession_number}\t"
                        f"{study.status.name}\t"
                        f"{created_at}\t"
                        f"{first_private_date}\t"
                        f"{release_date}"
                    )
            lines.append("")
        with file_path.open("w") as f:
            f.write("\n".join(lines))
            f.write("\n")
        logger.info("Reports is created: %s", file_path)
