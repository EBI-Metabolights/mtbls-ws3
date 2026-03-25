import asyncio
import datetime
import logging

from mtbls.application.use_cases.study_status.release_date_report import (
    PrivateStudyReleaseDateReporter,
)
from mtbls.run.cli.validation.validation_app import ValidationApp
from mtbls.run.config_utils import get_application_config_files

logger = logging.getLogger(__name__)


if __name__ == "__main__":
    today = datetime.datetime.now()
    config_file, secrets_file = get_application_config_files()
    app = ValidationApp(config_file=config_file, secrets_file=secrets_file)

    report_path = f"./.temp/report-{today.strftime('%Y-%m-%d')}.tsv"
    reporter = PrivateStudyReleaseDateReporter(app.study_read_repository)
    asyncio.run(reporter.create_report(report_path))
