import datetime
from unittest.mock import Mock

import pytest

from mtbls.application.services.interfaces.async_task.async_task_service import (
    AsyncTaskService,
)
from mtbls.application.services.interfaces.cache_service import CacheService
from mtbls.application.services.interfaces.validation_override_service import (
    ValidationOverrideService,
)
from mtbls.application.services.interfaces.validation_report_service import (
    ValidationReportService,
)
from mtbls.domain.shared.modifier import UpdateLog
from mtbls.domain.shared.validator.policy import (
    PolicyMessage,
    PolicyResult,
    PolicyResultList,
    ValidationResult,
)
from mtbls.domain.shared.validator.types import PolicyMessageType


@pytest.fixture(scope="function")
def async_task_service() -> AsyncTaskService:
    service = Mock(spec=AsyncTaskService)

    return service


@pytest.fixture(scope="function")
def cache_service() -> AsyncTaskService:
    service = Mock(spec=CacheService)
    return service


@pytest.fixture(scope="function")
def validation_report_service() -> ValidationReportService:
    service = Mock(spec=ValidationReportService)
    return service


@pytest.fixture(scope="function")
def validation_override_service() -> ValidationOverrideService:
    service = Mock(spec=ValidationOverrideService)
    return service


@pytest.fixture(scope="function")
def get_validation_result() -> PolicyResultList:
    def build_validation_result(resource_id: str):
        now = datetime.datetime.now(datetime.timezone.utc)
        result_list: PolicyResultList = PolicyResultList(
            results=[
                PolicyResult(
                    resource_id=resource_id,
                    metadata_modifier_enabled=True,
                    metadata_updates=[
                        UpdateLog(
                            action="test",
                            source="i_Investigation.txt",
                            new_value="",
                            old_value="",
                        )
                    ],
                    start_time=now,
                    completion_time=now,
                    assay_file_techniques={},
                    maf_file_techniques={},
                    messages=ValidationResult(
                        violations=[
                            PolicyMessage(
                                identifier="rule_11",
                                type=PolicyMessageType.ERROR,
                                title="rule title",
                                description="rule description",
                            )
                        ],
                        summary=[
                            PolicyMessage(
                                identifier="summary_rule_11",
                                type=PolicyMessageType.INFO,
                                title="summary title",
                                description="summary description",
                            )
                        ],
                    ),
                )
            ],
        )
        return result_list

    return build_validation_result
