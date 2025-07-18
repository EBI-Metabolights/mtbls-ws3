from mtbls.application.services.interfaces.validation_override_service import (
    ValidationOverrideService,
)
from mtbls.application.services.interfaces.validation_report_service import (
    ValidationReportService,
)
from mtbls.application.use_cases.validation.validation_reports import (
    apply_overrides_on_validations_result,
)
from mtbls.domain.shared.validator.policy import PolicySummaryResult


async def save_validation_report(
    resource_id: str,
    task_id: str,
    validation_result: PolicySummaryResult,
    validation_report_service: ValidationReportService,  # noqa: F821
    validation_override_service: ValidationOverrideService,
) -> PolicySummaryResult:
    overrides = await validation_override_service.get_validation_overrides(
        resource_id=resource_id
    )
    overrides.validation_overrides.sort(key=lambda x: x.rule_id)
    validation_result.overrides = overrides
    await apply_overrides_on_validations_result(
        validation_result=validation_result,
        validation_override_service=validation_override_service,
    )
    await validation_report_service.save_validation_report(
        resource_id=resource_id, task_id=task_id, validation_result=validation_result
    )
    return validation_result
