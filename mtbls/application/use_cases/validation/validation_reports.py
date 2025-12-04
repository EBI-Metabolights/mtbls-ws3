from typing import Union

from mtbls.application.services.interfaces.validation_override_service import (
    ValidationOverrideService,
)
from mtbls.application.services.interfaces.validation_report_service import (
    ValidationReportService,
)
from mtbls.domain.shared.data_types import ZeroOrPositiveInt
from mtbls.domain.shared.validation_result_file import ValidationResultFile
from mtbls.domain.shared.validator.policy import PolicyMessage, PolicySummaryResult
from mtbls.domain.shared.validator.types import PolicyMessageType
from mtbls.domain.shared.validator.validation import ValidationOverride


async def get_validation_reports(
    resource_id: str,
    offset: Union[None, ZeroOrPositiveInt],
    limit: Union[None, ZeroOrPositiveInt],
    validation_report_service: ValidationReportService,
) -> list[ValidationResultFile]:
    return await validation_report_service.find_all(
        resource_id=resource_id, offset=offset, limit=limit
    )


async def load_validation_report_by_task_id(
    resource_id: str,
    task_id: str,
    validation_report_service: ValidationReportService,
) -> PolicySummaryResult:
    return await validation_report_service.load_validation_report_by_task_id(
        resource_id=resource_id, task_id=task_id
    )


async def override_and_save_validation_report(
    resource_id: str,
    task_id: str,
    validation_result: PolicySummaryResult,
    validation_report_service: ValidationReportService,
    validation_override_service: ValidationOverrideService,
) -> PolicySummaryResult:
    overrides = await validation_override_service.get_validation_overrides(
        resource_id=resource_id
    )
    overrides.validation_overrides.sort(key=lambda x: x.rule_id + x.source_file)
    validation_result.overrides = overrides
    await apply_overrides_on_validations_result(
        validation_result=validation_result,
        validation_override_service=validation_override_service,
    )
    await validation_report_service.save_validation_report(
        resource_id=resource_id, task_id=task_id, validation_result=validation_result
    )
    return validation_result


async def apply_overrides_on_validations_result(
    validation_result: PolicySummaryResult,
    validation_override_service: ValidationOverrideService,
):
    overrides = validation_result.overrides

    overrides_map: dict[str, list[ValidationOverride]] = {}
    if overrides.validation_overrides:
        for override in overrides.validation_overrides:
            if override.enabled:
                if override.rule_id not in overrides_map:
                    overrides_map[override.rule_id] = []
                overrides_map[override.rule_id].append(override)
    summary_overrides: dict[str, set[PolicyMessageType]] = {}
    all_violations_map: dict[str, PolicyMessage] = {}
    if overrides_map:
        for violation in validation_result.messages.violations:
            all_violations_map[violation.identifier] = violation
            if violation.identifier in overrides_map:
                override = match_override(
                    overrides_map[violation.identifier], violation
                )
                if override:
                    violation.overridden = True
                    violation.override_comment = override.comment
                    violation.type = override.new_type
            summary_rule_id = violation.identifier[:14]
            if summary_rule_id not in summary_overrides:
                summary_overrides[summary_rule_id] = set()
            summary_overrides[summary_rule_id].add(violation.type)
        definitions = await validation_override_service.get_validation_definitions()

        for rule_id, overrides in overrides_map.items():
            for override in overrides:
                if override.enabled and rule_id not in all_violations_map:
                    definition = (
                        definitions.validations[rule_id]
                        if rule_id in definitions.validations
                        else None
                    )
                    rule_id = definition.rule_id if definition else override.rule_id
                    title = definition.title if definition else override.title
                    description = (
                        definition.description if definition else override.description
                    )
                    priority = definition.priority if definition else "HIGH"
                    section = definition.section if definition else ""
                    validation_result.messages.violations.append(
                        PolicyMessage(
                            identifier=rule_id,
                            title=title,
                            description=description,
                            priority=priority,
                            section=section,
                            type=override.new_type,
                            overridden=True,
                            override_comment=override.comment,
                            source_file=override.source_file,
                            source_column_header=override.source_column_header,
                            source_column_index=override.source_column_index,
                        )
                    )

    if not validation_result.messages.violations:
        validation_result.status = PolicyMessageType.SUCCESS
    else:
        violation_types: dict[PolicyMessageType, list[PolicyMessage]] = {}
        for violation in validation_result.messages.violations:
            if violation.type not in violation_types:
                violation_types[violation.type] = []
            violation_types[violation.type].append(violation)
        status = select_message_type(set(violation_types.keys()))
        validation_result.status = status

    for summary in validation_result.messages.summary:
        if summary.identifier in summary_overrides:
            message_type = select_summary_message_type(
                summary_overrides[summary.identifier]
            )
            if message_type != summary.type:
                summary.overridden = True

    validation_result.messages.violations.sort(
        key=lambda x: x.identifier + x.source_file + x.source_column_header
    )


def match_override(overrides: list[ValidationOverride], violation: PolicyMessage):
    override = None
    matches = []
    for x in overrides:
        if x.rule_id == violation.identifier:
            matches.append(1)
        if not x.source_file or x.source_file == violation.source_file:
            matches.append(1)
        if (
            not x.source_column_header
            or x.source_column_header == violation.source_column_header
        ):
            matches.append(1)
        if not x.source_file or x.source_column_index == violation.source_column_index:
            matches.append(1)
        if len(matches) == 4:
            override = x
            break
    return override


def select_message_type(violation_types: set[PolicyMessageType]):
    if PolicyMessageType.ERROR in violation_types:
        return PolicyMessageType.ERROR
    if PolicyMessageType.WARNING in violation_types:
        return PolicyMessageType.WARNING
    if PolicyMessageType.INFO in violation_types:
        return PolicyMessageType.INFO
    if PolicyMessageType.SUCCESS in violation_types:
        return PolicyMessageType.SUCCESS
    return PolicyMessageType.ERROR


def select_summary_message_type(violation_types: set[PolicyMessageType]):
    if PolicyMessageType.ERROR in violation_types:
        return PolicyMessageType.ERROR
    if PolicyMessageType.WARNING in violation_types:
        return PolicyMessageType.WARNING

    return PolicyMessageType.SUCCESS
