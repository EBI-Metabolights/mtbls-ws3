import datetime
import logging
import uuid

from mtbls.application.services.interfaces.validation_override_service import (
    ValidationOverrideService,
)
from mtbls.domain.entities.validation_override import ValidationOverrideList
from mtbls.domain.shared.validator.types import PolicyMessageType
from mtbls.domain.shared.validator.validation import (
    ValidationOverride,
    ValidationOverrideInput,
)

logger = logging.getLogger(__name__)


async def get_validation_overrides(
    resource_id: str,
    validation_override_service: ValidationOverrideService,
) -> ValidationOverrideList:
    return await validation_override_service.get_validation_overrides(
        resource_id=resource_id
    )


async def patch_validation_overrides(
    resource_id: str,
    validation_overrides: list[ValidationOverrideInput],
    validation_override_service: ValidationOverrideService,
) -> ValidationOverrideList:
    repo = validation_override_service
    overrides_content = await repo.get_validation_overrides(resource_id=resource_id)
    definitions = await repo.get_validation_definitions()

    for new_item in validation_overrides:
        if not new_item.update.new_type:
            raise ValueError("new_type must be defined.")

        if not new_item.rule_id and not new_item.override_id:
            raise ValueError("rule_id or override_id must be defined.")

        matched_overrides, unmatched_overrides = filter_overrides(
            overrides_content, new_item
        )
        now = datetime.datetime.now(datetime.timezone.utc)
        validations = definitions.validations
        if not matched_overrides:
            validation_definition = (
                validations[new_item.rule_id]
                if new_item.rule_id in validations
                else None
            )
            item = ValidationOverride(
                override_id=str(uuid.uuid4()),
                created_at=now,
                modified_at="",
                rule_id=new_item.rule_id,
                source_file=new_item.source_file,
                source_column_index=new_item.source_column_index,
                source_column_header=new_item.source_column_header,
                new_type=new_item.update.new_type,
                enabled=new_item.update.enabled,
                curator=new_item.update.curator,
                comment=new_item.update.comment,
                old_type=(
                    validation_definition.type
                    if validation_definition
                    else PolicyMessageType.ERROR
                ),
                title=validation_definition.title if validation_definition else "",
                description=(
                    validation_definition.description if validation_definition else ""
                ),
            )
            unmatched_overrides.append(item)
            logger.info(
                "New validation rule override (%s) is added for study %s: %s",
                item.rule_id,
                resource_id,
                item.model_dump_json(),
            )

        else:
            for item in matched_overrides:
                item.enabled = new_item.update.enabled
                item.curator = (
                    new_item.update.curator if new_item.update.curator else item.curator
                )
                item.comment = (
                    new_item.update.comment if new_item.update.comment else item.comment
                )
                item.new_type = new_item.update.new_type
                item.modified_at = now
                if not item.created_at:
                    item.created_at = now
                unmatched_overrides.append(item)
            updated_rules = [x.override_id for x in matched_overrides]
            updated_values = {
                "enabled": new_item.update.enabled,
                "new_type": new_item.update.new_type.value,
            }
            if new_item.update.curator:
                updated_values["curator"] = new_item.update.curator
            if new_item.update.comment:
                updated_values["comment"] = new_item.update.comment
            logger.info(
                "Validation rules are updated for %s. Updated override ids: %s. Updated value: %s",  # noqa: E501
                resource_id,
                updated_rules,
                updated_values,
            )
        unmatched_overrides.sort(
            key=lambda x: f"{x.rule_id}:{x.source_file}:{x.source_column_header}"
        )
        overrides_content.validation_overrides = unmatched_overrides

    await repo.save_validation_overrides(
        resource_id=resource_id, validation_overrides=overrides_content
    )
    return overrides_content


async def delete_validation_override(
    resource_id: str,
    override_id: str,
    validation_override_service: ValidationOverrideService,
) -> ValidationOverrideList:
    if not resource_id or not override_id:
        raise ValueError("resource_id and override_id must be defined.")

    repo = validation_override_service
    overrides_content = await repo.get_validation_overrides(resource_id=resource_id)

    unmatched_overrides: list[ValidationOverride] = []

    for override in overrides_content.validation_overrides:
        if override.override_id != override_id:
            unmatched_overrides.append(override)

    unmatched_overrides.sort(
        key=lambda x: f"{x.rule_id}:{x.source_file}:{x.source_column_header}"
    )
    overrides_content.validation_overrides = unmatched_overrides
    await repo.save_validation_overrides(
        resource_id=resource_id, validation_overrides=overrides_content
    )
    return overrides_content


def filter_overrides(
    overrides_content: ValidationOverrideList, new_item: ValidationOverrideInput
):
    unmatched_overrides: list[ValidationOverride] = []
    matched_overrides: list[ValidationOverride] = []
    for override in overrides_content.validation_overrides:
        checks = []
        if new_item.override_id:
            checks.append(override.override_id == new_item.override_id)
        if new_item.rule_id:
            checks.append(override.rule_id == new_item.rule_id)
        if new_item.source_file:
            checks.append(override.source_file == new_item.source_file)
        if new_item.source_column_header:
            checks.append(
                override.source_column_header == new_item.source_column_header
            )
        if (
            isinstance(new_item.source_column_index, str)
            and new_item.source_column_index
        ):
            checks.append(
                str(override.source_column_index) == new_item.source_column_index
            )
        elif (
            isinstance(new_item.source_column_index, int)
            and new_item.source_column_index > -1
        ):
            checks.append(
                str(override.source_column_index) == str(new_item.source_column_index)
            )
        matches = sum(1 for x in checks if x)
        if len(checks) > 0 and matches == len(checks):
            matched_overrides.append(override)
        else:
            unmatched_overrides.append(override)
    return matched_overrides, unmatched_overrides
