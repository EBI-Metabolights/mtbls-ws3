import logging

from mtbls.application.services.interfaces.validation_override_service import (
    ValidationOverrideService,
)
from mtbls.domain.entities.validation.validation_override import ValidationOverrideList
from mtbls.domain.shared.validator.validation import (
    ValidationOverrideInput,
)

logger = logging.getLogger(__name__)


async def patch_validation_overrides(
    resource_id: str,
    validation_override: ValidationOverrideInput,
    validation_override_service: ValidationOverrideService,
) -> ValidationOverrideList:
    repo = validation_override_service
    overrides_content = await repo.get_validation_overrides(resource_id=resource_id)
    version = overrides_content.validation_version
    return ValidationOverrideList(validation_version=version)
