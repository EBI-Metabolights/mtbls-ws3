from logging import getLogger

from mtbls.application.services.interfaces.policy_service import PolicyService
from mtbls.domain.entities.validation.validation_configuration import (
    FileTemplates,
    ValidationConfiguration,
    ValidationControls,
)

logger = getLogger(__name__)


async def get_validation_configuration(
    policy_service: PolicyService,
) -> ValidationConfiguration:
    controls: ValidationControls = await policy_service.get_control_lists()
    templates: FileTemplates = await policy_service.get_templates()

    return ValidationConfiguration(controls=controls, templates=templates)
