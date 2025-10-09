from logging import getLogger
from typing import Annotated

from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Body, Depends

from mtbls.application.services.interfaces.validation_override_service import (
    ValidationOverrideService,
)
from mtbls.application.use_cases.validation.validation_overrides import (
    patch_validation_overrides,
)
from mtbls.domain.entities.user import UserOutput
from mtbls.domain.entities.validation_override import ValidationOverrideList
from mtbls.domain.shared.validator.validation import (
    ValidationOverrideInput,
)
from mtbls.presentation.rest_api.core.responses import APIErrorResponse, APIResponse
from mtbls.presentation.rest_api.groups.auth.v1.routers.dependencies import (
    check_curator_role,
)
from mtbls.presentation.rest_api.shared.dependencies import get_resource_id

logger = getLogger(__name__)

router = APIRouter(tags=["Study Validation Overrides"], prefix="/curation/v1")


@router.patch(
    "/{resource_id}/validation-overrides",
    summary="Patch validation overrides for the study",
    description="Adds new override if it is not overrides list ",
    response_model=APIResponse[ValidationOverrideList],
)
@inject
async def patch_validation_overrides_endpoint(
    resource_id: Annotated[str, Depends(get_resource_id)],
    overrides: Annotated[
        list[ValidationOverrideInput],
        Body(
            title="Validation override input.",
            description="Override filters and updates",
        ),
    ],
    user: Annotated[UserOutput, Depends(check_curator_role)],
    validation_override_service: ValidationOverrideService = Depends(
        Provide["services.validation_override_service"]
    ),
):
    if not overrides:
        return APIErrorResponse(error_message="No overrides.")

    logger.info(
        "Override patch request for %s from user %s: %s",
        resource_id,
        user.id_,
        [x.model_dump_json() for x in overrides],
    )
    overrides = await patch_validation_overrides(
        resource_id=resource_id,
        validation_overrides=overrides,
        validation_override_service=validation_override_service,
    )

    return APIResponse[ValidationOverrideList](
        content=overrides, success_message="Validation overrides are updated."
    )
