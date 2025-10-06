from logging import getLogger
from typing import Annotated

from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Body, Depends, Path
from fastapi.openapi.models import Example

from mtbls.application.services.interfaces.validation_override_service import (
    ValidationOverrideService,
)
from mtbls.application.use_cases.validation.validation_overrides import (
    delete_validation_override,
    get_validation_overrides,
    patch_validation_overrides,
)
from mtbls.domain.entities.user import UserOutput
from mtbls.domain.entities.validation_override import ValidationOverrideList
from mtbls.domain.shared.validator.types import PolicyMessageType
from mtbls.domain.shared.validator.validation import (
    ValidationOverrideInput,
    ValidationOverrideUpdate,
)
from mtbls.presentation.rest_api.core.responses import APIResponse
from mtbls.presentation.rest_api.groups.auth.v1.routers.dependencies import (
    check_curator_role,
)
from mtbls.presentation.rest_api.shared.data_types import RESOURCE_ID_IN_PATH
from mtbls.presentation.rest_api.shared.dependencies import get_resource_id

logger = getLogger(__name__)

router = APIRouter(
    tags=["Study Validation Overrides"], prefix="/curation/v2/validation-overrides"
)


@router.get(
    "/{resource_id}",
    summary="Check and Get Study Validation Result",
    description="Checks Validation Task and return MetaboLights validations (if completed)",  # noqa: E501
    response_model=APIResponse[ValidationOverrideList],
)
@inject
async def get_validation_overrides_endpoint(
    resource_id: Annotated[str, Depends(get_resource_id)],
    user: Annotated[UserOutput, Depends(check_curator_role)],
    validation_override_service: ValidationOverrideService = Depends(  # noqa: FAST002
        Provide["services.validation_override_service"]
    ),
):
    overrides = await get_validation_overrides(
        resource_id=resource_id,
        validation_override_service=validation_override_service,
    )

    return APIResponse[ValidationOverrideList](
        content=overrides, success_message="Validation overrides are listed."
    )


@router.patch(
    "/{resource_id}",
    summary="Patch validation overrides for the study",
    description="Adds new override if it is not in validation overrides list or update multiple matched items.",  # noqa: E501
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
            openapi_examples={
                "Convert a rule to WARNING": Example(
                    value=[
                        ValidationOverrideInput(
                            rule_id="rule_s_100_100_002_01",
                            update=ValidationOverrideUpdate(
                                new_type=PolicyMessageType.WARNING,
                                comment="This rule is not applicable for this study.",
                            ),
                        )
                    ]
                ),
                "Convert a rule to ERROR": Example(
                    value=[
                        ValidationOverrideInput(
                            rule_id="rule_a_200_090_002_09",
                            update=ValidationOverrideUpdate(
                                new_type=PolicyMessageType.ERROR,
                                comment="This rule is required for this study.",
                            ),
                        )
                    ]
                ),
                "Define multiple overrides": Example(
                    value=[
                        ValidationOverrideInput(
                            rule_id="rule_s_100_100_002_01",
                            update=ValidationOverrideUpdate(
                                new_type=PolicyMessageType.WARNING,
                                comment="This rule is not applicable for this study.",
                            ),
                        ),
                        ValidationOverrideInput(
                            rule_id="rule_a_100_100_005_02",
                            update=ValidationOverrideUpdate(
                                new_type=PolicyMessageType.WARNING,
                                comment="This rule is not applicable for this study.",
                            ),
                        ),
                    ]
                ),
            },
        ),
    ],
    user: Annotated[UserOutput, Depends(check_curator_role)],
    validation_override_service: ValidationOverrideService = Depends(  # noqa: FAST002
        Provide["services.validation_override_service"]
    ),
):
    for override in overrides:
        if not override.update.curator:
            override.update.curator = user.email.split("@")[0]
    logger.info(
        "Override patch request for %s from user %s: %s",
        resource_id,
        user.id_,
        [x.model_dump_json() for x in overrides],
    )
    updated_overrides = await patch_validation_overrides(
        resource_id=resource_id,
        validation_overrides=overrides,
        validation_override_service=validation_override_service,
    )

    return APIResponse[ValidationOverrideList](
        content=updated_overrides, success_message="Validation overrides are updated."
    )


@router.delete(
    "/{resource_id}/{override_id}",
    summary="Delete validation override for the study.",
    description="Delete validation override for the study.",
    response_model=APIResponse[ValidationOverrideList],
)
@inject
async def delete_validation_overrides_endpoint(
    resource_id: Annotated[str, RESOURCE_ID_IN_PATH],
    override_id: Annotated[
        str, Path(description="Override id in study overrides list")
    ],
    user: Annotated[UserOutput, Depends(check_curator_role)],
    validation_override_service: ValidationOverrideService = Depends(  # noqa: FAST002
        Provide["services.validation_override_service"]
    ),
):
    overrides = await delete_validation_override(
        resource_id=resource_id,
        override_id=override_id,
        validation_override_service=validation_override_service,
    )
    return APIResponse[ValidationOverrideList](
        content=overrides, success_message="Selected validation override is deleted."
    )
