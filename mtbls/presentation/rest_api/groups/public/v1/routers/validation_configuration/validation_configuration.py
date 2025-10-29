from logging import getLogger
from typing import Annotated, Literal

from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends, Query

from mtbls.application.services.interfaces.policy_service import PolicyService
from mtbls.domain.entities.validation.validation_configuration import (
    FileTemplates,
    ValidationConfiguration,
    ValidationControls,
)
from mtbls.presentation.rest_api.core.responses import (
    APIResponse,
)

logger = getLogger(__name__)

router = APIRouter(tags=["Public"], prefix="/public/v2")


@router.get(
    "/validations/configuration",
    summary="Search ontology terms.",
    description="Search ontology terms using input",
    response_model=APIResponse[ValidationConfiguration],
)
@inject
async def get_validation_configuration(
    policy_service: PolicyService = Depends(  # noqa: FAST002
        Provide["services.policy_service"]
    ),
):
    controls_dict = await policy_service.get_control_lists()
    templates_dict = await policy_service.get_templates()

    return APIResponse[ValidationConfiguration](
        content=ValidationConfiguration(
            controls=ValidationControls.model_validate(controls_dict, by_alias=True),
            templates=FileTemplates.model_validate(templates_dict, by_alias=True),
        )
    )


@router.get(
    "/validations/configuration/templates",
    summary="Search ontology terms.",
    description="Search ontology terms using input",
    response_model=APIResponse[FileTemplates],
)
@inject
async def get_validation_templates(
    policy_service: PolicyService = Depends(  # noqa: FAST002
        Provide["services.policy_service"]
    ),
    version: Annotated[
        None | Literal["1.0", "2.0"], Query(title="template version")
    ] = None,
):
    templates_dict = await policy_service.get_templates()

    templates = FileTemplates.model_validate(templates_dict, by_alias=True)
    content = templates
    if version:
        filtered = FileTemplates()

        for k, items in templates.assay_file_header_templates.items():
            filtered.assay_file_header_templates[k] = [
                x for x in items if x.version == version
            ]
        for k, items in templates.sample_file_header_templates.items():
            filtered.sample_file_header_templates[k] = [
                x for x in items if x.version == version
            ]
        for k, items in templates.assignment_file_header_templates.items():
            filtered.assignment_file_header_templates[k] = [
                x for x in items if x.version == version
            ]
        content = filtered
    return APIResponse[FileTemplates](content=content)
