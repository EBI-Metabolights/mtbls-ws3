from logging import getLogger
from typing import Annotated

from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends, Response, status

from mtbls.application.services.interfaces.repositories.study.study_read_repository import (  # noqa: E501
    StudyReadRepository,
)
from mtbls.application.services.interfaces.repositories.study.study_write_repository import (  # noqa: E501
    StudyWriteRepository,
)
from mtbls.domain.entities.study import StudyOutput
from mtbls.domain.enums.user_role import UserRole
from mtbls.domain.shared.permission import StudyPermissionContext
from mtbls.presentation.rest_api.core.responses import APIErrorResponse, APIResponse
from mtbls.presentation.rest_api.groups.auth.v1.routers.dependencies import (
    check_read_permission,
    check_update_permission,
)
from mtbls.presentation.rest_api.groups.submission.v1.routers.dataset_license.schemas import (  # noqa: E501
    LICENSE_URLS,
    DatasetLicense,
    DatasetLicenseInfoConfiguration,
    DatasetLicenseResponse,
)

logger = getLogger(__name__)

router = APIRouter(
    tags=["MetaboLights Submission"],
    prefix="/submissions/v2/dataset-licenses",
)


@router.put(
    "/{resource_id}",
    summary="Add dataset license agreement for a study.",
    description="Update a study to indicate that the user has agreed "
    "to the dataset license and data policy. "
    "This is mandatory but is handled as a separate request.",
    response_model=APIResponse[DatasetLicenseResponse],
)
@inject
async def create_license_agreement(
    response: Response,
    context: Annotated[StudyPermissionContext, Depends(check_update_permission)],
    study_write_repository: StudyWriteRepository = Depends(  # noqa: FAST002
        Provide["repositories.study_write_repository"]
    ),
    study_read_repository: StudyReadRepository = Depends(  # noqa: FAST002
        Provide["repositories.study_read_repository"]
    ),
    default_dataset_license_config: DatasetLicenseInfoConfiguration = Depends(  # noqa: FAST002
        Provide["default_dataset_license_config"]
    ),
):
    resource_id = context.study.accession_number
    study: StudyOutput = await study_read_repository.get_study_by_accession(resource_id)
    license_name = study.dataset_license or ""
    license_version = study.dataset_license_version or ""
    license_url = LICENSE_URLS.get((license_name.upper(), license_version.upper()))
    agreeing_user = study.dataset_license_agreeing_user or ""
    if not license_url:
        license_url = ""
    default_license_name = default_dataset_license_config.name or ""
    default_license_version = default_dataset_license_config.version or ""

    if not default_license_name and not default_license_version:
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        response_message = APIErrorResponse(
            error="Dataset license is not configured properly."
        )
        response_message.content = DatasetLicenseResponse(
            dataset=None, message="Dataset license is not configured properly."
        )
        return response_message
    dl = DatasetLicense(
        name=license_name,
        agreed=True if len(license_name) > 0 else False,
        version=license_version,
        agreeing_user=agreeing_user,
        license_url=license_url,
    )
    if (
        license_name == default_license_name
        and license_version == default_license_version
    ):
        response_message = APIResponse()
        response_message.content = DatasetLicenseResponse(
            dataset=dl, message="Dataset license is same."
        )
        return response_message

    if (
        license_name
        and license_version
        and context.user.role not in (UserRole.CURATOR, UserRole.SYSTEM_ADMIN)
    ):
        msg = "A user has already agreed to the dataset license for this study."
        response.status_code = status.HTTP_403_FORBIDDEN
        response_message = APIErrorResponse(error_message=msg)
        response_message.content = DatasetLicenseResponse(
            dataset=None, message="Operation failed."
        )
        return response_message
    study.dataset_license = default_license_name
    study.dataset_license_version = default_license_version
    study.dataset_license_agreeing_user = str(context.user.id_)
    default_license_url = LICENSE_URLS.get(
        (default_license_name.upper(), default_license_version.upper())
    )
    dl = DatasetLicense(
        name=default_license_name,
        agreed=True,
        version=default_license_version,
        agreeing_user=str(context.user.id_),
        license_url=default_license_url,
    )
    await study_write_repository.update(entity=study)

    # dump happy response
    response = APIResponse[DatasetLicenseResponse]()
    response.content = DatasetLicenseResponse(dataset=dl)
    response.content.message = f"Dataset license saved successfully for {resource_id}"
    return response


@router.get(
    "/{resource_id}",
    summary="Add dataset license agreement for a study.",
    description="Update a study to indicate that the user has agreed to the dataset license and data policy. This is mandatory but is handled as a separate request.",  # noqa: E501
    response_model=APIResponse[DatasetLicenseResponse],
)
@inject
async def get_study_license_agreement(
    response: Response,
    context: Annotated[StudyPermissionContext, Depends(check_read_permission)],
    study_read_repository: StudyReadRepository = Depends(  # noqa: FAST002
        Provide["repositories.study_read_repository"]
    ),
):
    resource_id = context.study.accession_number
    study: StudyOutput = await study_read_repository.get_study_by_accession(resource_id)
    license_name = study.dataset_license or ""
    license_version = study.dataset_license_version or ""
    dataset_license_agreeing_user = study.dataset_license_agreeing_user or ""
    license_url = LICENSE_URLS.get((license_name.upper(), license_version.upper()))
    if not license_url:
        license_url = ""

    dl = DatasetLicense(
        name=license_name,
        agreed=True if len(license_name) > 0 else False,
        version=license_version,
        agreeing_user=dataset_license_agreeing_user,
        license_url=license_url,
    )

    response: APIResponse[DatasetLicenseResponse] = APIResponse[
        DatasetLicenseResponse
    ]()
    response.content = DatasetLicenseResponse(dataset=dl)
    response.content.message = (
        f"Dataset license retrieved {resource_id}"
        if dl is not None
        else "This study has no agreed license"
    )
    return response
