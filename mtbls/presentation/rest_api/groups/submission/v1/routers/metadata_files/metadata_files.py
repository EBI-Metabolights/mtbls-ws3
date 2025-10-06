from logging import getLogger
from typing import Annotated

from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends

from mtbls.application.services.interfaces.study_metadata_service_factory import (
    StudyMetadataServiceFactory,
)
from mtbls.domain.entities.study_file import StudyFileOutput
from mtbls.domain.shared.permission import StudyPermissionContext
from mtbls.presentation.rest_api.core.responses import (
    APIListResponse,
)
from mtbls.presentation.rest_api.groups.auth.v1.routers.dependencies import (
    check_read_permission,
)
from mtbls.presentation.rest_api.shared.data_types import RESOURCE_ID_IN_PATH

logger = getLogger(__name__)

router = APIRouter(tags=["Metadata Files"], prefix="/submissions/v2/metadata-files")


@router.get(
    "/{resource_id}",
    summary="Get ISA metadata files",
    description="Get ISA metadata files",
    response_model=APIListResponse[StudyFileOutput],
)
@inject
async def get_isa_table_files_wrapper(
    resource_id: Annotated[str, RESOURCE_ID_IN_PATH],
    context: Annotated[StudyPermissionContext, Depends(check_read_permission)],
    study_metadata_service_factory: StudyMetadataServiceFactory = Depends(
        Provide["services.study_metadata_service_factory"]
    ),
):
    resource_id = context.study.accession_number
    metadata_service = await study_metadata_service_factory.create_service(resource_id)
    with metadata_service:
        isa_files = await metadata_service.list_isa_files()

    response = APIListResponse[StudyFileOutput](content=isa_files)
    return response
