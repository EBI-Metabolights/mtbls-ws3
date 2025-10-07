from logging import getLogger
from typing import Annotated

from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends, Path, Response, status

from mtbls.application.services.interfaces.http_client import HttpClient
from mtbls.application.services.interfaces.repositories.study.study_read_repository import (  # noqa E501
    StudyReadRepository,
)
from mtbls.domain.entities.study import StudyOutput
from mtbls.presentation.rest_api.core.responses import (
    APIErrorResponse,
    APIListResponse,
)
from mtbls.presentation.rest_api.groups.public.v1.routers.studies.schemas import (
    StudyTitle,
)
from mtbls.presentation.rest_api.groups.public.v1.routers.studies.service import (
    find_studies_on_europmc_by_orcid,
)

logger = getLogger(__name__)

router = APIRouter(tags=["Public"], prefix="/public/v2")


@router.get(
    "/orcid-ids/{orcid}/studies",
    summary="Get MetaboLights studies based on ORCID ID.",
    description="Search ORCID ID on EuropePMC, "
    "get the articles and the study referenced by those articles.",
    response_model=APIListResponse[StudyTitle],
)
@inject
async def get_studies_by_orcid(
    response: Response,
    orcid: Annotated[str, Path(title="ORCID Id")],
    study_read_repository: StudyReadRepository = Depends(  # noqa: FAST002
        Provide["repositories.study_read_repository"]
    ),
    http_client: HttpClient = Depends(  # noqa: FAST002
        Provide["gateways.http_client"]
    ),
):
    if not orcid:
        response.status_code = status.HTTP_400_BAD_REQUEST
        response_message = APIErrorResponse(error="ORCID is not valid.")
        return response_message

    studies: list[StudyOutput] = await study_read_repository.get_studies_by_orcid(
        orcid=orcid
    )
    if not studies:
        response.status_code = status.HTTP_404_NOT_FOUND
        response_message = APIErrorResponse(error="ORCID is not defined in database.")
        return response_message

    study_titles = await find_studies_on_europmc_by_orcid(
        http_client=http_client, orcid_id=orcid, submitter_studies=studies
    )
    response: APIListResponse[StudyTitle] = APIListResponse[StudyTitle]()
    response.content = study_titles
    return response
