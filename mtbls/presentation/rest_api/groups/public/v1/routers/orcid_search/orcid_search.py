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
    APIResponse,
)
from mtbls.presentation.rest_api.groups.public.v1.routers.orcid_search.schemas import (
    EuroPmcSearchResult,
)
from mtbls.presentation.rest_api.groups.public.v1.routers.orcid_search.service import (
    find_studies_on_europmc_by_orcid,
)

logger = getLogger(__name__)

router = APIRouter(tags=["Public"], prefix="/public/v2")


@router.get(
    "/orcids/{orcid}/studies",
    summary="Find referenced MetaboLights studies using ORCID.",
    description="Search articles of the researcer with ORCID on EuropePMC "
    "and find studies referenced in those articles.",
    response_model=APIResponse[EuroPmcSearchResult],
)
@inject
async def get_studies_by_orcid(
    response: Response,
    orcid: Annotated[str, Path(title="ORCID")],
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

    all_studies: list[StudyOutput] = await study_read_repository.get_studies_by_orcid(
        orcid=orcid
    )

    study_list = await find_studies_on_europmc_by_orcid(
        http_client=http_client,
        study_read_repository=study_read_repository,
        orcid_id=orcid,
        submitter_public_studies=all_studies,
    )
    response: APIResponse[EuroPmcSearchResult] = APIResponse[EuroPmcSearchResult](
        content=EuroPmcSearchResult(study_list=study_list)
    )
    return response
