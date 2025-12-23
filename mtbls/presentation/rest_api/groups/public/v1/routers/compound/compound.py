
from logging import getLogger

from fastapi import APIRouter, Depends
from dependency_injector.wiring import Provide, inject

from mtbls.application.services.interfaces.repositories.compound.compound_read_repository import CompoundReadRepository
from mtbls.domain.entities.compound import Compound
from mtbls.presentation.rest_api.core.responses import APIResponse


logger = getLogger(__name__)

router = APIRouter(tags=["Public"], prefix="/public/v2/compound")


@router.get(
    "/{compound_id}",
    summary="Get compound by ID.",
    description="Retrieve compound information by its unique identifier.",
    response_model=APIResponse[Compound],
)
@inject
async def get_compound_by_id(
    compound_id: str,
    compound_read_repository: CompoundReadRepository =Depends(
        Provide["repositories.compound_read_repository"]
    ),
):
    compound = await compound_read_repository.get_compound_by_id(compound_id)
    response: APIResponse[Compound] = APIResponse[Compound]()
    response.content = compound
    return response