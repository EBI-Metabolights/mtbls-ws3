from logging import getLogger
from typing import List, Optional

from fastapi import APIRouter, Depends, Query
from dependency_injector.wiring import Provide, inject
from pydantic import BaseModel

from mtbls.application.services.interfaces.repositories.compound.compound_read_repository import (
    CompoundReadRepository,
)
from mtbls.application.services.interfaces.repositories.compound.compound_similarity_repository import (
    CompoundSimilarityRepository,
)
from mtbls.domain.entities.compound import Compound
from mtbls.domain.entities.similar_compound import SimilarCompound, SimilarCompoundsResult
from mtbls.presentation.rest_api.core.responses import APIResponse


logger = getLogger(__name__)

router = APIRouter(tags=["Public"], prefix="/public/v2/compound")


class CompoundWithSimilar(BaseModel):
    """Compound with optional similar compounds."""

    compound: Optional[Compound] = None
    similar_compounds: Optional[List[SimilarCompound]] = None


@router.get(
    "/{compound_id}",
    summary="Get compound by ID.",
    description="Retrieve compound information by its unique identifier. "
    "Optionally include similar compounds by setting include_similar=true. "
    "Optionally include raw MongoDB document by setting include_raw=true.",
    response_model=APIResponse[CompoundWithSimilar],
)
@inject
async def get_compound_by_id(
    compound_id: str,
    include_similar: bool = Query(
        default=False,
        description="Include list of structurally similar compounds",
    ),
    include_raw: bool = Query(
        default=False,
        description="Include raw MongoDB document in response",
    ),
    compound_read_repository: CompoundReadRepository = Depends(
        Provide["repositories.compound_read_repository"]
    ),
    compound_similarity_repository: CompoundSimilarityRepository = Depends(
        Provide["repositories.compound_similarity_repository"]
    ),
):
    compound = await compound_read_repository.get_compound_by_id(compound_id)

    # Strip raw field if not requested
    if compound and not include_raw:
        compound.raw = None

    similar_compounds = None
    if include_similar and compound:
        try:
            similar_compounds = await compound_similarity_repository.find_similar_by_id(
                compound_id
            )
        except ValueError as e:
            logger.warning(f"Could not find similar compounds for {compound_id}: {e}")
            similar_compounds = []

    result = CompoundWithSimilar(compound=compound, similar_compounds=similar_compounds)
    response: APIResponse[CompoundWithSimilar] = APIResponse[CompoundWithSimilar]()
    response.content = result
    return response


@router.get(
    "/{compound_id}/similar",
    summary="Get similar compounds.",
    description="Find compounds structurally similar to the specified compound "
    "using Tanimoto similarity on Morgan fingerprints.",
    response_model=APIResponse[SimilarCompoundsResult],
)
@inject
async def get_similar_compounds(
    compound_id: str,
    compound_similarity_repository: CompoundSimilarityRepository = Depends(
        Provide["repositories.compound_similarity_repository"]
    ),
):
    try:
        similar_compounds = await compound_similarity_repository.find_similar_by_id(
            compound_id
        )
        result = SimilarCompoundsResult(
            query_compound_id=compound_id,
            similar_compounds=similar_compounds,
            threshold=0.7,  # TODO: get from config
            total_found=len(similar_compounds),
        )
        response: APIResponse[SimilarCompoundsResult] = APIResponse[SimilarCompoundsResult]()
        response.content = result
        return response
    except ValueError as e:
        logger.error(f"Similarity search failed for {compound_id}: {e}")
        response = APIResponse[SimilarCompoundsResult]()
        response.content = SimilarCompoundsResult(
            query_compound_id=compound_id,
            similar_compounds=[],
            threshold=0.7,
            total_found=0,
        )
        response.message = str(e)
        return response