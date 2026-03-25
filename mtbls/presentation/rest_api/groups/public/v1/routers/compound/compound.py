from logging import getLogger
from typing import Annotated, List, Optional

from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Body, Depends, Query
from pydantic import BaseModel, Field

from mtbls.application.services.interfaces.repositories.compound.compound_read_repository import (
    CompoundReadRepository,
)
from mtbls.application.services.interfaces.repositories.compound.compound_similarity_repository import (
    CompoundSimilarityRepository,
)
from mtbls.domain.entities.compound import Compound
from mtbls.domain.entities.similar_compound import (
    SimilarCompound,
    SimilarCompoundsResult,
)
from mtbls.presentation.rest_api.core.responses import APIResponse

logger = getLogger(__name__)

router = APIRouter(tags=["Public"], prefix="/public/v2/compound")

# Maximum number of compounds that can be retrieved in a single batch request
BATCH_SIZE_LIMIT = 100


class CompoundWithSimilar(BaseModel):
    """Compound with optional similar compounds."""

    compound: Optional[Compound] = None
    similar_compounds: Optional[List[SimilarCompound]] = None


class BatchCompoundRequest(BaseModel):
    """Request model for batch compound retrieval."""

    compound_ids: Annotated[
        List[str],
        Field(
            description="List of compound IDs to retrieve (e.g., ['MTBLC1', 'MTBLC2'])",
            min_length=1,
            max_length=BATCH_SIZE_LIMIT,
        ),
    ]


class BatchCompoundResult(BaseModel):
    """Result model for batch compound retrieval."""

    compounds: Annotated[
        List[Compound],
        Field(description="List of compounds that were found"),
    ]
    missing_ids: Annotated[
        List[str],
        Field(description="List of compound IDs that were not found"),
    ]
    total_requested: Annotated[
        int,
        Field(description="Total number of IDs requested"),
    ]
    total_found: Annotated[
        int,
        Field(description="Number of compounds found"),
    ]


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
            logger.warning(
                "Could not find similar compounds for %s: %s", compound_id, e
            )
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
        response: APIResponse[SimilarCompoundsResult] = APIResponse[
            SimilarCompoundsResult
        ]()
        response.content = result
        return response
    except ValueError as e:
        logger.error("Similarity search failed for %s: %s", compound_id, e)
        response = APIResponse[SimilarCompoundsResult]()
        response.content = SimilarCompoundsResult(
            query_compound_id=compound_id,
            similar_compounds=[],
            threshold=0.7,
            total_found=0,
        )
        response.message = str(e)
        return response


@router.post(
    "/batch",
    summary="Get multiple compounds by IDs.",
    description=f"Retrieve multiple compounds in a single request by providing a list of compound IDs. "
    f"Maximum {BATCH_SIZE_LIMIT} compounds per request. Returns found compounds and a list of any IDs that were not found.",  # noqa: E501
    response_model=APIResponse[BatchCompoundResult],
)
@inject
async def get_compounds_batch(
    request: BatchCompoundRequest = Body(...),
    include_raw: bool = Query(
        default=False,
        description="Include raw MongoDB document in response for each compound",
    ),
    compound_read_repository: CompoundReadRepository = Depends(
        Provide["repositories.compound_read_repository"]
    ),
):
    """
    Batch retrieve compounds by their IDs.

    This endpoint is more efficient than making multiple individual requests
    when you need to retrieve several compounds at once.
    """
    # Deduplicate IDs while preserving order
    unique_ids = list(dict.fromkeys(request.compound_ids))

    compounds, missing_ids = await compound_read_repository.get_compounds_by_ids(
        unique_ids
    )

    # Strip raw field if not requested
    if not include_raw:
        for compound in compounds:
            compound.raw = None

    result = BatchCompoundResult(
        compounds=compounds,
        missing_ids=missing_ids,
        total_requested=len(unique_ids),
        total_found=len(compounds),
    )

    response: APIResponse[BatchCompoundResult] = APIResponse[BatchCompoundResult]()
    response.content = result
    return response
