import json
from logging import getLogger
from typing import Annotated, Any

from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Body, Depends, Query, Response, status
from fastapi.responses import StreamingResponse

from mtbls.domain.entities.search.index_search import (
    IndexSearchResult,
    StudySearchInput,
)
from mtbls.presentation.rest_api.core.responses import APIErrorResponse, APIResponse

logger = getLogger(__name__)

router = APIRouter(tags=["Public"], prefix="/public/v2/public-study-index")


@router.post(
    "/search",
    summary="MetaboLights Study Search from public study index.",
    description=(
        "MetaboLights search results from public study index. "
        "Sanitised output format. Set include_all_ids=true to get all matching "
        "study IDs alongside paginated results (only when query or filters are applied)."
    ),
    response_model=APIResponse[IndexSearchResult],  # IndexSearchResult or raw dict
)
@inject
async def search_study_index(
    response: Response,
    q: Annotated[StudySearchInput, Body()],
    include_all_ids: bool = Query(
        default=False,
        description="Include all matching study IDs in response "
        "(only populated when query or filters are applied)",
    ),
    elasticsearch_study_search_service=Depends(
        Provide["gateways.elasticsearch_study_gateway"]
    ),
):
    if not q:
        response.status_code = status.HTTP_400_BAD_REQUEST
        response_message = APIErrorResponse(error="Search query is not valid.")
        return response_message
    result = await elasticsearch_study_search_service.search(
        query=q, include_all_ids=include_all_ids
    )
    response: APIResponse[IndexSearchResult] = APIResponse[IndexSearchResult](
        content=result
    )
    return response


@router.post(
    "/search/raw",
    summary="MetaboLights Study Search from public study index. (Raw ES Response)",
    description=(
        "MetaboLights Statistics from public study indexRaw Elasticsearch Response format for APIConnectors."
    ),
    response_model=APIResponse[Any],
)
@inject
async def search_study_index_raw(
    response: Response,
    q: Annotated[StudySearchInput, Body()],
    elasticsearch_study_search_service=Depends(
        Provide["gateways.elasticsearch_study_gateway"]
    ),
):
    if not q:
        response.status_code = status.HTTP_400_BAD_REQUEST
        response_message = APIErrorResponse(error="Search query is not valid.")
        return response_message
    result = await elasticsearch_study_search_service.search(query=q, raw=True)
    response: APIResponse[Any] = APIResponse[Any](content=result.body)
    return response


@router.get(
    "/mapping",
    summary="Get Elasticsearch mapping for the public study index.",
    description="Returns the mapping of the configured public study index.",
    response_model=APIResponse[Any],
)
@inject
async def get_study_index_mapping(
    elasticsearch_study_search_service=Depends(
        Provide["gateways.elasticsearch_study_gateway"]
    ),
):
    mapping = await elasticsearch_study_search_service.get_index_mapping()
    return APIResponse[Any](content=mapping)


@router.post(
    "/search/export",
    summary="Export study search results as JSON.",
    description=(
        "Streams all matching study documents (up to 10,000) as a JSON array. "
        "Uses the same query and filter logic as the search endpoint."
    ),
    responses={
        200: {
            "content": {"application/json": {}},
            "description": "JSON file containing all matching study documents.",
        }
    },
)
@inject
async def export_study_search(
    q: Annotated[StudySearchInput, Body()],
    elasticsearch_study_search_service=Depends(
        Provide["gateways.elasticsearch_study_gateway"]
    ),
):
    async def generate():
        metadata = q.model_dump(exclude_none=True)
        yield '{"metadata":' + json.dumps(metadata) + ',"results":['
        first = True
        async for item in elasticsearch_study_search_service.export_results(query=q):
            if not first:
                yield ","
            yield json.dumps(item)
            first = False
        yield "]}"

    return StreamingResponse(
        generate(),
        media_type="application/json",
        headers={"Content-Disposition": 'attachment; filename="study-export.json"'},
    )
