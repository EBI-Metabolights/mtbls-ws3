


import json
from typing import Annotated, Any

from fastapi import APIRouter, Body, Response, status
from fastapi.params import Depends
from fastapi.responses import StreamingResponse

from dependency_injector.wiring import Provide, inject

from mtbls.domain.entities.search.index_search import CompoundSearchInput, IndexSearchResult
from mtbls.presentation.rest_api.core.responses import APIErrorResponse, APIResponse


router = APIRouter(tags=["Public"], prefix="/public/v2/public-compound-index")

@router.post(
    "/search",
    summary="MetaboLights Compound Search from public compound index.",
    description="MetaboLights search results from public compound index.",
    response_model=APIResponse[IndexSearchResult],  # IndexSearchResult or raw dict
)
@inject
async def search_compound_index(
    response: Response,
    q: Annotated[CompoundSearchInput, Body()],
    elasticsearch_compound_search_service=Depends(
        Provide["gateways.elasticsearch_compound_gateway"]
    ),
):
    if not q:
        response.status_code = status.HTTP_400_BAD_REQUEST
        response_message = APIErrorResponse(error="Search query is not valid.")
        return response_message
    result = await elasticsearch_compound_search_service.search(query=q)
    response: APIResponse[IndexSearchResult] = APIResponse[IndexSearchResult](
        content=result
    )
    return response


@router.get(
    "/mapping",
    summary="Get the Elasticsearch mapping for the public compound index.",
    description="Retrieve the Elasticsearch mapping for the configured public compound index.",
    response_model=APIResponse[dict[str, Any]], 
)
@inject
async def get_compound_index_mapping(
    elasticsearch_compound_search_service=Depends(
        Provide["gateways.elasticsearch_compound_gateway"]
    ),
):
    mapping = await elasticsearch_compound_search_service.get_index_mapping()
    response: APIResponse[dict[str, Any]] = APIResponse[dict[str, Any]](
        content=mapping
    )
    return response


@router.post(
    "/search/export",
    summary="Export compound search results as JSON.",
    description=(
        "Streams all matching compound documents (up to 10,000) as a JSON array. "
        "Uses the same query and filter logic as the search endpoint."
    ),
    responses={
        200: {
            "content": {"application/json": {}},
            "description": "JSON file containing all matching compound documents.",
        }
    },
)
@inject
async def export_compound_search(
    q: Annotated[CompoundSearchInput, Body()],
    elasticsearch_compound_search_service=Depends(
        Provide["gateways.elasticsearch_compound_gateway"]
    ),
):
    async def generate():
        metadata = q.model_dump(exclude_none=True)
        yield '{"metadata":' + json.dumps(metadata) + ',"results":['
        first = True
        async for item in elasticsearch_compound_search_service.export_results(query=q):
            if not first:
                yield ","
            yield json.dumps(item)
            first = False
        yield "]}"

    return StreamingResponse(
        generate(),
        media_type="application/json",
        headers={"Content-Disposition": 'attachment; filename="compound-export.json"'},
    )