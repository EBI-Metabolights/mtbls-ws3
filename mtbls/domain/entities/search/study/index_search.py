from typing import Annotated
from pydantic import BaseModel, Field
from typing import Any, Dict, List, Literal, Optional

class BaseSearchResult(BaseModel):
    result: Annotated[Any, Field(description="Search result (override in subclass)")] # type intended to be overriden by implementing classes.
    message: Annotated[str, Field(description="Message indicating outcome of the search query")] = ""



class SortModel(BaseModel):
    field: Annotated[str, Field(description="Field name to sort on")]
    direction: Annotated[Literal["asc", "desc"], Field(description="Sort direction")]


class PageModel(BaseModel):
    current: Annotated[int, Field(ge=1, description="Current page number (1-indexed)")]
    size: Annotated[int, Field(gt=0, description="Number of results per page")]


class FilterModel(BaseModel):
    field: Annotated[str, Field(description="Field being filtered")]
    values: Annotated[List[str], Field(default_factory=list, description="List of selected values")]
    operator: Annotated[
        Optional[Literal["all", "any", "none"]],
        Field(default="all", description="Filter operator (all/any/none)")
    ]


class BaseSearchInput(BaseModel):
    query: Annotated[
        Optional[str],
        Field(default=None, description="Search term or query string")
    ]
    

class IndexSearchInput(BaseSearchInput):
    query: Annotated[
        Optional[str],
        Field(default=None, description="Search term or query string")
    ]
    page: Annotated[PageModel, Field(description="Pagination parameters")]
    sort: Annotated[
        Optional[SortModel],
        Field(default=None, description="Sort configuration")
    ]
    filters: Annotated[
        Optional[List[FilterModel]],
        Field(default_factory=list, description="Applied filters (derived from selected facets)")
    ]
    facets: Annotated[
        Optional[Dict[str, Any]],
        Field(default=None, description="Facet configuration from the UI (value/range specs)")
    ]

class FacetBucket(BaseModel):
    value: Any
    count: int


class FacetResponse(BaseModel):
    # Search UI wants: { <facetName>: { data: [{value, count}, ...] } }
    data: List[FacetBucket]


class IndexSearchResult(BaseModel):
    results: List[Dict[str, Any]]
    totalResults: int
    facets: Dict[str, FacetResponse] = Field(default_factory=dict)
    requestId: str

class IndexSearchResultEnvelope(BaseSearchResult):
    result: Annotated[IndexSearchResult, Field(description="Index search result")] = {}
    message: Annotated[str, Field(description="Message indicating outcome of the search query")] = ""
