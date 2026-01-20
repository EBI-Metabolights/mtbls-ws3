from typing import Annotated, Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


class BaseSearchResult(BaseModel):
    result: Annotated[
        Any, Field(description="Search result (override in subclass)")
    ]  # type intended to be overriden by implementing classes.
    message: Annotated[
        str, Field(description="Message indicating outcome of the search query")
    ] = ""


class SortModel(BaseModel):
    field: Annotated[str, Field(description="Field name to sort on")]
    direction: Annotated[Literal["asc", "desc"], Field(description="Sort direction")]


class PageModel(BaseModel):
    current: Annotated[int, Field(ge=1, description="Current page number (1-indexed)")]
    size: Annotated[int, Field(gt=0, description="Number of results per page")]


class FilterModel(BaseModel):
    field: Annotated[str, Field(description="Field being filtered")]
    values: Annotated[
        List[Any], Field(default_factory=list, description="List of selected values")
    ]
    operator: Annotated[
        Optional[Literal["all", "any", "none"]],
        Field(default="all", description="Filter operator (all/any/none)"),
    ]


class BaseSearchInput(BaseModel):
    query: Annotated[
        Optional[str], Field(default=None, description="Search term or query string")
    ]


class IndexSearchInput(BaseSearchInput):
    """Base class for index search inputs with common fields."""

    query: Annotated[
        Optional[str], Field(default=None, description="Search term or query string")
    ]
    page: Annotated[PageModel, Field(description="Pagination parameters")]
    sort: Annotated[
        Optional[SortModel], Field(default=None, description="Sort configuration")
    ]
    filters: Annotated[
        Optional[List[FilterModel]],
        Field(
            default_factory=list,
            description="Applied filters (derived from selected facets)",
        ),
    ]
    facets: Annotated[
        Optional[Dict[str, Any]],
        Field(
            default=None,
            description="Facet configuration from the UI (value/range specs)",
        ),
    ]


class CompoundSearchInput(IndexSearchInput):
    """Search input for compound search with compound-specific filters."""

    study_ids: Annotated[
        Optional[List[str]],
        Field(
            default=None,
            description="Filter compounds by study IDs (e.g., ['MTBLS1', 'MTBLS2']). Returns compounds that appear in the specified studies.",
        ),
    ]
    study_ids_operator: Annotated[
        Optional[Literal["any", "all"]],
        Field(
            default="any",
            description="Operator for study_ids filter: 'any' returns compounds in ANY of the studies, 'all' returns compounds in ALL of the studies.",
        ),
    ]


class StudySearchInput(IndexSearchInput):
    """Search input for study search with study-specific filters."""

    database_identifiers: Annotated[
        Optional[List[str]],
        Field(
            default=None,
            description="Filter studies by compound database identifiers (e.g., ['HMDB0031111']). Returns studies containing these compounds.",
        ),
    ]
    metabolite_identifications: Annotated[
        Optional[List[str]],
        Field(
            default=None,
            description="Filter studies by compound names (e.g., ['Lithocholic acid 3-O-glucuronide']). Returns studies containing these compounds.",
        ),
    ]
    study_ids: Annotated[
        Optional[List[str]],
        Field(
            default=None,
            description="Filter to specific study IDs (e.g., ['MTBLS1', 'MTBLS2']). Primarily used internally after resolving chemical filters.",
        ),
    ]


class FacetBucket(BaseModel):
    value: Any
    count: int


class FacetResponse(BaseModel):
    # Search UI wants: { <facetName>: { data: [{value, count}, ...] } }
    type: Literal["value", "range"]
    data: List[FacetBucket]


class IndexSearchResult(BaseModel):
    results: List[Dict[str, Any]]
    totalResults: int
    facets: Dict[str, List[FacetResponse]] = Field(default_factory=dict)
    requestId: str
    all_study_ids: Annotated[
        Optional[List[str]],
        Field(
            default=None,
            description="All matching study IDs (only populated when include_all_ids=true and search has query/filters)",
        ),
    ]


class IndexSearchResultEnvelope(BaseSearchResult):
    result: Annotated[IndexSearchResult, Field(description="Index search result")] = {}
    message: Annotated[
        str, Field(description="Message indicating outcome of the search query")
    ] = ""
