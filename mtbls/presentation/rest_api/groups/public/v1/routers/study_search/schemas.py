from typing import Annotated, Any

from pydantic import BaseModel, Field


class IndexSearchResult(BaseModel):
    result: Annotated[dict[str, Any], Field(description="Index search result")] = {}
    message: Annotated[str, Field(description="search message")] = ""


class IndexSearchInput(BaseModel):
    query: Annotated[dict[str, Any], Field(description="Index search query")] = {}
