from typing import Any

from pydantic import BaseModel


class RestApiEndpointConfiguration(BaseModel):
    path: str
    method: Any
    http_method: str
    summary: str
    description: str = ""
    response_model: Any
