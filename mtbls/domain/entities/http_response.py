from typing import Any

from pydantic import BaseModel


class HttpResponse(BaseModel):
    status_code: int
    headers: None | dict[str, Any] = None
    json_data: None | dict[str, Any] = None
    error: bool = (False,)
    error_message: None | str = None
