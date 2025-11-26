import abc
from io import BufferedWriter
from typing import Any

from mtbls.domain.entities.http_response import HttpResponse
from mtbls.domain.enums.http_request_type import HttpRequestType


class HttpClient(abc.ABC):
    @abc.abstractmethod
    async def send_request(
        self,
        method: HttpRequestType,
        url: str,
        headers: None | dict[str, str] = None,
        params: None | dict[str, str] = None,
        json: None | dict[str, Any] = None,
        timeout: None | int = None,
        follow_redirects: bool = False,
        raise_error_for_status: bool = True,
    ) -> HttpResponse: ...

    @abc.abstractmethod
    async def stream(
        self,
        buffered_writer: BufferedWriter,
        method: HttpRequestType,
        url: str,
        headers: None | dict[str, str] = None,
        params: None | dict[str, str] = None,
        json: None | dict[str, Any] = None,
        timeout: None | int = None,
        follow_redirects: bool = False,
        raise_error_for_status: bool = True,
    ) -> int: ...
