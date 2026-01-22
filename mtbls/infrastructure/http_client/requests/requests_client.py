import json as json_utils
import logging
from io import BufferedWriter
from typing import Any

import requests

from mtbls.application.services.interfaces.http_client import HttpClient
from mtbls.domain.entities.http_response import HttpResponse
from mtbls.domain.enums.http_request_type import HttpRequestType

logger = logging.getLogger(__name__)


class RequestsClient(HttpClient):
    def __init__(self, max_timeount_in_seconds: int = 60):
        super().__init__()
        self.max_timeount_in_seconds = max_timeount_in_seconds

    async def send_request(
        self,
        method: HttpRequestType,
        url: str,
        headers: dict[str, str] = None,
        params: dict[str, str] = None,
        json: None | dict[str, Any] = None,
        timeout: None | int = None,
        follow_redirects: bool = False,
        raise_error_for_status: bool = True,
    ) -> HttpResponse:
        timeout = (
            timeout
            if timeout is not None and timeout > 0
            else self.max_timeount_in_seconds
        )
        response: requests.Response = requests.request(
            method.value,
            url,
            params=params,
            headers=headers,
            follow_redirects=follow_redirects,
            timeout=timeout,
            json=json,
        )
        if response.status_code == 404:
            return HttpResponse(
                status_code=response.status_code,
                headers=dict(response.headers),
                json_data={},
            )
        if raise_error_for_status:
            response.raise_for_status()
        try:
            return HttpResponse(
                status_code=response.status_code,
                headers=dict(response.headers),
                json_data=json_utils.loads(response.text),
            )
        except Exception:
            return HttpResponse(
                status_code=response.status_code, headers=dict(response.headers)
            )

    async def stream(
        self,
        buffered_writer: BufferedWriter,
        method: HttpRequestType,
        url: str,
        headers: dict[str, str] = None,
        params: dict[str, str] = None,
        json: None | dict[str, Any] = None,
        timeout: None | int = None,
        follow_redirects: bool = False,
        raise_error_for_status: bool = True,
    ) -> HttpResponse:
        timeout = (
            timeout
            if timeout is not None and timeout > 0
            else self.max_timeount_in_seconds
        )
        with requests.request(
            method=method.value,
            url=url,
            params=params,
            headers=headers,
            json=json,
            timeout=timeout,
            follow_redirects=follow_redirects,
            stream=True,
        ) as stream:
            response: requests.Response = stream
            if raise_error_for_status:
                response.raise_for_status()

            for chunk in response.iter_bytes():
                buffered_writer.write(chunk)
            return HttpResponse(
                status_code=response.status_code, headers=dict(response.headers)
            )
