import logging
from typing import Optional

from fastapi import Request
from fastapi.openapi.models import OAuthFlows as OAuthFlowsModel
from fastapi.security import OAuth2
from fastapi.security.utils import get_authorization_scheme_param

logger = logging.getLogger(__name__)


class OAuth2ClientCredentials(OAuth2):
    def __init__(
        self,
        tokenUrl: None | str = "",
        refreshUrl: None | str = None,
        scheme_name: str = None,
        scopes: dict = None,
        auto_error: bool = True,
        description: str = None,
    ):
        if not scopes:
            scopes = {}
        password = {"tokenUrl": tokenUrl, "scopes": scopes}
        if refreshUrl:
            password["refreshUrl"] = refreshUrl
        flows = OAuthFlowsModel(password=password)
        super().__init__(
            flows=flows,
            scheme_name=scheme_name,
            auto_error=auto_error,
            description=description,
        )

    async def __call__(self, request: Request) -> Optional[str]:
        authorization: str = request.headers.get("Authorization")
        _, param = get_authorization_scheme_param(authorization)
        return param


_initiated = False
oauth2_scheme = OAuth2ClientCredentials()


def get_oauth2_scheme(auth_service_url, realm_name) -> OAuth2ClientCredentials:
    global _initiated
    global oauth2_scheme
    if _initiated:
        return oauth2_scheme
    if auth_service_url and realm_name:
        token_url = f"{auth_service_url.rstrip('/')}/realms/{realm_name}/protocol/openid-connect/token"
        refresh_url = token_url
    else:
        logger.warning(
            "Authentication service URL or realm name is not defined. "
            "Local scheme will be used."
        )
        token_url = "auth/v1/token"
        refresh_url = "auth/v1/refresh"
    oauth2_scheme = OAuth2ClientCredentials(
        tokenUrl=token_url,
        refreshUrl=refresh_url,
        description="Please login to use web services that require authorization.",
    )

    _initiated = True
    return oauth2_scheme
