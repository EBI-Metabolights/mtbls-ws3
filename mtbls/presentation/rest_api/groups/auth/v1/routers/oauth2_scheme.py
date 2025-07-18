from functools import lru_cache
from typing import Optional

from fastapi import HTTPException, Request, status
from fastapi.openapi.models import OAuthFlows as OAuthFlowsModel
from fastapi.security import OAuth2
from fastapi.security.utils import get_authorization_scheme_param


class OAuth2ClientCredentials(OAuth2):
    def __init__(
        self,
        tokenUrl: str,
        scheme_name: str = None,
        scopes: dict = None,
        auto_error: bool = True,
        description: str = None,
    ):
        if not scopes:
            scopes = {}
        flows = OAuthFlowsModel(password={"tokenUrl": tokenUrl, "scopes": scopes})
        super().__init__(
            flows=flows,
            scheme_name=scheme_name,
            auto_error=auto_error,
            description=description,
        )

    async def __call__(self, request: Request) -> Optional[str]:
        authorization: str = request.headers.get("Authorization")
        scheme, param = get_authorization_scheme_param(authorization)
        if not authorization or scheme.lower() != "bearer":
            if self.auto_error:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Authentication is required",
                    headers={"WWW-Authenticate": "Bearer"},
                )
            return None
        return param


class OptionalOAuth2ClientCredentials(OAuth2ClientCredentials):
    def __init__(
        self,
        tokenUrl: str,
        scheme_name: str = None,
        scopes: dict = None,
        auto_error: bool = True,
        description: str = None,
    ):
        super().__init__(
            tokenUrl=tokenUrl,
            scheme_name=scheme_name,
            scopes=scopes,
            auto_error=auto_error,
            description=description,
        )

    async def __call__(self, request: Request) -> Optional[str]:
        authorization: str = request.headers.get("Authorization")
        scheme, param = get_authorization_scheme_param(authorization)
        return param


oauth2_scheme = OAuth2ClientCredentials(
    tokenUrl="auth/v1/token",
    description="Please login to use web services that require authorization.",
)

optional_oauth2_scheme = OptionalOAuth2ClientCredentials(
    tokenUrl="auth/v1/token",
    description="Please login to use web services that require authorization.",
)


@lru_cache(maxsize=1)
def get_oauth2_scheme() -> OAuth2ClientCredentials:
    return OAuth2ClientCredentials(
        tokenUrl="auth/v1/token",
        description="Please login to use web services that require authorization.",
    )
