from logging import getLogger
from typing import Annotated

from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends, Form, Response, status

from mtbls.application.services.interfaces.auth.authentication_service import (
    AuthenticationService,
)
from mtbls.domain.enums.jwt_token_content import JwtToken
from mtbls.domain.enums.token_type import TokenType
from mtbls.domain.exceptions.auth import AuthenticationError
from mtbls.presentation.rest_api.core.responses import APIResponse, Status
from mtbls.presentation.rest_api.groups.auth.v1.routers import oauth2_scheme
from mtbls.presentation.rest_api.groups.auth.v1.routers.types import (
    OAuth2TokenRequestModel,
)

logger = getLogger(__name__)

router = APIRouter(tags=["Auth"], prefix="/auth/v1")


@router.post("/token", response_model=JwtToken, summary="Create JWT Token")
@inject
async def get_oauth2_token(
    response: Response,
    form_data: Annotated[
        OAuth2TokenRequestModel, Form(description="Authentication information.")
    ],
    authentication_service: AuthenticationService = Depends(  # noqa: FAST002
        Provide["services.authentication_service"]
    ),
):
    username = form_data.username
    password = form_data.password
    client_secret = form_data.client_secret
    access_token = None
    response.status_code = status.HTTP_401_UNAUTHORIZED
    if username and password:
        if client_secret:
            return JwtToken(
                message="If you want to authenticate with client secret, "
                "user name/password pair must be empty."
            )

        try:
            access_token = await authentication_service.authenticate_with_password(
                username, password
            )
            if access_token:
                logger.info("%s is authenticated with username and password.", username)
        except Exception as ex:
            logger.error("Authentication failed for user '%s'", username)
            logger.debug(str(ex))
            return JwtToken(message=f"Authentication failed for user '{username}'")
    elif client_secret:
        if username or password:
            return JwtToken(
                message="If you want to authenticate with client secret, "
                "user name/password pair must be empty."
            )
        try:
            access_token = await authentication_service.authenticate_with_token(
                token_type=TokenType.API_TOKEN, token=client_secret
            )
        except Exception as ex:
            logger.error(
                "Authentication failed with API token '%s'", client_secret[0] + "..."
            )
            logger.debug(str(ex))
            return JwtToken(
                message="Authentication failed with API token "
                + client_secret[0]
                + "..."
            )
        if access_token:
            logger.info("%s is authenticated with API token.", client_secret[0] + "...")

    if not access_token:
        logger.error("Authentication token is not valid.")
        return JwtToken(message="Invalid username or password / api token")

    response.status_code = status.HTTP_200_OK
    return JwtToken(access_token=access_token, token_type="bearer")


@router.delete("/token", summary="Revoke JWT Token", include_in_schema=False)
@inject
async def revoke_oauth2_token(
    jwt_token: Annotated[str, Depends(oauth2_scheme.oauth2_scheme)],
    authentication_service: AuthenticationService = Depends(  # noqa: FAST002
        Provide["services.authentication_service"]
    ),
):
    if not jwt_token:
        raise AuthenticationError("Invalid jwt token.")
    await authentication_service.revoke_jwt_token(jwt_token)
    return APIResponse(status=Status.SUCCESS, message="token revoked")
