from typing import Annotated, Union

from fastapi import Form
from pydantic import BaseModel


class OAuth2TokenRequestModel(BaseModel):
    grant_type: Annotated[
        Union[None, str], Form(default="password", pattern="password")
    ] = "password"
    username: Union[None, str] = None
    password: Union[None, str] = None
    scope: Union[None, str] = ""
    client_id: Union[None, str] = None
    client_secret: Union[None, str] = None


class AuthenticationMessage(BaseModel):
    authenticated: bool = False
    message: str = ""
