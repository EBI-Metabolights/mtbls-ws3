from pydantic import BaseModel


class JwtTokenInput(BaseModel):
    sub: str = 0  # Subject if the token
    scopes: list[str] = []  # custom scopes
    role: str = ""  # custom role


class JwtTokenContent(BaseModel):
    jti: str = ""  # JWT unique ID
    sub: str = 0  # Subject if the token
    exp: int = 0  # Expiration timestamp (epoch)
    iat: int = 0  # Issued at timestamp (epoch)
    nbf: int = 0  # Not before timestamp (epoch)
    aud: str = ""  # Audience
    iss: str = ""  # Issuer
    scopes: list[str] = []  # custom scopes
    role: str = ""  # custom role


class JwtToken(BaseModel):
    access_token: str = ""
    token_type: str = ""
    message: str = ""
