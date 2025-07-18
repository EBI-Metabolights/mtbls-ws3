from pydantic import BaseModel


class StandaloneAuthenticationConfiguration(BaseModel):
    application_secret_key: str
    access_token_hash_algorithm: str = "HS256"
    access_token_expires_delta_in_minutes: int = 24 * 60
    revocation_management_enabled: bool = True
    revoked_access_token_prefix: str = "revoked_jwt_token"
