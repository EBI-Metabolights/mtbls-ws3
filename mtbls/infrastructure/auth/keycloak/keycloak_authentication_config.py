from pydantic import BaseModel


class KeycloakAuthenticationConfiguration(BaseModel):
    host: str
    realm_name: str
    client_id: str
    client_secret: str
