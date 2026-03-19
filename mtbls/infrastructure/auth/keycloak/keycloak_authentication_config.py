from pydantic import BaseModel


class KeycloakAuthenticationConfiguration(BaseModel):
    host: str
    realm_name: str
    client_id: str
    client_secret: str
    admin_username: str
    admin_password: str
