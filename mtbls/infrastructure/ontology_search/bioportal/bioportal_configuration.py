from pydantic import BaseModel


class BioPortalConfiguration(BaseModel):
    timeout_in_seconds: int = 10
    default_search_result_size: int = 20
    origin: str = "BioPortal"
    origin_url: str = "https://data.bioontology.org"
    api_token: str = ""
