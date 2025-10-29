from pydantic import BaseModel


class OlsConfiguration(BaseModel):
    timeout_in_seconds: int = 10
    default_search_result_size: int = 20
    origin: str = "OLS"
    origin_url: str = "https://www.ebi.ac.uk/ols4"
