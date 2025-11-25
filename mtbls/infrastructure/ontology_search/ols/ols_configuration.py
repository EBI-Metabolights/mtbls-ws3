from pydantic import BaseModel


class OlsConfiguration(BaseModel):
    timeout_in_seconds: int = 10
    default_search_result_size: int = 20
    origin: str = "OLS"
    origin_url: str = "https://www.ebi.ac.uk/ols4"
    success_result_cache_timeout_in_seconds: int = 60 * 60 * 24 * 3
