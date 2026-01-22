from pydantic import BaseModel


class MhdConfiguration(BaseModel):
    public_study_base_url: str = (
        "http://ftp.ebi.ac.uk/pub/databases/metabolights/studies/public"
    )
    public_ftp_base_url: str = (
        "ftp://ftp.ebi.ac.uk/pub/databases/metabolights/studies/public"
    )
    study_http_base_url: str = "https://www.ebi.ac.uk/metabolights"
    mhd_webservice_base_url: str = "https://www.metabolomicshub.org/api/submission/v0_1"
    api_key: str = ""
