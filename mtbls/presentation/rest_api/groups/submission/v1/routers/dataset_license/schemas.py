from typing import Annotated, Optional

from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel


class DatasetLicenseInfoConfiguration(BaseModel):
    name: str = ""
    version: str = ""


class DatasetLicense(BaseModel):
    """Base model class to convert python attributes to camel case"""

    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
        json_schema_serialization_defaults_required=True,
    )

    name: str
    version: str
    agreed: bool
    agreeing_user: str
    license_url: str


LICENSE_URLS = {
    ("CC0 1.0 UNIVERSAL", "1.0"): "https://creativecommons.org/publicdomain/zero/1.0/",
    (
        "EMBL-EBI TERMS OF USE",
        "5TH FEBRUARY 2024",
    ): "https://www.ebi.ac.uk/about/terms-of-use/",
}


class DatasetLicenseResponse(BaseModel):
    dataset: Annotated[
        Optional[DatasetLicense],
        Field(
            default=None,
            description="The name and version of the dataset license, and the agreeing user.",
        ),
    ]
    message: Annotated[
        str,
        Field(default="", description="Message related to the task."),
    ]
