from typing import Annotated

from pydantic import BaseModel, Field


class StudyTitle(BaseModel):
    accession: Annotated[
        str, Field(default="", description="MetaboLights Study accession number")
    ]
    title: Annotated[
        str, Field(default="", description="Title of the MetaboLights Study")
    ]
