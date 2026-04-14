from enum import Enum
from typing import Annotated

from pydantic import BaseModel, Field


class Polarity(str, Enum):
    NOT_DEFINED = "NOT_DEFINED"
    POSITIVE = "POSITIVE"
    NEGATIVE = "NEGATIVE"
    ALTERNATING = "ALTERNATING"

    @staticmethod
    def match(data: str):
        if not data:
            return Polarity.NOT_DEFINED

        if data.lower().startswith("pos"):
            return Polarity.POSITIVE
        elif data.lower().startswith("neg"):
            return Polarity.NEGATIVE
        elif data.lower().startswith("alt"):
            return Polarity.ALTERNATING

        return Polarity.NOT_DEFINED


class ColumnType(str, Enum):
    NOT_DEFINED = "NOT_DEFINED"
    HILIC = "HILIC"
    REVERSE_PHASE = "REVERSE_PHASE"

    @staticmethod
    def match(data: str):
        if not data:
            return ColumnType.NOT_DEFINED

        if data.lower().startswith("hilic"):
            return ColumnType.HILIC
        elif data.lower().startswith("re"):
            return ColumnType.REVERSE_PHASE

        return ColumnType.NOT_DEFINED


class StudyLookup(BaseModel):
    value: Annotated[str, Field("")] = ""
    name: Annotated[str, Field("")] = ""
    title: Annotated[str, Field("")] = ""
    study_no: Annotated[int, Field(0)] = 0
    technique: Annotated[str, Field("")] = ""
    url: Annotated[str, Field("")] = ""


class StudyMsAssayLookup(BaseModel):
    value: Annotated[str, Field("")] = ""
    name: Annotated[str, Field("")] = ""
    study_id: Annotated[str, Field("")] = ""
    polarity: Annotated[str, Field("")] = ""
    organism_part: Annotated[str, Field("")] = ""
    column_type: Annotated[str, Field("")] = ""
    column_model: Annotated[str, Field("")] = ""

    param_polarity: Annotated[str, Field("")] = ""
    param_center: Annotated[int, Field("")] = 1
    param_min_max_peak_width: Annotated[str, Field("")] = ""
    parameters_merged: Annotated[str, Field("")] = ""
    text: Annotated[str, Field("")] = ""
    url: Annotated[str, Field("")] = ""


class StudyMsAssayFileLookup(BaseModel):
    value: Annotated[str, Field("")] = ""
    assay_id: Annotated[str, Field("")] = ""
    study_id: Annotated[str, Field("")] = ""
    file_format: Annotated[str, Field("")] = ""
    ms_level: Annotated[str, Field("")] = ""
    peak_peaking: Annotated[str, Field("")] = ""
    column_type: Annotated[str, Field("")] = ""
    scan_polarity: Annotated[str, Field("")] = ""
    organism: Annotated[str, Field("")] = ""
    organism_part: Annotated[str, Field("")] = ""
    description: Annotated[str, Field("")] = ""
    name: Annotated[str, Field("")] = ""
    is_dir: Annotated[bool, Field(False)] = False
    url: Annotated[str, Field("")] = ""
