from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from metabolights_utils.models.metabolights.model import CurationRequest, StudyStatus
from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel


class BaseEsIndex(BaseModel):
    model_config = ConfigDict(
        alias_generator=to_camel, populate_by_name=True, use_enum_values=False
    )


class IndexValue(BaseEsIndex):
    value: Union[str, int, float, None] = ""

    def __hash__(self):
        if isinstance(self.value, str) and not self.value:
            return hash("")
        return hash(self.value)


class IndexValueList(BaseEsIndex):
    header_name: str = ""
    data: List[IndexValue] = []


class Country(BaseEsIndex):
    code: str = ""
    name: str = ""


class BaseIndexItem(BaseEsIndex):
    status: StudyStatus = StudyStatus.DORMANT
    curation_request: CurationRequest = CurationRequest.NO_CURATION
    indexed_datetime: Union[None, str, datetime] = None
    public_release_date: Union[None, str, datetime] = None
    last_update_datetime: Union[None, str, datetime] = None
    identifier: str = ""
    object_type: str = ""


class BaseIsaTableIndexItem(BaseIndexItem):
    study_id: str = ""
    numeric_study_id: int = 0
    fields: Dict[str, Union[List[IndexValue], List[Any]]] = {}
    additional_fields: List[IndexValueList] = []
    invalid_values: Dict[str, Union[List[IndexValue], List[Any]]] = {}
    row_index: int = -1
    file_name: str = ""
    file_id: str = ""
    country: Country = Country()


class BaseIndexValue(BaseEsIndex):
    unit: Optional[str] = None
    term_source_ref: Optional[str] = None
    term_accession_number: Optional[str] = None


class IndexOntologyValue(IndexValue):
    term_source_ref: Optional[str] = None
    term_accession_number: Optional[str] = None


class IndexUnitOntologyValue(IndexValue):
    unit: Optional[str] = None
    term_source_ref: Optional[str] = None
    term_accession_number: Optional[str] = None


class IndexPrimitiveValueList(BaseEsIndex):
    header_name: str = ""
    data: List = []


class ProtocolFields(BaseEsIndex):
    name: str = ""
    fields: Dict[str, IndexValueList] = {}
    additional_fields: List[IndexValueList] = []


PROTOCOL_NAMES = {
    "sampleCollection",
    "extraction",
    "chromatography",
    "massSpectrometry",
    "metaboliteIdentification",
    "dataTransformation",
    "nmrAssay",
    "nmrSample",
    "nmrSpectroscopy",
    "preparation",
    "histology",
    "magneticResonanceImaging",
    "inVivoMagneticResonanceSpectroscopy",
    "inVivoMagneticResonanceAssay",
    "directInfusion",
    "flowInjectionAnalysis",
    "capillaryElectrophoresis",
    "solidPhaseExtractionIonMobilitySpectrometry",
}
