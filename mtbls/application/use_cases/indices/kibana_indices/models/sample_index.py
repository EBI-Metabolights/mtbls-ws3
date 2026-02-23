from typing import Any, List

from mtbls.application.use_cases.indices.kibana_indices.models.common import (
    BaseEsIndex,
    BaseIsaTableIndexItem,
)


class FactorDescription(BaseEsIndex):
    name: str = ""
    term: str = ""
    term_source_ref: str = ""
    term_accession_number: str = ""


class SampleIndexItem(BaseIsaTableIndexItem):
    factors: List[FactorDescription] = []
    factor_header_names: List[str] = []
    factor_terms: List[str] = []


class FactorIndexItem(BaseEsIndex):
    data: List[Any] = []
    desc: FactorDescription = FactorDescription()


DEFAULT_SAMPLE_COLUMN_NAMES = {
    "Characteristics[Organism part]",
    "Characteristics[Organism]",
    "Characteristics[Variant]",
    "Characteristics[Sample type]",
    "Characteristics[Disease]",
    "Characteristics[CellType]",
    "Sample Name",
    "Source Name",
}
