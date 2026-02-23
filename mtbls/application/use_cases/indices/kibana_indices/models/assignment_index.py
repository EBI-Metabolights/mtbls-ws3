from enum import Enum
from typing import Dict, List

from metabolights_utils.models.isa.common import (
    AssayTechnique,
    OrganismAndOrganismPartPair,
)

from mtbls.application.use_cases.indices.kibana_indices.models.common import (
    BaseIsaTableIndexItem,
    IndexValue,
)


class MatchCategory(str, Enum):
    QUANTITATIVE = "QUANTITATIVE"
    QUALITATIVE = "QUALITATIVE"


class AssignmentIndexItem(BaseIsaTableIndexItem):
    technique: AssayTechnique = AssayTechnique()
    samples: List[OrganismAndOrganismPartPair] = []
    sample_match_category: MatchCategory = MatchCategory.QUALITATIVE
    meta: Dict[str, List[IndexValue]] = {}


ASSIGNMENT_FLOAT_VALUE_FIELDS = {"retention_time", "mass_to_charge"}
ASSIGNMENT_INT_VALUE_FIELDS = {}

DERIVED_ASSAY_COLUMN_NAMES = {
    "Parameter Value[Solvent]",
    "Parameter Value[Sample pH]",
    "Parameter Value[Temperature]",
    "Parameter Value[NMR Probe]",
    "Parameter Value[Number of transients]",
    "Parameter Value[Pulse sequence name]",
    "Parameter Value[Scan polarity]",
    "Parameter Value[Instrument]",
    "Parameter Value[DI Instrument]",
    "Parameter Value[CE Instrument]",
    "Parameter Value[FIA Instrument]",
    "Parameter Value[SPE-IMS Instrument]",
    "Parameter Value[Ion source]",
    "Parameter Value[Mass analyzer]",
    "Parameter Value[Chromatography Instrument]",
    "Parameter Value[Column model]",
    "Parameter Value[Column type]",
    "Parameter Value[Cartridge type]",
    "Parameter Value[Matrix application]",
    "Parameter Value[Matrix]",
    "Parameter Value[Sample preservation]",
    "Parameter Value[Tissue modification]",
    "Parameter Value[Resolving power m/z]",
    "Parameter Value[Resolving power]",
    "Parameter Value[Pixel size x]",
    "Parameter Value[Pixel size y]",
    "Parameter Value[Column model 1]",
    "Parameter Value[Column model 2]",
    "Parameter Value[Column type 1]",
    "Parameter Value[Column type 2]",
}

DEFAULT_MAF_COLUMN_NAMES = {
    "database_identifier",
    "chemical_formula",
    "smiles",
    "inchi",
    "metabolite_identification",
    "chemical_shift",
    "multiplicity",
    "mass_to_charge",
    "fragmentation",
    "modifications",
    "charge",
    "retention_time",
    "taxid",
    "species",
    "database",
    "database_version",
    "reliability",
    "uri",
    "search_engine",
    "search_engine_score",
    "smallmolecule_abundance_sub",
    "smallmolecule_abundance_stdev_sub",
    "smallmolecule_abundance_std_error_sub",
}
