from pydantic import BaseModel

from mtbls.domain.shared.mhd_configuration import MhdConfiguration
from mtbls.domain.shared.validator.types import ValidationPhase

ALL_VALIDATION_PHASES = [
    ValidationPhase.PHASE_1,
    ValidationPhase.PHASE_2,
    ValidationPhase.PHASE_3,
    ValidationPhase.PHASE_4,
]


class DbConfiguration(BaseModel):
    database: str
    user: str
    password: str
    host: str
    port: int = 5432


class ValidationRunConfiguration(BaseModel):
    apply_modifiers: bool = True
    skip_result_file_modification: bool = False
    validation_phases: list[ValidationPhase] | None = ALL_VALIDATION_PHASES
    mhd_configuration: MhdConfiguration = MhdConfiguration()
    metadata_files_root_path: None | str = None
    db_connection: None | DbConfiguration = None
    assignmet_sheet_limit: None | int = None
