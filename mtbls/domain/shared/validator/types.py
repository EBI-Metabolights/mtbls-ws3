from enum import IntEnum, StrEnum


class PhaseLevel(IntEnum):
    PHASE_1 = 1
    PHASE_2 = 2
    PHASE_3 = 3
    PHASE_4 = 4


class ValidationPhase(StrEnum):
    PHASE_1 = "PHASE_1"
    PHASE_2 = "PHASE_2"
    PHASE_3 = "PHASE_3"
    PHASE_4 = "PHASE_4"

    @staticmethod
    def get_phase(phase: int):
        if phase == PhaseLevel.PHASE_1:
            return ValidationPhase.PHASE_1
        if phase == PhaseLevel.PHASE_2:
            return ValidationPhase.PHASE_2
        if phase == PhaseLevel.PHASE_3:
            return ValidationPhase.PHASE_3
        if phase == PhaseLevel.PHASE_4:
            return ValidationPhase.PHASE_4
        return None


class PolicyMessageType(StrEnum):
    ERROR = "ERROR"
    WARNING = "WARNING"
    INFO = "INFO"
    SUCCESS = "SUCCESS"

    def get_level(self) -> int:
        if self.value == PolicyMessageType.SUCCESS:
            return 10
        if self.value == PolicyMessageType.INFO:
            return 20
        if self.value == PolicyMessageType.WARNING:
            return 30
        return 40
