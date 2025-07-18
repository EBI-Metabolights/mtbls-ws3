import enum


class CurationType(enum.IntEnum):
    MANUAL_CURATION = 0
    NO_CURATION = 1
    SEMI_AUTOMATED_CURATION = 2
