import enum


class CurationStatus(enum.IntEnum):
    NOT_CURATED = 0
    PENDING = 1
    IN_CURATION = 2
    CURATED = 3
    OBSOLETE = 4
