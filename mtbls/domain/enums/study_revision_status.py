import enum


class StudyRevisionStatus(enum.IntEnum):
    INITIATED = 0
    IN_PROGRESS = 1
    FAILED = 2
    SUCCESS = 3
