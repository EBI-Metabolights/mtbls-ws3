import enum


class UserRole(enum.IntEnum):
    SUBMITTER = 0
    CURATOR = 1
    ANONYMOUS = 2
    REVIEWER = 3
    SYSTEM_ADMIN = 4
