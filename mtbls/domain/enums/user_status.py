import enum


class UserStatus(enum.IntEnum):
    NEW = 0
    VERIFIED = 1
    ACTIVE = 2
    FROZEN = 3
