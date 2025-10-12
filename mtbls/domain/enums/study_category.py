import enum
from typing import Union


class StudyCategory(enum.IntEnum):
    OTHER = 0
    MS_MHD_ENABLED = 1
    MS_IMAGING = 2
    MS_OTHER = 3
    NMR = 4
    MS_MHD_LEGACY = 5

    @staticmethod
    def from_name(name: str) -> Union[None, "StudyCategory"]:
        if not name:
            return None
        name = name.strip().upper().replace(" ", "_")
        return StudyCategory.__members__.get(name, None)
