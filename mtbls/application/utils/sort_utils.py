import pathlib


def sort_by_study_id(key: str):
    if not key:
        return -1
    if key:
        val = pathlib.Path(key).name.upper().replace("MTBLS", "").replace("REQ", "")
        if val.isnumeric():
            return int(val)
    return -1
