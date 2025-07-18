import json

from pydantic import field_serializer, field_validator

from mtbls.domain.entities.study_file import (
    HashAlgorithm,
    StudyFileInput,
    StudyFileOutput,
)


class SqlDbStudyFileInput(StudyFileInput):
    @field_serializer("hashes", "tags")
    @classmethod
    def hashes_serializer(cls, value):
        return json.dumps(value)

    @field_validator("hashes", mode="before")
    @classmethod
    def hashes_validator(cls, value):
        if isinstance(value, str):
            json_data = json.loads(value)
            return {HashAlgorithm(x): json_data[x] for x in json_data}

        if isinstance(value, dict):
            return {HashAlgorithm(x): value[x] for x in value}
        return {}

    @field_validator("is_directory", "is_link", mode="before")
    @classmethod
    def bool_validator(cls, value):
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return True if value == "1" else False
        if isinstance(value, int):
            return True if value > 0 else False
        return False

    @field_validator("tags", mode="before")
    @classmethod
    def tags_validator(cls, value):
        if isinstance(value, str):
            json_data = json.loads(value)
            return {x: json_data[x] for x in json_data}

        if isinstance(value, dict):
            return {x: value[x] for x in value}
        return {}


class SqlDbStudyFileOutput(SqlDbStudyFileInput, StudyFileOutput): ...
