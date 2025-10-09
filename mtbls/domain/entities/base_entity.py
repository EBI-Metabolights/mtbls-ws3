from typing import Annotated, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator

from mtbls.domain.enums.entity import Entity


class BaseEntity(BaseModel):
    model_config = ConfigDict(from_attributes=True, strict=True)


class BaseInputEntity(BaseEntity): ...


class BaseOutputEntity(BaseEntity):
    id_: Annotated[Union[int, str], Field(alias="_id")] = ""

    @field_validator("id_", mode="before")
    def id_validator(val):
        if isinstance(val, int):
            return val
        return str(val) if val else ""


class BaseStudyInput(BaseInputEntity):
    model_config = ConfigDict(
        from_attributes=True, strict=True, entity_type=Entity.Study
    )


class BaseStudy(BaseOutputEntity):
    model_config = ConfigDict(
        from_attributes=True, strict=True, entity_type=Entity.Study
    )


class BaseUserInput(BaseInputEntity):
    model_config = ConfigDict(
        from_attributes=True, strict=True, entity_type=Entity.User
    )


class BaseUser(BaseOutputEntity):
    model_config = ConfigDict(
        from_attributes=True, strict=True, entity_type=Entity.User
    )


class BaseStatisticInput(BaseInputEntity):
    model_config = ConfigDict(
        from_attributes=True, strict=True, entity_type=Entity.Statistic
    )


class BaseStatistic(BaseOutputEntity):
    model_config = ConfigDict(
        from_attributes=True, strict=True, entity_type=Entity.Statistic
    )


class BaseStudyFileInput(BaseInputEntity):
    model_config = ConfigDict(
        from_attributes=True, strict=True, entity_type=Entity.StudyFile
    )


class BaseStudyFile(BaseOutputEntity):
    model_config = ConfigDict(
        from_attributes=True, strict=True, entity_type=Entity.StudyFile
    )


class BaseStudyRevisionInput(BaseInputEntity):
    model_config = ConfigDict(
        from_attributes=True, strict=True, entity_type=Entity.StudyRevision
    )


class BaseStudyRevision(BaseOutputEntity):
    model_config = ConfigDict(
        from_attributes=True, strict=True, entity_type=Entity.StudyRevision
    )
