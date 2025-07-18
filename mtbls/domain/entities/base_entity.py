from typing import Annotated, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator

from mtbls.domain.enums.entity import Entity


class BaseEntity(BaseModel):
    __entity_type__: Union[None, Entity] = None

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
    __entity_type__: Entity = Entity.Study


class BaseStudy(BaseOutputEntity):
    __entity_type__: Entity = Entity.Study


class BaseUserInput(BaseInputEntity):
    __entity_type__: Entity = Entity.User


class BaseUser(BaseOutputEntity):
    __entity_type__: Entity = Entity.User


class BaseStudyFileInput(BaseInputEntity):
    __entity_type__: Entity = Entity.StudyFile


class BaseStudyFile(BaseOutputEntity):
    __entity_type__: Entity = Entity.StudyFile
