import datetime
from typing import Union

from pydantic import ConfigDict, field_validator

from mtbls.domain.entities.base_entity import BaseStudyRevision
from mtbls.domain.enums.entity import Entity
from mtbls.domain.enums.mhd_share_status import MhdShareStatus
from mtbls.domain.enums.study_revision_status import StudyRevisionStatus


class StudyRevisionInput(BaseStudyRevision):
    model_config = ConfigDict(
        from_attributes=True, strict=True, entity_type=Entity.StudyRevision
    )
    accession_number: Union[None, str] = None
    revision_number: Union[None, int] = None
    revision_datetime: Union[None, datetime.datetime] = None
    revision_comment: Union[None, str] = None
    created_by: Union[None, str] = None
    status: Union[None, StudyRevisionStatus] = None
    task_started_at: Union[None, datetime.datetime] = None
    task_completed_at: Union[None, datetime.datetime] = None
    task_message: Union[None, str] = None
    mhd_share_status: Union[None, MhdShareStatus] = None

    @field_validator("status")
    @classmethod
    def status_validator(cls, value):
        if value is None:
            return None
        if isinstance(value, StudyRevisionStatus):
            return value
        if isinstance(value, int):
            return StudyRevisionStatus(value)
        return None

    @field_validator("status")
    @classmethod
    def mhd_share_status_validator(cls, value):
        if value is None:
            return None
        if isinstance(value, MhdShareStatus):
            return value
        if isinstance(value, int):
            return MhdShareStatus(value)
        return None


class StudyRevisionOutput(StudyRevisionInput):
    model_config = ConfigDict(
        from_attributes=True, strict=True, entity_type=Entity.StudyRevision
    )
    id_: Union[int, str]
