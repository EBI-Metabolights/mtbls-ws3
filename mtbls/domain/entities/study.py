import datetime
from typing import Union

from pydantic import ConfigDict

from mtbls.domain.entities.base_entity import BaseStudy
from mtbls.domain.entities.study_revision import StudyRevisionOutput
from mtbls.domain.entities.user import UserOutput
from mtbls.domain.enums.curation_type import CurationType
from mtbls.domain.enums.entity import Entity
from mtbls.domain.enums.study_category import StudyCategory
from mtbls.domain.enums.study_status import StudyStatus
from mtbls.domain.shared.data_types import Integer


class StudyInput(BaseStudy):
    model_config = ConfigDict(
        from_attributes=True, strict=True, entity_type=Entity.Study
    )
    accession_number: Union[None, str] = None
    obfuscation_code: Union[None, str] = None
    release_date: Union[None, datetime.datetime] = None
    submission_date: Union[None, datetime.datetime] = None
    update_date: Union[None, datetime.datetime] = None
    status_date: Union[None, datetime.datetime] = None
    study_size: Union[None, Integer] = None
    status: StudyStatus = StudyStatus.DORMANT
    study_type: Union[None, str] = None
    curation_type: CurationType = CurationType.NO_CURATION
    dataset_license: Union[None, str] = None
    dataset_license_version: Union[None, str] = None
    dataset_license_agreeing_user: Union[None, str] = None
    reserved_accession: Union[None, str] = None
    reserved_submission_id: Union[None, str] = None
    first_public_date: Union[None, datetime.datetime] = None
    first_private_date: Union[None, datetime.datetime] = None
    revision_number: int = 0
    revision_datetime: Union[None, datetime.datetime] = None
    sample_type: Union[None, str] = "minimum"
    template_version: str = "1.0"
    data_policy_agreement: int = 0
    mhd_accession: Union[None, str] = None
    mhd_model_version: Union[None, str] = None
    study_category: StudyCategory = StudyCategory.OTHER


class StudyOutput(StudyInput):
    model_config = ConfigDict(
        from_attributes=True, strict=True, entity_type=Entity.Study
    )
    id_: Union[int, str]
    revisions: None | list[StudyRevisionOutput] = []
    submitters: None | list[UserOutput] = []
