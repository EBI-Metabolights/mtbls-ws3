from typing import Union

from metabolights_utils.common import CamelCaseModel


class UpdateLog(CamelCaseModel):
    action: str
    source: str
    old_value: str
    new_value: str


class StudyMetadataModifierResult(CamelCaseModel):
    resource_id: str = ""
    bucket_name: Union[str, None] = None
    has_error: bool = False
    error_message: str = ""
    logs: list[UpdateLog] = []
