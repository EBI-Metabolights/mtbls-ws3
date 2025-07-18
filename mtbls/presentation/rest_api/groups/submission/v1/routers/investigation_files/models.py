from typing import Generic

from metabolights_utils.common import CamelCaseModel

from mtbls.domain.shared.repository.paginated_output import T
from mtbls.presentation.rest_api.core.base import APIBaseModel


class FieldData(CamelCaseModel, Generic[T]):
    value: T


class StudyJsonListResponse(APIBaseModel, Generic[T]):
    resource_id: str
    path: str
    action: str
    indices: list[int] = []
    data: list[T] = []
