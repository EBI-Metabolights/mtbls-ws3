import abc
from typing import Union

from mtbls.domain.entities.user import UserOutput
from mtbls.domain.shared.permission import StudyPermissionContext


class AuthorizationService(abc.ABC):
    @abc.abstractmethod
    async def get_permissions(
        self,
        username: Union[None, str],
        resource_id: str,
        sub_resource: Union[None, str] = None,
    ) -> StudyPermissionContext: ...

    @abc.abstractmethod
    async def get_user_resource_permission(
        self,
        user: Union[None, UserOutput],
        resource_id: str,
        sub_resource: Union[None, str] = None,
    ) -> StudyPermissionContext: ...
