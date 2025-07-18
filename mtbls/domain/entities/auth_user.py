from typing import Union

from starlette import authentication

from mtbls.domain.entities.user import UserOutput
from mtbls.domain.shared.permission import StudyPermissionContext


class UnauthenticatedUser(authentication.BaseUser):
    def __init__(self):
        self._permission_context: Union[None, StudyPermissionContext] = None

    @property
    def is_authenticated(self) -> bool:
        return False

    @property
    def display_name(self) -> str:
        return ""

    @property
    def identity(self) -> str:
        return ""

    @property
    def permission_context(self) -> StudyPermissionContext:
        return self._permission_context

    @permission_context.setter
    def permission_context(self, value):
        self._permission_context = value


class AuthenticatedUser(authentication.BaseUser):
    def __init__(self, user: UserOutput):
        self._user: UserOutput = user
        self._full_name = f"{self._user.first_name} {self._user.last_name}"
        self._permission_context: Union[None, StudyPermissionContext] = None

    @property
    def is_authenticated(self) -> bool:
        return True

    @property
    def display_name(self) -> str:
        return self._full_name

    @property
    def identity(self) -> str:
        return self._user.username

    @property
    def user_detail(self) -> UserOutput:
        return self._user

    @property
    def permission_context(self) -> StudyPermissionContext:
        return self._permission_context

    @permission_context.setter
    def permission_context(self, value):
        self._permission_context = value
