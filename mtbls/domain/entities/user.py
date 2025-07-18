import datetime
from typing import Annotated, Union

from pydantic import Field, field_validator

from mtbls.domain.entities.base_entity import BaseUser
from mtbls.domain.enums.user_role import UserRole
from mtbls.domain.enums.user_status import UserStatus


class UserInput(BaseUser):
    username: Union[None, str] = None
    email: Union[None, str] = None
    role: Union[None, UserRole, int] = UserRole.ANONYMOUS
    status: Union[None, UserStatus, int] = UserStatus.NEW
    password_hash: Annotated[Union[None, str], Field(repr=False)] = None
    api_token: Annotated[Union[None, str], Field(repr=False)] = None
    first_name: Union[None, str] = None
    last_name: Union[None, str] = None
    orcid: Union[None, str] = None
    join_date: Union[None, datetime.datetime] = None
    address: Union[None, str] = None
    affiliation: Union[None, str] = None
    affiliation_url: Union[None, str] = None

    @field_validator("role")
    @classmethod
    def role_validator(cls, value):
        if value is None:
            return None
        if isinstance(value, UserRole):
            return value
        if isinstance(value, int):
            return UserRole(value)
        return None

    @field_validator("status")
    @classmethod
    def status_validator(cls, value):
        if value is None:
            return None
        if isinstance(value, UserStatus):
            return value
        if isinstance(value, int):
            return UserStatus(value)
        return None


class UserOutput(UserInput):
    id_: int


class IdentityOutput(BaseUser):
    id_: int
    first_name: Union[None, str] = None
    last_name: Union[None, str] = None
    orcid: Union[None, str] = None
    username: Union[None, str] = None
    email: Union[None, str] = None
    role: Union[None, UserRole] = UserRole.ANONYMOUS
    status: Union[None, UserStatus] = UserStatus.NEW
    password_hash: Annotated[Union[None, str], Field(repr=False)] = None
    api_token: Annotated[Union[None, str], Field(repr=False)] = None
    join_date: Union[None, datetime.datetime] = None
