import abc
from typing import Any, Generic, TypeVar, Union

from pydantic import BaseModel

from mtbls.domain.entities.study import StudyOutput
from mtbls.domain.entities.user import UserOutput

T = TypeVar("T")
R = TypeVar("R")


class ResourcePermission(BaseModel):
    create: bool = False
    read: bool = False
    update: bool = False
    delete: bool = False


class PermissionContext(abc.ABC, BaseModel, Generic[T, R]):
    user: Union[None, T] = None
    is_owner: bool = False
    study: Union[None, R] = None
    permissions: ResourcePermission = ResourcePermission()
    parameters: dict[str, Any] = {}


class StudyPermissionContext(PermissionContext[UserOutput, StudyOutput]): ...
