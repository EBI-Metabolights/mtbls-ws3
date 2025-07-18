import logging
from typing import Union

from mtbls.application.services.interfaces.auth.authorization_service import (
    AuthorizationService,
)
from mtbls.application.services.interfaces.repositories.study.study_read_repository import (
    StudyReadRepository,
)
from mtbls.application.services.interfaces.repositories.user.user_read_repository import (
    UserReadRepository,
)
from mtbls.domain.entities.user import UserOutput
from mtbls.domain.enums.study_status import StudyStatus
from mtbls.domain.enums.user_role import UserRole
from mtbls.domain.enums.user_status import UserStatus
from mtbls.domain.exceptions.repository import StudyResourceNotFoundError
from mtbls.domain.shared.permission import StudyPermissionContext

logger = logging.getLogger(__name__)


class AuthorizationServiceImpl(AuthorizationService):
    def __init__(
        self,
        user_read_repository: UserReadRepository,
        study_read_repository: StudyReadRepository,
    ) -> None:
        self.user_read_repository = user_read_repository
        self.study_read_repository = study_read_repository

    async def get_user_resource_permission(
        self,
        user: Union[None, UserOutput],
        resource_id: str,
        sub_resource: Union[None, str] = None,
    ):
        permission_context = StudyPermissionContext(user=user)

        try:
            permission_context.study = (
                await self.study_read_repository.get_study_by_accession(resource_id)
            )
            if not permission_context.study:
                raise StudyResourceNotFoundError(f"Study  {resource_id} not found")
        except Exception as e:
            logger.error("%s: %s", type(e), str(e))
            return permission_context

        return await self.update_permission_context(permission_context)

    async def get_permissions(
        self, username: str, resource_id: str, sub_resource: Union[None, str] = None
    ) -> StudyPermissionContext:
        permission_context = StudyPermissionContext()

        try:
            permission_context.study = (
                await self.study_read_repository.get_study_by_accession(resource_id)
            )
            if not permission_context.study:
                raise StudyResourceNotFoundError(f"Study  {resource_id} not found")
        except Exception as e:
            logger.exception(e)
            return permission_context

        try:
            permission_context.user = (
                await self.user_read_repository.get_user_by_username(username)
            )
            if not permission_context.user:
                raise Exception("User not found")
        except Exception as e:
            logger.exception(e)
            if permission_context.study.status == StudyStatus.PUBLIC:
                permission_context.permissions.read = True
            return permission_context

        return await self.update_permission_context(permission_context)

    async def update_permission_context(
        self, permission_context: StudyPermissionContext
    ):
        if permission_context.study.status == StudyStatus.PUBLIC:
            permission_context.permissions.read = True
        if permission_context.user and permission_context.user.username:
            study_users = (
                await self.study_read_repository.get_study_submitters_by_accession(
                    permission_context.study.accession_number
                )
            )
            user_names = {}
            if study_users:
                user_names = {x.username for x in study_users}
            if permission_context.user.username in user_names:
                permission_context.is_owner = True

            if permission_context.user.status not in (UserStatus.ACTIVE,):
                return permission_context

            if permission_context.user.role in (
                UserRole.CURATOR,
                UserRole.SYSTEM_ADMIN,
            ):
                permission_context.permissions.create = True
                permission_context.permissions.read = True
                permission_context.permissions.update = True
                permission_context.permissions.delete = True
                return permission_context

            if permission_context.user.role in (UserRole.SUBMITTER,):
                permission_context.permissions.create = True
                permission_context.permissions.read = True
                if permission_context.study.status == StudyStatus.PROVISIONAL:
                    permission_context.permissions.update = True
                    permission_context.permissions.delete = True

        return permission_context
