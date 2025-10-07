import logging
from typing import Union

from fastapi import Request, Security
from typing_extensions import Annotated

from mtbls.application.context.request_tracker import get_request_tracker
from mtbls.domain.entities.auth_user import AuthenticatedUser, UnauthenticatedUser
from mtbls.domain.entities.user import UserOutput
from mtbls.domain.enums.study_status import StudyStatus
from mtbls.domain.enums.user_role import UserRole
from mtbls.domain.exceptions.auth import AuthenticationError, AuthorizationError
from mtbls.domain.shared.permission import StudyPermissionContext
from mtbls.presentation.rest_api.groups.auth.v1.routers import oauth2_scheme
from mtbls.presentation.rest_api.shared.data_types import RESOURCE_ID_IN_PATH

logger = logging.getLogger(__name__)


async def check_read_permission(
    resource_id: Annotated[str, RESOURCE_ID_IN_PATH],
    jwt_token: Union[None, str] = Security(oauth2_scheme.oauth2_scheme),
    request: Request = None,
) -> StudyPermissionContext:
    get_request_tracker().resource_id_var.set(resource_id if resource_id else "-")

    if isinstance(request.user, AuthenticatedUser):
        if not jwt_token:
            raise AuthenticationError("Invalid jwt token.")
        context = request.user.permission_context
        if not context or not context.study or not context.permissions.read:
            logger.warning(
                "User %s is not granted to view resource %s",
                context.user.id_,
                resource_id,
            )
            raise AuthorizationError(
                f"User {context.user.id_} is not granted to view resource {resource_id}"
            )
        logger.debug(
            "User %s is granted to view resource %s",
            context.user.id_,
            resource_id,
        )
        return context
    elif isinstance(request.user, UnauthenticatedUser):
        context = request.user.permission_context
        if context and context.study and context.study.status == StudyStatus.PUBLIC:
            logger.debug(
                "Unauthenticated user is granted to view PUBLIC resource %s",
                resource_id,
            )
            return context

    logger.warning(
        "Unauthenticated user %s is not granted to view resource  %s",
        resource_id,
    )
    raise AuthorizationError(f"User has no authorization to read {resource_id}.")


async def check_update_permission(
    resource_id: Annotated[str, RESOURCE_ID_IN_PATH],
    jwt_token: Union[None, str] = Security(oauth2_scheme.oauth2_scheme),
    request: Request = None,
) -> StudyPermissionContext:
    if not jwt_token:
        raise AuthenticationError("Invalid jwt token.")
    get_request_tracker().resource_id_var.set(resource_id if resource_id else "-")
    if isinstance(request.user, AuthenticatedUser):
        context = request.user.permission_context
        if not context or not context.study:
            raise AuthorizationError(
                resource_id, f"{resource_id} is not valid or user has no permission."
            )
        if not context.permissions.update:
            raise AuthorizationError(
                context.user.id_,
                resource_id,
                f"User {context.user.id_} is not granted to update resource {resource_id}",  # noqa: E501
            )
        logger.debug(
            "User %s is granted to update resource %s",
            context.user.id_,
            resource_id,
        )
        return context
    raise AuthenticationError("User has not authenticated.")


async def check_curator_role(
    jwt_token: Union[None, str] = Security(oauth2_scheme.oauth2_scheme),
    request: Request = None,
) -> UserOutput:
    if not jwt_token:
        raise AuthenticationError("Invalid jwt token.")

    if isinstance(request.user, AuthenticatedUser):
        user = request.user.user_detail
        if not user or user.role not in [UserRole.CURATOR, UserRole.SYSTEM_ADMIN]:
            raise AuthorizationError("User does not have curation permission.")
        logger.debug("User %s has curator role.", user.id_)
        return user

    raise AuthenticationError("User has not authenticated.")
