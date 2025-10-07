import logging
from typing import Union

from fastapi import Request, Security
from typing_extensions import Annotated

from mtbls.application.context.request_tracker import get_request_tracker
from mtbls.domain.entities.auth_user import AuthenticatedUser, UnauthenticatedUser
from mtbls.domain.enums.study_status import StudyStatus
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
