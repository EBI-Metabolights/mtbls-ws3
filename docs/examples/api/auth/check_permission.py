import logging
from typing import Union

from fastapi import Request, Security
from typing_extensions import Annotated

from mtbls.application.context.request_tracker import get_request_tracker
from mtbls.domain.entities.auth_user import AuthenticatedUser
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
    logger.warning(
        "Unauthenticated user %s is granted to view resource %s",
        resource_id,
    )
    raise AuthenticationError("User has not authenticated.")
