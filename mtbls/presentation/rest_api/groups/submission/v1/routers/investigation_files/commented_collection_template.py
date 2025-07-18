from logging import getLogger
from typing import Annotated, Union

from dependency_injector.wiring import Provide, inject
from fastapi import Body, Depends, Path, Query
from metabolights_utils.common import CamelCaseModel
from metabolights_utils.models.isa.common import IsaAbstractModel

from mtbls.application.services.interfaces.study_metadata_service_factory import (
    StudyMetadataServiceFactory,
)
from mtbls.domain.entities.investigation import (
    CommentItem,
    OntologyItem,
)
from mtbls.domain.shared.permission import StudyPermissionContext
from mtbls.presentation.rest_api.core.responses import (
    APIErrorResponse,
    APIResponse,
)
from mtbls.presentation.rest_api.groups.auth.v1.routers.dependencies import (
    check_read_permission,
    check_update_permission,
)
from mtbls.presentation.rest_api.groups.submission.v1.routers.investigation_files.collection_with_fields_template import (
    StudyItemCollectionWithFields,
)
from mtbls.presentation.rest_api.groups.submission.v1.routers.investigation_files.endpoint_config import (
    RestApiEndpointConfiguration,
)
from mtbls.presentation.rest_api.groups.submission.v1.routers.investigation_files.models import (
    StudyJsonListResponse,
)
from mtbls.presentation.rest_api.shared.data_types import RESOURCE_ID_IN_PATH

logger = getLogger(__file__)


class CommentedStudyItemCollection(StudyItemCollectionWithFields):
    def __init__(
        self,
        endpoint_prefix: str,
        item_name: str,
        collection_name: str,
        collection_route_path: str,
        collection_jsonpath: str,
        input_model_class: type[CamelCaseModel],
        output_model_class: type[CamelCaseModel],
        target_model_class: type[IsaAbstractModel],
        get_items_enabled: bool = True,
        get_item_enabled: bool = True,
        post_item_enabled: bool = True,
        patch_item_enabled: bool = True,
        delete_item_enabled: bool = True,
        updatable_fields: Union[
            None, list[tuple[str, Union[str, OntologyItem]]]
        ] = None,
    ):
        super().__init__(
            endpoint_prefix=endpoint_prefix,
            item_name=item_name,
            collection_name=collection_name,
            collection_route_path=collection_route_path,
            collection_jsonpath=collection_jsonpath,
            input_model_class=input_model_class,
            output_model_class=output_model_class,
            target_model_class=target_model_class,
            get_items_enabled=get_items_enabled,
            get_item_enabled=get_item_enabled,
            post_item_enabled=post_item_enabled,
            patch_item_enabled=patch_item_enabled,
            delete_item_enabled=delete_item_enabled,
            updatable_fields=updatable_fields,
        )

        self.comments_jsonpath = self.item_jsonpath + ".comments"

    def get_comments_get_configuration(self) -> RestApiEndpointConfiguration:
        return RestApiEndpointConfiguration(
            path=self.item_route_path + "/comments",
            method=self._get_comments,
            http_method="GET",
            summary=f"Get comments of the selected {self.item_name.lower().replace('_', ' ')}",
            response_model=APIResponse[StudyJsonListResponse[CommentItem]],
        )

    def get_comment_set_configuration(self) -> RestApiEndpointConfiguration:
        return RestApiEndpointConfiguration(
            path=self.item_route_path + "/comments",
            method=self._set_comment,
            http_method="PUT",
            summary=f"Update or create a comment for the selected {self.item_name.lower().replace('_', ' ')}",
            response_model=APIResponse[StudyJsonListResponse[CommentItem]],
        )

    def get_comment_delete_configuration(self) -> RestApiEndpointConfiguration:
        return RestApiEndpointConfiguration(
            path=self.item_route_path + "/comments",
            method=self._delete_comment,
            http_method="DELETE",
            summary=f"Delete a comment from the selected {self.item_name.lower().replace('_', ' ')}",
            response_model=APIResponse[StudyJsonListResponse[CommentItem]],
        )

    def get_endpoint_configurations(self) -> list[RestApiEndpointConfiguration]:
        configs = super().get_endpoint_configurations()
        if self.comments_jsonpath:
            configs.extend(
                [
                    self.get_comments_get_configuration(),
                    self.get_comment_set_configuration(),
                    self.get_comment_delete_configuration(),
                ]
            )
        return configs

    def _get_comments(self):
        index_description = f"{self.item_name.lower()} item index"

        @inject
        async def get_comments(
            resource_id: Annotated[str, RESOURCE_ID_IN_PATH],
            index: Annotated[int, Path(description=index_description)],
            context: Annotated[StudyPermissionContext, Depends(check_read_permission)],
            study_metadata_service_factory: StudyMetadataServiceFactory = Depends(
                Provide[  # noqa: FAST002
                    "services.study_metadata_service_factory"
                ]
            ),
            filter: Annotated[
                Union[None, str],
                Query(
                    description="Comment name and value filter. e.g. name=x or name=x,value=y"
                ),
            ] = None,
        ):
            filters = self.convert_filter_to_dict(filter)
            route_path = self._get_route_path(resource_id)
            template = self.comments_jsonpath
            if not filter:
                template = self.comments_jsonpath + "[*]"
            jsonpath = self._get_rendered_json_path(
                template, [index], subitem_filter=filters
            )
            metadata_service = await study_metadata_service_factory.create_service(
                resource_id
            )
            with metadata_service:
                (
                    comments,
                    indices,
                ) = await metadata_service.process_investigation_file(
                    operation="get",
                    target_jsonpath=jsonpath,
                    output_model_class=CommentItem,
                )
                if filter and not comments:
                    return APIErrorResponse(
                        error_message=f"Comment with filter {filter} is not found "
                        f"for {resource_id} {self.item_name.lower()} [{index}]."
                    )
                comments = comments if comments else []
                return APIResponse(
                    content=StudyJsonListResponse[CommentItem](
                        resource_id=resource_id,
                        action="get",
                        path=route_path,
                        data=comments,
                        indices=indices,
                    )
                )

        return get_comments

    def _set_comment(self):
        index_description = f"{self.item_name.lower()} item index"

        @inject
        async def update_comment(
            resource_id: Annotated[str, RESOURCE_ID_IN_PATH],
            index: Annotated[int, Path(description=index_description)],
            context: Annotated[
                StudyPermissionContext, Depends(check_update_permission)
            ],
            comment: Annotated[
                CommentItem,
                Body(description=f"{self.item_name.lower()} comment content"),
            ],
            study_metadata_service_factory: StudyMetadataServiceFactory = Depends(
                Provide["services.study_metadata_service_factory"]  # noqa: FAST002
            ),
        ):
            route_path = self._get_route_path(resource_id)
            if not comment.name:
                ValueError(resource_id, route_path, "Invalid comment name.")

            jsonpath = self._get_rendered_json_path(
                self.comments_jsonpath, [index], subitem_filter={"name": comment.name}
            )

            metadata_service = await study_metadata_service_factory.create_service(
                resource_id
            )
            with metadata_service:
                data, indices = await metadata_service.process_investigation_file(
                    operation="patch",
                    target_jsonpath=jsonpath,
                    output_model_class=CommentItem,
                    input_data=comment,
                )

                if data is not None:
                    return APIResponse(
                        content=StudyJsonListResponse(
                            resource_id=resource_id,
                            action="update",
                            path=route_path,
                            data=data,
                            indices=indices,
                        )
                    )
                return APIErrorResponse(
                    error_message=f"Invalid input {self.item_name.lower()} index {index} or filter for the study {resource_id}"
                )

            return APIErrorResponse(
                error_message="Investigation file does not have study"
            )

        return update_comment

    def _delete_comment(self):
        index_description = f"{self.item_name.lower()} item index"

        @inject
        async def delete_comment(
            resource_id: Annotated[str, RESOURCE_ID_IN_PATH],
            index: Annotated[int, Path(description=index_description)],
            context: Annotated[
                StudyPermissionContext, Depends(check_update_permission)
            ],
            comment: Annotated[
                CommentItem,
                Body(description=f"{self.item_name.lower()} comment content"),
            ],
            study_metadata_service_factory: StudyMetadataServiceFactory = Depends(
                Provide["services.study_metadata_service_factory"]  # noqa: FAST002
            ),
        ):
            route_path = self._get_route_path(resource_id)
            if not comment.name:
                ValueError(resource_id, route_path, "Invalid comment name.")
            jsonpath = self._get_rendered_json_path(
                self.comments_jsonpath,
                [index],
                subitem_filter={"name": comment.name, "value": comment.value},
            )
            metadata_service = await study_metadata_service_factory.create_service(
                resource_id
            )
            with metadata_service:
                data, indices = await metadata_service.process_investigation_file(
                    operation="delete",
                    target_jsonpath=jsonpath,
                    output_model_class=CommentItem,
                    input_data=comment,
                )

                if data is not None:
                    return APIResponse(
                        content=StudyJsonListResponse(
                            resource_id=resource_id,
                            action="delete",
                            path=route_path,
                            data=data,
                            indices=indices,
                        )
                    )
                return APIErrorResponse(
                    error_message=f"Invalid input {self.item_name.lower()} index {index} or filter for the study {resource_id}"
                )

            return APIErrorResponse(
                error_message="Investigation file does not have study"
            )

        return delete_comment
