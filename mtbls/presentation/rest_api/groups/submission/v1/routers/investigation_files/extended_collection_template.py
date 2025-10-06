from logging import getLogger
from typing import Annotated, Union

from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Body, Depends, Path, Query
from metabolights_utils.common import CamelCaseModel
from metabolights_utils.models.isa.common import IsaAbstractModel
from metabolights_utils.models.isa.investigation_file import (
    OntologyAnnotation,
    ValueTypeAnnotation,
)

from mtbls.application.services.interfaces.study_metadata_service_factory import (
    StudyMetadataServiceFactory,
)
from mtbls.domain.entities.investigation import (
    OntologyItem,
    ValueTypeItem,
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
from mtbls.presentation.rest_api.groups.submission.v1.routers.investigation_files.commented_collection_template import (  # noqa: E501
    CommentedStudyItemCollection,
)
from mtbls.presentation.rest_api.groups.submission.v1.routers.investigation_files.endpoint_config import (  # noqa: E501
    RestApiEndpointConfiguration,
)
from mtbls.presentation.rest_api.groups.submission.v1.routers.investigation_files.models import (  # noqa: E501
    StudyJsonListResponse,
)
from mtbls.presentation.rest_api.shared.data_types import RESOURCE_ID_IN_PATH

logger = getLogger(__file__)


class ExtendedStudyItemCollection(CommentedStudyItemCollection):
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
        ontology_subitems: Union[None, list[str]] = None,
        value_type_subitems: Union[None, list[str]] = None,
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

        self.ontology_subitems = ontology_subitems
        self.value_type_subitems = value_type_subitems

    def add_api_routes(self, api_router: APIRouter):
        super().add_api_routes(api_router=api_router)
        if self.ontology_subitems:
            for subitem in self.ontology_subitems:
                configs = self.get_subitem_configurations(
                    subitem=subitem,
                    subitem_model_class=OntologyItem,
                    target_subitem_model_class=OntologyAnnotation,
                )

                for config in configs:
                    api_router.add_api_route(
                        path=config.path,
                        endpoint=config.method(),
                        tags=[f"Investigation File {self.collection_name}"],
                        methods=[config.http_method],
                        summary=config.summary,
                        description=config.summary,
                        response_model=config.response_model,
                    )
        if self.value_type_subitems:
            for subitem in self.value_type_subitems:
                configs = self.get_subitem_configurations(
                    subitem=subitem,
                    subitem_model_class=ValueTypeItem,
                    target_subitem_model_class=ValueTypeAnnotation,
                )

                for config in configs:
                    api_router.add_api_route(
                        path=config.path,
                        endpoint=config.method(),
                        tags=[f"Investigation File {self.collection_name}"],
                        methods=[config.http_method],
                        summary=config.summary,
                        description=config.summary,
                        response_model=config.response_model,
                    )

    # def subitem_get_configuration(
    #     self, subitem: str, subitem_model_class: type[CamelCaseModel]
    # ) -> RestApiEndpointConfiguration:
    #     return RestApiEndpointConfiguration(
    #         path=self.item_route_path + "/" + subitem + "/" + "{sub_index}",
    #         method=self._get_a_subitem(
    #             subitem=subitem, subitem_model_class=subitem_model_class
    #         ),
    #         http_method="GET",
    #         summary=f"Get {subitem.replace("_", " ")} from the selected {self.item_name.lower().replace("_", " ")}",  # noqa: E501
    #         response_model=APIResponse[StudyJsonResponse[subitem_model_class]],
    #     )

    def subitem_create_configuration(
        self,
        subitem: str,
        subitem_model_class: type[CamelCaseModel],
        target_subitem_model_class: type[IsaAbstractModel],
    ) -> RestApiEndpointConfiguration:
        return RestApiEndpointConfiguration(
            path=self.item_route_path + "/" + subitem,
            method=self._create_subitem(
                subitem=subitem,
                subitem_model_class=subitem_model_class,
                target_subitem_model_class=target_subitem_model_class,
            ),
            http_method="POST",
            summary=f"Add new item to the selected {self.item_name.lower().replace('_', ' ')} {subitem.replace('_', ' ')} list.",  # noqa: E501
            response_model=APIResponse[StudyJsonListResponse[subitem_model_class]],
        )

    def subitem_update_configuration(
        self, subitem: str, subitem_model_class: type[CamelCaseModel]
    ) -> RestApiEndpointConfiguration:
        return RestApiEndpointConfiguration(
            path=self.item_route_path + "/" + subitem + "/" + "{sub_index}",
            method=self._update_a_subitem(
                subitem=subitem, subitem_model_class=subitem_model_class
            ),
            http_method="PUT",
            summary=f"Update an item of {self.item_name.lower().replace('_', ' ')} {subitem.replace('_', ' ')} list.",  # noqa: E501
            response_model=APIResponse[StudyJsonListResponse[subitem_model_class]],
        )

    def subitem_list_configuration(
        self, subitem: str, subitem_model_class: type[CamelCaseModel]
    ) -> RestApiEndpointConfiguration:
        return RestApiEndpointConfiguration(
            path=self.item_route_path + "/" + subitem,
            method=self._get_subitems(
                subitem=subitem, subitem_model_class=subitem_model_class
            ),
            http_method="GET",
            summary=f"Add an item to selected {self.item_name.lower().replace('_', ' ')} {subitem.replace('_', ' ')} list.",  # noqa: E501
            response_model=APIResponse[StudyJsonListResponse[subitem_model_class]],
        )

    def subitem_delete_configuration(
        self, subitem: str, subitem_model_class: type[CamelCaseModel]
    ) -> RestApiEndpointConfiguration:
        return RestApiEndpointConfiguration(
            path=self.item_route_path + "/" + subitem + "/" + "{sub_index}",
            method=self._delete_subitem(subitem, subitem_model_class),
            http_method="DELETE",
            summary="Delete one item from the "
            f"{self.item_name.lower().replace('_', ' ')} "
            f"{subitem.replace('_', ' ')} list.",
            response_model=APIResponse[StudyJsonListResponse[subitem_model_class]],
        )

    def get_subitem_configurations(
        self,
        subitem: str,
        subitem_model_class: type[CamelCaseModel],
        target_subitem_model_class: type[IsaAbstractModel],
    ) -> list[RestApiEndpointConfiguration]:
        configs = [
            self.subitem_create_configuration(
                subitem, subitem_model_class, target_subitem_model_class
            ),
            self.subitem_list_configuration(subitem, subitem_model_class),
            # self.subitem_get_configuration(subitem, subitem_model_class),
            self.subitem_update_configuration(subitem, subitem_model_class),
            self.subitem_delete_configuration(subitem, subitem_model_class),
        ]

        return configs

    def _create_subitem(
        self,
        subitem: str,
        subitem_model_class: type[CamelCaseModel],
        target_subitem_model_class: type[IsaAbstractModel],
    ):
        content_description = f"{self.item_name.lower()} content."
        index_description = f"{self.item_name.lower()} item index"

        def _create_subitem_decorator():
            @inject
            async def post_a_subitem(
                resource_id: Annotated[str, RESOURCE_ID_IN_PATH],
                index: Annotated[int, Path(description=index_description)],
                context: Annotated[
                    StudyPermissionContext, Depends(check_update_permission)
                ],
                updated_content: Annotated[
                    subitem_model_class,
                    Body(description=content_description),
                ],
                study_metadata_service_factory: StudyMetadataServiceFactory = Depends(
                    Provide[  # noqa: FAST002
                        "services.study_metadata_service_factory"
                    ]
                ),
            ):
                route_path = self._get_route_path(resource_id)

                jsonpath_template = self.item_jsonpath + f".{subitem.lower()}"

                jsonpath = self._get_rendered_json_path(
                    jsonpath=jsonpath_template, values=[index]
                )

                metadata_service = await study_metadata_service_factory.create_service(
                    resource_id
                )
                with metadata_service:
                    data, indices = await metadata_service.process_investigation_file(
                        operation="insert",
                        target_jsonpath=jsonpath,
                        output_model_class=subitem_model_class,
                        input_data=updated_content,
                    )

                    if data is not None:
                        return APIResponse(
                            content=StudyJsonListResponse(
                                resource_id=resource_id,
                                action="add",
                                path=route_path,
                                data=data,
                                indices=indices,
                            )
                        )
                return APIErrorResponse(
                    error_message="Investigation file does not have study"
                )

            return post_a_subitem

        return _create_subitem_decorator

    def _get_subitems(self, subitem: str, subitem_model_class: type[CamelCaseModel]):
        index_description = f"{self.item_name.lower()} item index"
        subitem_filter = (
            f"{self.item_name.lower()} {subitem} filter. e.g. name=x or term=x,value=y"
        )
        sub_index_description = f"{self.item_name.lower()} {subitem} index"

        def _get_subitems_decorator():
            @inject
            async def get_list_subitems(
                resource_id: Annotated[str, RESOURCE_ID_IN_PATH],
                index: Annotated[int, Path(description=index_description)],
                context: Annotated[
                    StudyPermissionContext, Depends(check_read_permission)
                ],
                study_metadata_service_factory: StudyMetadataServiceFactory = Depends(
                    Provide[  # noqa: FAST002
                        "services.study_metadata_service_factory"
                    ]
                ),
                sub_index: Annotated[
                    Union[int], Query(description=sub_index_description)
                ] = None,
                filter: Annotated[
                    Union[None, str],
                    Query(description=subitem_filter),
                ] = None,
            ):
                filters = self.convert_filter_to_dict(filter)
                route_path = self._get_route_path(resource_id)
                jsonpath_template = self.item_jsonpath + "." + subitem
                if not filters and sub_index is None:
                    jsonpath_template += "[*]"
                elif sub_index is not None:
                    jsonpath_template += "[$2]"

                jsonpath = self._get_rendered_json_path(
                    jsonpath=jsonpath_template,
                    values=[index, sub_index],
                    subitem_filter=filters,
                )

                metadata_service = await study_metadata_service_factory.create_service(
                    resource_id
                )
                with metadata_service:
                    (
                        data,
                        indices,
                    ) = await metadata_service.process_investigation_file(
                        operation="get",
                        target_jsonpath=jsonpath,
                        output_model_class=subitem_model_class,
                    )

                    return APIResponse(
                        content=StudyJsonListResponse[subitem_model_class](
                            resource_id=resource_id,
                            action="list",
                            path=route_path,
                            data=data,
                            indices=indices,
                        )
                    )

            return get_list_subitems

        return _get_subitems_decorator

    # def _get_a_subitem(self, subitem: str, subitem_model_class: type[CamelCaseModel]):
    #     index_description = f"{self.item_name.lower()} item index"

    #     sub_index_description = f"{self.item_name.lower()} {subitem} index"

    #     def _get_a_subitem_decorator():
    #         @inject
    #         async def get_subitem(
    #             resource_id: Annotated[str, Depends(get_resource_id)],
    #             index: Annotated[int, Path(description=index_description)],
    #             context: Annotated[
    #                 StudyPermissionContext, Depends(check_read_permission)
    #             ],
    #             sub_index: Annotated[
    #                 Union[int], Path(description=sub_index_description)
    #             ] = None,
    #             item_filter: Annotated[None, dict[str, str]] = None,
    #             study_metadata_service_factory: StudyMetadataServiceFactory = Depends(
    #                 Provide[  # noqa: FAST002
    #                     "services.study_metadata_service_factory"
    #                 ]
    #             ),
    #         ):
    #             jsonpath_template = self.item_jsonpath + f".{subitem.lower()}[$2]"
    #             json_response = await self._get(
    #                 study_metadata_service_factory=study_metadata_service_factory,
    #                 resource_id=resource_id,
    #                 model_class=subitem_model_class,
    #                 jsonpath_template=jsonpath_template,
    #                 render_values=[index, sub_index],
    #             )
    #             if not json_response:
    #                 return APIErrorResponse(
    #                     error_message=f"Invalid input {self.item_name.lower()} index "
    #                     f"{index} {subitem} {sub_index} for the study {resource_id}"
    #                 )

    #             return APIResponse(content=json_response)

    #         return get_subitem

    #     return _get_a_subitem_decorator

    def _update_a_subitem(
        self, subitem: str, subitem_model_class: type[CamelCaseModel]
    ):
        index_description = f"{self.item_name.lower()} item index"

        sub_index_description = f"{self.item_name.lower()} {subitem} index"

        def update_subitem():
            @inject
            async def update_item(
                resource_id: Annotated[str, RESOURCE_ID_IN_PATH],
                index: Annotated[int, Path(description=index_description)],
                sub_index: Annotated[int, Path(description=sub_index_description)],
                context: Annotated[
                    StudyPermissionContext, Depends(check_update_permission)
                ],
                updated_content: Annotated[
                    subitem_model_class,
                    Body(description=f"Updated {self.item_name.lower()} content"),
                ],
                study_metadata_service_factory: StudyMetadataServiceFactory = Depends(
                    Provide["services.study_metadata_service_factory"]  # noqa: FAST002
                ),
            ):
                route_path = self._get_route_path(resource_id)
                jsonpath_template = self.item_jsonpath + f".{subitem.lower()}[$2]"
                jsonpath = self._get_rendered_json_path(
                    jsonpath=jsonpath_template, values=[index, sub_index]
                )
                metadata_service = await study_metadata_service_factory.create_service(
                    resource_id
                )
                with metadata_service:
                    data, indices = await metadata_service.process_investigation_file(
                        operation="update-object",
                        target_jsonpath=jsonpath,
                        output_model_class=subitem_model_class,
                        input_data=updated_content,
                    )

                    if data is not None:
                        return APIResponse(
                            content=StudyJsonListResponse(
                                resource_id=resource_id,
                                action="update-object",
                                path=route_path,
                                data=data,
                                indices=indices,
                            )
                        )
                    return APIErrorResponse(
                        error_message=f"Invalid input {self.item_name.lower()} index "
                        f"{index} or sub index {sub_index} for the study {resource_id}"
                    )

                return APIErrorResponse(
                    error_message="Investigation file does not have study"
                )

            return update_item

        return update_subitem

    def _delete_subitem(self, subitem: str, subitem_model_class: type[CamelCaseModel]):
        index_description = f"{self.item_name.lower()} item index"

        sub_index_description = f"{self.item_name.lower()} {subitem} index"

        def delete_subitem():
            @inject
            async def _delete_subitem(
                resource_id: Annotated[str, RESOURCE_ID_IN_PATH],
                index: Annotated[int, Path(description=index_description)],
                sub_index: Annotated[int, Path(description=sub_index_description)],
                context: Annotated[
                    StudyPermissionContext, Depends(check_update_permission)
                ],
                study_metadata_service_factory: StudyMetadataServiceFactory = Depends(
                    Provide[  # noqa: FAST002
                        "services.study_metadata_service_factory"
                    ]
                ),
            ):
                route_path = self._get_route_path(resource_id)
                jsonpath_template = self.item_jsonpath + f".{subitem.lower()}[$2]"
                jsonpath = self._get_rendered_json_path(
                    jsonpath=jsonpath_template, values=[index, sub_index]
                )
                metadata_service = await study_metadata_service_factory.create_service(
                    resource_id
                )
                with metadata_service:
                    data, indices = await metadata_service.process_investigation_file(
                        operation="delete",
                        target_jsonpath=jsonpath,
                        output_model_class=subitem_model_class,
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
                    error_message=f"Invalid input {self.item_name.lower()} "
                    f"index {index} or filter for the study {resource_id}"
                )

            return _delete_subitem

        return delete_subitem
