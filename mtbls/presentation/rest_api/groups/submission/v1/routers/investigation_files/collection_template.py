from logging import getLogger
from typing import Annotated, Any, Union

from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Body, Depends, Path, Query
from fastapi.params import Param
from metabolights_utils.common import CamelCaseModel
from metabolights_utils.models.isa.common import IsaAbstractModel
from pydantic.alias_generators import to_camel

from mtbls.application.context.request_tracker import get_request_tracker
from mtbls.application.services.interfaces.study_metadata_service_factory import (
    StudyMetadataServiceFactory,
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
from mtbls.presentation.rest_api.groups.submission.v1.routers.investigation_files.endpoint_config import (
    RestApiEndpointConfiguration,
)
from mtbls.presentation.rest_api.groups.submission.v1.routers.investigation_files.models import (
    StudyJsonListResponse,
)
from mtbls.presentation.rest_api.shared.data_types import RESOURCE_ID_IN_PATH
from mtbls.presentation.rest_api.shared.dependencies import get_resource_id

logger = getLogger(__file__)


class StudyItemCollection:
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
    ):
        self.endpoint_prefix = endpoint_prefix
        self.item_name = item_name
        self.collection_name = collection_name
        self.collection_route_path = collection_route_path

        self.item_route_path = self.collection_route_path + "/" + "{index}"
        self.collection_route_path_pattern = rf"{self.collection_route_path}($|/$)"
        self.item_route_path_pattern = rf"{self.collection_route_path}/([0-9]+)($|/$)"
        self.collection_jsonpath = collection_jsonpath
        self.item_jsonpath = collection_jsonpath + "[$1]"
        self.input_model_class = input_model_class
        self.output_model_class = output_model_class
        self.target_model_class = target_model_class
        self.get_items_enabled = get_items_enabled
        self.delete_item_enabled = delete_item_enabled
        self.patch_item_enabled = patch_item_enabled
        self.post_item_enabled = post_item_enabled
        self.get_item_enabled = get_item_enabled

    def get_endpoint_configurations(self) -> list[RestApiEndpointConfiguration]:
        items = []
        if self.post_item_enabled:
            items.append(self.get_create_item_configuration())
        if self.get_items_enabled:
            items.append(self.get_items_configuration())
        if self.patch_item_enabled:
            items.append(self.get_patch_item_configuration())
        if self.delete_item_enabled:
            items.append(self.get_delete_item_configuration())
        return items

    def add_api_routes(self, api_router: APIRouter):
        endpoints = self.get_endpoint_configurations()
        for endpoint in endpoints:
            api_router.add_api_route(
                path=endpoint.path,
                endpoint=endpoint.method(),
                tags=[f"Investigation File {self.collection_name}"],
                methods=[endpoint.http_method],
                summary=endpoint.summary,
                description=endpoint.summary,
                response_model=endpoint.response_model,
            )

    def get_items_configuration(self) -> RestApiEndpointConfiguration:
        return RestApiEndpointConfiguration(
            path=self.collection_route_path,
            method=self._get_items,
            http_method="GET",
            summary=f"Get {self.collection_name.lower().replace('_', ' ')}",
            response_model=APIResponse[StudyJsonListResponse[self.output_model_class]],
        )

    def get_create_item_configuration(self) -> RestApiEndpointConfiguration:
        return RestApiEndpointConfiguration(
            path=self.collection_route_path,
            method=self._create_an_item,
            http_method="POST",
            summary=f"Create new {self.item_name.lower().replace('_', ' ')}",
            response_model=APIResponse[StudyJsonListResponse[self.output_model_class]],
        )

    def get_patch_item_configuration(self) -> RestApiEndpointConfiguration:
        return RestApiEndpointConfiguration(
            path=self.item_route_path,
            method=self._patch_an_item,
            http_method="PATCH",
            summary=f"Patch the selected {self.item_name.lower().replace('_', ' ')}",
            response_model=APIResponse[StudyJsonListResponse[self.output_model_class]],
        )

    def get_delete_item_configuration(self) -> RestApiEndpointConfiguration:
        return RestApiEndpointConfiguration(
            path=self.item_route_path,
            method=self._delete_an_item,
            http_method="DELETE",
            summary=f"Delete the selected {self.item_name.lower().replace('_', ' ')}",
            response_model=APIResponse[StudyJsonListResponse[self.output_model_class]],
        )

    def _copy_mapped_values(self, source: dict[str, Any], target: dict[str, Any]):
        for key, value in source.items():
            if key in target:
                if (
                    isinstance(
                        value,
                        (dict, list),
                    )
                ) and not value:
                    continue
                target[key] = value

    def _get_route_path(self, resource_id: str):
        full_route_path = get_request_tracker().route_path_var.get()
        path_parts = full_route_path.split(resource_id)
        if len(path_parts) > 1:
            return "".join(path_parts[1:])

        return full_route_path

    def _get_rendered_json_path(
        self,
        jsonpath: Union[None, str],
        values: list[Union[int, str]],
        subitem_filter: Union[None, dict[str, str]] = None,
        index: Union[None, int] = None,
    ) -> str:
        for i in range(len(values)):
            jsonpath = jsonpath.replace(f"[${i + 1}]", f"[{values[i]}]")

        camelcase_jsonpath = ".".join([to_camel(x) for x in jsonpath.split(".")])
        if index is not None:
            camelcase_jsonpath += f"[{index}]"
        elif subitem_filter:
            filter_terms = []
            for key, value in subitem_filter.items():
                camel_key = to_camel(key)
                filter_terms.append(f"@.{camel_key} == '{value}'")
            filter = " & ".join(filter_terms)
            camelcase_jsonpath = f"{camelcase_jsonpath}[?({filter})]"
        return camelcase_jsonpath

    def _get_items(self):
        index_description = (
            f"{self.item_name.lower()} item index, e.g. 0 or 1 (First item index is 0)."
        )
        filter_description = f"{self.item_name.lower()} item filter, e.g. name=Extraction,description=data"

        @inject
        async def get_items_wrapper(
            resource_id: Annotated[str, Depends(get_resource_id)],
            context: Annotated[StudyPermissionContext, Depends(check_read_permission)],
            study_metadata_service_factory: StudyMetadataServiceFactory = Depends(
                Provide[  # noqa: FAST002
                    "services.study_metadata_service_factory"
                ]
            ),
            index: Annotated[
                Union[None, int], Query(description=index_description)
            ] = None,
            filter: Annotated[
                Union[None, str], Query(description=filter_description)
            ] = None,
        ):
            filters = self.convert_filter_to_dict(filter)
            json_response = await self._get_all(
                study_metadata_service_factory=study_metadata_service_factory,
                resource_id=resource_id,
                model_class=self.output_model_class,
                jsonpath_template=self.collection_jsonpath,
                render_values=[],
                subitem_filter=filters,
                index=index,
            )

            return APIResponse(content=json_response)

        return get_items_wrapper

    def _create_an_item(self):
        updated_content_description = f"{self.item_name.lower()} content."

        @inject
        async def create_an_item_wrapper(
            resource_id: Annotated[str, Depends(get_resource_id)],
            context: Annotated[
                StudyPermissionContext, Depends(check_update_permission)
            ],
            updated_content: Annotated[
                self.input_model_class,
                Body(description=updated_content_description),
            ],
            study_metadata_service_factory: StudyMetadataServiceFactory = Depends(
                Provide[  # noqa: FAST002
                    "services.study_metadata_service_factory"
                ]
            ),
        ):
            route_path = self._get_route_path(resource_id)
            metadata_service = await study_metadata_service_factory.create_service(
                resource_id
            )
            jsonpath = self._get_rendered_json_path(self.collection_jsonpath, [])
            with metadata_service:
                data, indices = await metadata_service.process_investigation_file(
                    operation="insert",
                    target_jsonpath=jsonpath,
                    output_model_class=self.output_model_class,
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

        return create_an_item_wrapper

    def _patch_an_item(self):
        index_description = f"{self.item_name.lower()} item index"

        @inject
        async def patch_item_wrapper(
            resource_id: Annotated[str, RESOURCE_ID_IN_PATH],
            index: Annotated[int, Path(description=index_description)],
            context: Annotated[
                StudyPermissionContext, Depends(check_update_permission)
            ],
            updated_content: Annotated[
                self.input_model_class,
                Body(description=f"Updated {self.item_name.lower()} content"),
            ],
            ignore_empty_input_fields: Annotated[
                bool,
                Param(
                    description="Empty fields in the request will be ignored "
                    "and target resource fields will not be updated.",
                ),
            ] = True,
            study_metadata_service_factory: StudyMetadataServiceFactory = Depends(
                Provide["services.study_metadata_service_factory"]  # noqa: FAST002
            ),
        ):
            route_path = self._get_route_path(resource_id)
            jsonpath = self._get_rendered_json_path(self.item_jsonpath, [index])
            metadata_service = await study_metadata_service_factory.create_service(
                resource_id
            )
            with metadata_service:
                operation = "patch" if ignore_empty_input_fields else "update-object"

                data, indices = await metadata_service.process_investigation_file(
                    operation=operation,
                    target_jsonpath=jsonpath,
                    output_model_class=self.output_model_class,
                    input_data=updated_content,
                )
                if data is not None:
                    return APIResponse(
                        content=StudyJsonListResponse(
                            resource_id=resource_id,
                            action="patch",
                            path=route_path,
                            data=data,
                            indice=indices,
                        )
                    )
                return APIErrorResponse(
                    error_message=f"Invalid input {self.item_name.lower()} index {index} or filter for the study {resource_id}"
                )

            return APIErrorResponse(
                error_message="Investigation file does not have study"
            )

        return patch_item_wrapper

    def _delete_an_item(self):
        index_description = f"{self.item_name.lower()} item index"

        @inject
        async def delete_item_wrapper(
            resource_id: Annotated[str, RESOURCE_ID_IN_PATH],
            index: Annotated[int, Path(description=index_description)],
            context: Annotated[
                StudyPermissionContext, Depends(check_update_permission)
            ],
            study_metadata_service_factory: StudyMetadataServiceFactory = Depends(
                Provide[  # noqa: FAST002
                    "services.study_metadata_service_factory"
                ]
            ),
        ):
            jsonpath = self._get_rendered_json_path(self.item_jsonpath, [index])
            route_path = self._get_route_path(resource_id)
            metadata_service = await study_metadata_service_factory.create_service(
                resource_id
            )
            with metadata_service:
                data, indices = await metadata_service.process_investigation_file(
                    operation="delete",
                    target_jsonpath=jsonpath,
                    output_model_class=self.output_model_class,
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

        return delete_item_wrapper

    async def _get_all(
        self,
        study_metadata_service_factory: StudyMetadataServiceFactory,
        resource_id: str,
        model_class: type[CamelCaseModel],
        jsonpath_template: str,
        render_values: list[int],
        subitem_filter: Union[None, dict[str, str]] = None,
        index: Union[None, int] = None,
    ) -> StudyJsonListResponse:
        render_values = [str(x) for x in render_values]
        jsonpath = self._get_rendered_json_path(
            jsonpath=jsonpath_template,
            values=render_values,
            subitem_filter=subitem_filter,
            index=index,
        )
        if not subitem_filter and not index:
            jsonpath += "[*]"
        route_path = self._get_route_path(resource_id)
        metadata_service = await study_metadata_service_factory.create_service(
            resource_id
        )
        response = None
        with metadata_service:
            data, indices = await metadata_service.process_investigation_file(
                operation="get",
                target_jsonpath=jsonpath,
                output_model_class=model_class,
            )
            if data is not None:
                response = StudyJsonListResponse[model_class](
                    resource_id=resource_id,
                    action="get",
                    path=route_path,
                    data=data,
                    indices=indices,
                )

        return response

    def convert_filter_to_dict(self, filter: str) -> dict[str, str]:
        filters = {}
        if filter:
            for x in filter.split(","):
                parts = x.split("=")
                if len(parts) == 2:
                    k, v = parts
                    filters[k.strip()] = v
                else:
                    raise ValueError(f"Invalid filter {x}. Use key=value format")

        return filters
