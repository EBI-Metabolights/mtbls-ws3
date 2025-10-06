from logging import getLogger
from typing import Annotated, Any, Union

from dependency_injector.wiring import Provide, inject
from fastapi import Body, Depends, Path
from metabolights_utils.common import CamelCaseModel
from metabolights_utils.models.isa.common import IsaAbstractModel
from pydantic.alias_generators import to_camel

from mtbls.application.services.interfaces.study_metadata_service_factory import (
    StudyMetadataServiceFactory,
)
from mtbls.domain.entities.investigation import OntologyItem
from mtbls.domain.shared.permission import StudyPermissionContext
from mtbls.presentation.rest_api.core.responses import (
    APIErrorResponse,
    APIResponse,
)
from mtbls.presentation.rest_api.groups.auth.v1.routers.dependencies import (
    check_read_permission,
    check_update_permission,
)
from mtbls.presentation.rest_api.groups.submission.v1.routers.investigation_files.collection_template import (  # noqa: E501
    StudyItemCollection,
)
from mtbls.presentation.rest_api.groups.submission.v1.routers.investigation_files.endpoint_config import (  # noqa: E501
    RestApiEndpointConfiguration,
)
from mtbls.presentation.rest_api.groups.submission.v1.routers.investigation_files.models import (  # noqa: E501
    FieldData,
    StudyJsonListResponse,
)
from mtbls.presentation.rest_api.shared.data_types import RESOURCE_ID_IN_PATH

logger = getLogger(__name__)


class StudyItemCollectionWithFields(StudyItemCollection):
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
        )
        self.updatable_fields = updatable_fields

    def get_endpoint_configurations(self) -> list[RestApiEndpointConfiguration]:
        items = super().get_endpoint_configurations()
        if self.updatable_fields:
            for field, field_type in self.updatable_fields:
                items.append(self.get_field_configuration(field, field_type))
                items.append(self.get_update_field_configuration(field, field_type))
        return items

    def get_field_configuration(
        self, field_name: str, field_type: Union[str, OntologyItem]
    ) -> RestApiEndpointConfiguration:
        return RestApiEndpointConfiguration(
            path=self.item_route_path + "/" + field_name,
            method=self._get_item_field(field_name, field_type),
            http_method="GET",
            summary=f"Get {field_name.replace('_', ' ')} of the selected {self.item_name.lower().replace('_', ' ')}",  # noqa: E501
            response_model=APIResponse[StudyJsonListResponse[FieldData[field_type]]],
        )

    def get_update_field_configuration(
        self, field_name: str, field_type: Union[str, OntologyItem]
    ) -> RestApiEndpointConfiguration:
        return RestApiEndpointConfiguration(
            path=self.item_route_path + "/" + field_name,
            method=self._update_item_field(field_name, field_type),
            http_method="PUT",
            summary=f"Update {field_name.replace('_', ' ')} of the selected {self.item_name.lower().replace('_', ' ')}",  # noqa: E501
            response_model=APIResponse[StudyJsonListResponse[FieldData[field_type]]],
        )

    def _get_item_field(self, field_name: str, field_type: Union[str, OntologyItem]):
        index_description = f"{self.item_name.lower()} item index"

        def get_item_wrapper():
            @inject
            async def get_item_field_wrapper(
                resource_id: Annotated[str, RESOURCE_ID_IN_PATH],
                context: Annotated[
                    StudyPermissionContext, Depends(check_read_permission)
                ],
                index: Annotated[int, Path(description=index_description)],
                study_metadata_service_factory: StudyMetadataServiceFactory = Depends(
                    Provide[  # noqa: FAST002
                        "services.study_metadata_service_factory"
                    ]
                ),
            ):
                jsonpath_template = self._get_rendered_json_path(
                    self.item_jsonpath, [index]
                )
                json_response = await self._get_field_value(
                    field_name=field_name,
                    field_type=field_type,
                    study_metadata_service_factory=study_metadata_service_factory,
                    resource_id=resource_id,
                    jsonpath_template=jsonpath_template,
                    render_values=[index],
                )
                if not json_response:
                    return APIErrorResponse(
                        error_message=f"Invalid input {self.item_name.lower()} index {index} or filter for the study {resource_id}"  # noqa: E501
                    )

                return APIResponse(content=json_response)

            return get_item_field_wrapper

        return get_item_wrapper

    def _update_item_field(self, field_name: str, field_type: Union[str, OntologyItem]):
        index_description = f"{self.item_name.lower()} item index"
        field_description = f"{self.item_name.lower()} {field_name} value"

        def update_item_wrapper():
            @inject
            async def update_item_field_wrapper(
                resource_id: Annotated[str, RESOURCE_ID_IN_PATH],
                context: Annotated[
                    StudyPermissionContext, Depends(check_update_permission)
                ],
                index: Annotated[int, Path(description=index_description)],
                value: Annotated[
                    FieldData[field_type], Body(description=field_description)
                ],
                study_metadata_service_factory: StudyMetadataServiceFactory = Depends(
                    Provide[  # noqa: FAST002
                        "services.study_metadata_service_factory"
                    ]
                ),
            ):
                jsonpath_template = self._get_rendered_json_path(
                    self.item_jsonpath, [index]
                )
                json_response = await self._update_field_value(
                    field_name=field_name,
                    field_type=field_type,
                    study_metadata_service_factory=study_metadata_service_factory,
                    resource_id=resource_id,
                    jsonpath_template=jsonpath_template,
                    render_values=[index],
                    value=value,
                )
                if not json_response:
                    return APIErrorResponse(
                        error_message=f"Invalid input {self.item_name.lower()} index {index} or filter for the study {resource_id}"  # noqa: E501
                    )

                return APIResponse(content=json_response)

            return update_item_field_wrapper

        return update_item_wrapper

    async def _get_field_value(
        self,
        field_name: str,
        field_type: Union[str, OntologyItem],
        study_metadata_service_factory: StudyMetadataServiceFactory,
        resource_id: str,
        jsonpath_template: str,
        render_values: list[int],
    ) -> Union[None, StudyJsonListResponse]:
        render_values = [str(x) for x in render_values]
        jsonpath = self._get_rendered_json_path(
            jsonpath=jsonpath_template, values=render_values
        )
        route_path = self._get_route_path(resource_id)
        metadata_service = await study_metadata_service_factory.create_service(
            resource_id
        )
        response = None
        with metadata_service:
            field_path = jsonpath + "." + to_camel(field_name)
            result, _ = await metadata_service.process_investigation_file(
                operation="get",
                target_jsonpath=field_path,
                output_model_class=field_type,
            )
            data = [FieldData(value=x) for x in result]
            if data:
                response = StudyJsonListResponse[FieldData[field_type]](
                    resource_id=resource_id,
                    action="get",
                    path=route_path,
                    data=data,
                    indices=[],
                )

        return response

    async def _update_field_value(
        self,
        field_name: str,
        field_type: Any,
        study_metadata_service_factory: StudyMetadataServiceFactory,
        resource_id: str,
        jsonpath_template: str,
        render_values: list[int],
        value: FieldData,
    ) -> Union[None, StudyJsonListResponse]:
        render_values = [str(x) for x in render_values]
        jsonpath = self._get_rendered_json_path(
            jsonpath=jsonpath_template, values=render_values
        )
        route_path = self._get_route_path(resource_id)
        metadata_service = await study_metadata_service_factory.create_service(
            resource_id
        )
        response = None
        with metadata_service:
            data, _ = await metadata_service.process_investigation_file(
                operation="update-string",
                target_jsonpath=jsonpath,
                output_model_class=field_type,
                input_data=value.value,
                field_name=field_name,
            )

            if data is not None:
                response = StudyJsonListResponse[FieldData[field_type]](
                    resource_id=resource_id,
                    action="update",
                    path=route_path + "/" + field_name,
                    data=[value],
                    indices=[],
                )

        return response
