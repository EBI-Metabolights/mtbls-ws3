from logging import getLogger
from typing import Annotated, Union

from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Body, Depends, Query

from mtbls.application.services.interfaces.study_metadata_service_factory import (
    StudyMetadataServiceFactory,
)
from mtbls.application.services.study_metadata_service.models import (
    IsaTableDataUpdates,
)
from mtbls.domain.entities.isa_table import (
    ColumnDefinition,
    IsaTableData,
    IsaTableFileObject,
    IsaTableRow,
)
from mtbls.domain.shared.data_types import ZeroOrPositiveInt
from mtbls.domain.shared.permission import StudyPermissionContext
from mtbls.presentation.rest_api.core.responses import (
    APIListResponse,
    APIResponse,
)
from mtbls.presentation.rest_api.groups.auth.v1.routers.dependencies import (
    check_read_permission,
    check_update_permission,
)
from mtbls.presentation.rest_api.shared.data_types import RESOURCE_ID_IN_PATH

logger = getLogger(__file__)

router = APIRouter(prefix="/submissions/v1")


def get_isa_file_items(data_type: str, filename_regex: str):
    file_name_description = f"{data_type} file name"

    @inject
    async def get_isa_items(
        resource_id: Annotated[str, RESOURCE_ID_IN_PATH],
        context: Annotated[StudyPermissionContext, Depends(check_read_permission)],
        file_name: Annotated[
            str, Query(description=file_name_description, pattern=filename_regex)
        ],
        study_metadata_service_factory: StudyMetadataServiceFactory = Depends(
            Provide["services.study_metadata_service_factory"]
        ),
        offset: Annotated[
            Union[None, ZeroOrPositiveInt], Query(description="row number")
        ] = None,
        limit: Annotated[
            Union[None, ZeroOrPositiveInt],
            Query(description="maximum number of rows"),
        ] = None,
    ):
        resource_id = context.study.accession_number
        metadata_service = await study_metadata_service_factory.create_service(
            resource_id
        )
        with metadata_service:
            isa_table_data = await metadata_service.load_isa_table_file(
                object_key=file_name,
                offset=offset,
                limit=limit,
            )

        isa_table_data.columns = [
            ColumnDefinition.model_validate(x, from_attributes=True)
            for x in isa_table_data.columns
        ]
        response = APIResponse[IsaTableData](content=isa_table_data)

        if not isa_table_data or not isa_table_data.rows:
            response.success_message = "There is no data that matches the criteria."
        else:
            response.success_message = f"{len(isa_table_data.rows)} rows."
        return response

    return get_isa_items


def update_isa_file_items(data_type: str, filename_regex: str):
    file_name_description = f"{data_type} file name"

    @inject
    async def update_isa_table_row(
        resource_id: Annotated[str, RESOURCE_ID_IN_PATH],
        file_name: Annotated[
            str, Query(description=file_name_description, pattern=filename_regex)
        ],
        context: Annotated[StudyPermissionContext, Depends(check_update_permission)],
        study_metadata_service_factory: StudyMetadataServiceFactory = Depends(
            Provide["services.study_metadata_service_factory"]
        ),
        updates: Annotated[IsaTableDataUpdates, Body(description="Updates")] = None,
    ):
        if not file_name:
            file_name = f"s_{resource_id}.txt"
        metadata_service = await study_metadata_service_factory.create_service(
            resource_id
        )
        with metadata_service:
            isa_table_data = await metadata_service.update_isa_table_rows(
                object_key=file_name, updates=updates
            )

        response = APIListResponse[IsaTableRow](
            success_message=f"{resource_id}", content=isa_table_data
        )
        if not isa_table_data:
            response.success_message = "There is no data that matches the criteria."
        else:
            response.success_message = f"{len(isa_table_data)} samples."
        return response

    return update_isa_table_row


def get_isa_file_headers(data_type: str, filename_regex: str):
    file_name_description = f"{data_type} file name"

    @inject
    async def get_headers(
        resource_id: Annotated[str, RESOURCE_ID_IN_PATH],
        file_name: Annotated[
            str, Query(description=file_name_description, pattern=filename_regex)
        ],
        context: Annotated[StudyPermissionContext, Depends(check_read_permission)],
        study_metadata_service_factory: StudyMetadataServiceFactory = Depends(
            Provide["services.study_metadata_service_factory"]
        ),
    ):
        resource_id = context.study.accession_number
        metadata_service = await study_metadata_service_factory.create_service(
            resource_id
        )
        with metadata_service:
            isa_table = await metadata_service.get_isa_table_data_columns(
                object_key=file_name
            )

        response = APIResponse[IsaTableFileObject](content=isa_table)
        return response

    return get_headers


def pub_isa_file_headers(data_type: str, filename_regex: str):
    file_name_description = f"{data_type} file name"

    @inject
    async def get_headers(
        resource_id: Annotated[str, RESOURCE_ID_IN_PATH],
        file_name: Annotated[
            str, Query(description=file_name_description, pattern=filename_regex)
        ],
        context: Annotated[StudyPermissionContext, Depends(check_read_permission)],
        study_metadata_service_factory: StudyMetadataServiceFactory = Depends(
            Provide["services.study_metadata_service_factory"]
        ),
    ):
        resource_id = context.study.accession_number
        metadata_service = await study_metadata_service_factory.create_service(
            resource_id
        )
        with metadata_service:
            isa_table = await metadata_service.save_isa_table_file(object_key=file_name)

        response = APIResponse[IsaTableFileObject](content=isa_table)
        return response

    return get_headers


filename_regex_map = {
    "sample": r"s_(.+)\.txt",
    "assay": r"s_(.+)\.txt",
    "assignment": r"m_(.+)\.tsv",
}


def add_api_routes(router: APIRouter):
    for item, tag in [
        ("sample", "Study Sample Files"),
        ("assay", "Study Assay Files"),
        ("assignment", "Metabolite Assignment Files"),
    ]:
        router.add_api_route(
            path="/" + item + "-files/{resource_id}",
            endpoint=get_isa_file_items(item, filename_regex_map[item]),
            tags=[tag],
            summary=f"Get {tag.lower()}",
            methods=["GET"],
            description=f"Get {tag.lower()}",
            response_model=APIResponse[IsaTableData],
        )

        router.add_api_route(
            path="/" + item + "-files/{resource_id}",
            endpoint=update_isa_file_items(item, filename_regex_map[item]),
            tags=[tag],
            summary=f"Update {tag.lower()}",
            methods=["PATCH"],
            description=f"Update {tag.lower()}",
            response_model=APIListResponse[IsaTableRow],
        )

        router.add_api_route(
            path="/" + item + "-files/{resource_id}/headers",
            endpoint=get_isa_file_headers(item, filename_regex_map[item]),
            tags=[tag],
            summary=f"Get {tag.lower()} column headers",
            methods=["GET"],
            description=f"Get {item} column headers",
            response_model=APIResponse[IsaTableFileObject],
        )


add_api_routes(router=router)
