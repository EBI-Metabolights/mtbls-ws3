import json
import logging
from pathlib import Path
from typing_extensions import get_origin

from mtbls.application.services.interfaces.repositories.file_object.file_object_read_repository import (  # noqa: E501
    FileObjectReadRepository,
)
from mtbls.application.services.interfaces.repositories.study.study_read_repository import (  # noqa: E501
    StudyReadRepository,
)
from mtbls.application.services.interfaces.search_index_management_gateway import (  # noqa: E501
    IndexDocumentInfo,
    SearchIndexManagementGateway,
)
from mtbls.domain.enums.filter_operand import FilterOperand
from mtbls.domain.enums.sort_order import SortOrder
from mtbls.domain.enums.study_status import StudyStatus
from mtbls.domain.shared.repository.entity_filter import EntityFilter
from mtbls.domain.shared.repository.paginated_output import PaginatedOutput
from mtbls.domain.shared.repository.query_options import QueryFieldOptions
from mtbls.domain.shared.repository.sort_option import SortOption
from mtbls.presentation.cli.indices.public_study_search.schemas import (
    PublicStudyLiteIndexModel,
)

logger = logging.getLogger()


async def sync_search_index(
    index_name: str,
    index_management_gateway: SearchIndexManagementGateway,
    study_read_repository: StudyReadRepository,
    index_cache_files_object_repository: FileObjectReadRepository,
    mappings_file_path: None | str = None,
    recreate_index: bool = True,
    debug=False,
    study_statuses: list[StudyStatus] | None = None,
):
    if study_statuses is None:
        study_statuses = [StudyStatus.PUBLIC]
    await index_management_gateway.create_index(
        index=index_name,
        delete_before=recreate_index,
        mappings_file_path=mappings_file_path,
    )
    indexed_documents = await index_management_gateway.get_document_ids(
        index=index_name, update_field_name="updatedTime"
    )
    indexed_documents_dict = {x.id: x for x in indexed_documents}

    # DB metadata
    db_study_metadata: PaginatedOutput = await study_read_repository.select_fields(
        query_field_options=QueryFieldOptions(
            selected_fields=[
                "accession_number",
                "status",
                "created_at",
                "first_private_date",
                "first_public_date",
                "revision_datetime",
                "update_date",
            ],
            filters=[
                EntityFilter(
                    key="status",
                    value=study_statuses,
                    operand=FilterOperand.IN,
                )
            ],
            sort_options=[SortOption(key="created_at", order=SortOrder.DESC)],
        )
    )

    db_studies_dict = {
        x[0]: IndexDocumentInfo(
            id=x[0], updated_at=max([time for time in x[2:] if time])
        )
        for x in db_study_metadata.data
    }

    indexed_documents_set = {x for x in indexed_documents_dict.keys()}
    db_studies_set = {x for x in db_studies_dict.keys()}
    all_studies_in_scope = indexed_documents_set.union(db_studies_set)
    cached_documents = set()
    uncached_documents = set()
    for resource_id in all_studies_in_scope:
        filename = f"{resource_id}.json"
        if await index_cache_files_object_repository.exists(
            resource_id=resource_id, object_key=filename
        ):
            cached_documents.add(resource_id)
        else:
            uncached_documents.add(resource_id)

    will_be_indexed = db_studies_set - indexed_documents_set

    will_be_deleted = indexed_documents_set - db_studies_set

    will_be_updated = {
        x
        for x in indexed_documents_set
        if x in db_studies_set
        and db_studies_dict[x].updated_at > indexed_documents_dict[x].updated_at
    }

    # reindex updated studies
    will_be_indexed = will_be_indexed.union(will_be_updated)
    # TODO: Ensure all required resources are indexed
    missing_cache_documents = will_be_indexed - cached_documents
    if missing_cache_documents:
        logger.warning("Missing cache documents %s", ", ".join(missing_cache_documents))
    for resource_id in will_be_deleted:
        index_management_gateway.remove_document(index=index_name, id=resource_id)

    temp_path = Path(".temp")
    for resource_id in will_be_indexed:
        target_path = temp_path / Path(f"{resource_id}.json")
        target_path = target_path.resolve()
        if resource_id in cached_documents:
            await index_cache_files_object_repository.download(
                resource_id=resource_id,
                object_key=target_path.name,
                target_path=target_path,
            )
            try:
                raw_text = target_path.read_text()
                if not raw_text.strip():
                    logger.warning(
                        "Empty cache JSON for %s at %s", resource_id, target_path
                    )
                    continue
                file_content = json.loads(raw_text)
            except json.JSONDecodeError as ex:
                logger.warning(
                    "Invalid cache JSON for %s at %s: %s", resource_id, target_path, ex
                )
                continue
            model = PublicStudyLiteIndexModel.model_validate(file_content)
            if debug:
                for name, f in type(model).model_fields.items():
                    if get_origin(f.annotation) is set:
                        print("STILL A SET:", name, f.annotation)
                for name in type(model).model_fields:
                    try:
                        model.model_dump(by_alias=True, include={name})
                    except Exception as e:
                        print("Dump fails on field:", name, "->", e)

            try:
                body = model.model_dump(by_alias=True)
            except Exception as ex:
                import traceback

                traceback.print_exc()
                raise ex
            await index_management_gateway.index_document(
                index=index_name, id=resource_id, body=body
            )

    pass
