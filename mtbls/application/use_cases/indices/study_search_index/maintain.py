import datetime
import json
import pathlib
import shutil
import traceback
import uuid
from logging import getLogger

import aiofiles

from mtbls.application.services.interfaces.http_client import HttpClient
from mtbls.application.services.interfaces.repositories.file_object.file_object_write_repository import (
    FileObjectWriteRepository,
)
from mtbls.application.services.interfaces.repositories.study.study_read_repository import (
    StudyReadRepository,
)
from mtbls.application.services.interfaces.search_index_management_gateway import (
    SearchIndexManagementGateway,
)
from mtbls.application.use_cases.indices.kibana_indices.utils import (
    find_studies_will_be_processed,
    get_study_ids,
    load_json_file,
)
from mtbls.application.use_cases.indices.study_search_index.schemas import (
    PublicStudyLiteIndexModel,
)
from mtbls.application.use_cases.indices.study_search_index.update_json_files import (
    update_study_metadata_json_files,
)
from mtbls.domain.enums.study_status import StudyStatus

logger = getLogger(__name__)


async def load_file(file_path: pathlib.Path):
    async with aiofiles.open(file_path, "rb") as file:
        contents = await file.read()
        json_file = json.loads(contents)
        return json_file


async def maintain_study_search_index(
    study_read_repository: StudyReadRepository,
    http_client: HttpClient,
    index_cache_files_object_repository: FileObjectWriteRepository,
    metadata_files_object_repository: FileObjectWriteRepository,
    search_index_management_gateway: SearchIndexManagementGateway,
    index_name: str,
    index_mapping_file: None | str = None,
    index_settings_file: None | str = None,
    temp_folder: None | str = None,
    index_update_field: str = "modifiedTime",
    db_update_field: str = "update_date",
    target_study_status_list: None | list[StudyStatus] = None,
    recreate_index: bool = False,
):
    if not target_study_status_list:
        target_study_status_list = [StudyStatus.PUBLIC]
    exist = await search_index_management_gateway.exists(index=index_name)
    if not exist:
        await create_index(
            search_index_management_gateway=search_index_management_gateway,
            index_name=index_name,
            index_mapping_file=index_mapping_file,
            index_settings_file=index_settings_file,
            recreate_index=True,
        )
        recreate_index = True

    (
        new_study_ids,
        updated_study_ids,
        deleted_study_ids,
    ) = await find_studies_will_be_processed(
        study_read_repository=study_read_repository,
        search_index_management_gateway=search_index_management_gateway,
        index_name=index_name,
        index_update_field=index_update_field,
        db_update_field=db_update_field,
        target_study_status_list=target_study_status_list,
    )
    force_to_reindex_studies = updated_study_ids.copy()
    force_to_reindex_studies.extend(new_study_ids)

    await update_study_metadata_json_files(
        study_read_repository=study_read_repository,
        index_cache_files_object_repository=index_cache_files_object_repository,
        metadata_files_object_repository=metadata_files_object_repository,
        http_client=http_client,
        force_reindex_studies=force_to_reindex_studies,
    )
    if recreate_index:
        study_ids = await get_study_ids(
            study_read_repository=study_read_repository,
            target_study_status_list=target_study_status_list,
            db_update_field=db_update_field,
        )
    else:
        study_ids = force_to_reindex_studies
        for resource_id in deleted_study_ids:
            search_index_management_gateway.delete_document(
                index=index_name, id=resource_id
            )

    await create_index(
        search_index_management_gateway=search_index_management_gateway,
        index_name=index_name,
        index_mapping_file=index_mapping_file,
        index_settings_file=index_settings_file,
        recreate_index=recreate_index,
    )

    if not temp_folder:
        temp_folder = f"/tmp/reindex-public-studies/{uuid.uuid4().hex}"
    temp_path = pathlib.Path(temp_folder)
    temp_path.mkdir(parents=True, exist_ok=True)
    try:
        for idx, study_id in enumerate(study_ids):
            try:
                object_key = f"{study_id}.json"
                target_file_path = temp_path / pathlib.Path(object_key)
                file_exist = await index_cache_files_object_repository.exists(
                    study_id, object_key
                )
                if not file_exist:
                    logger.error(
                        "%s is skipped. File does not exist: %s", study_id, object_key
                    )
                    continue
                await index_cache_files_object_repository.download(
                    study_id, object_key, target_file_path
                )
                if not target_file_path.exists():
                    logger.error(
                        "%s is skipped. File does not exist: %s",
                        study_id,
                        target_file_path,
                    )
                    continue

                study_index_dict = await load_file(target_file_path)
                study_index = PublicStudyLiteIndexModel.model_validate(study_index_dict)

                info = await index_cache_files_object_repository.get_info(
                    study_id, object_key
                )
                if not info.updated_at:
                    study_index.modified_time = datetime.datetime.fromtimestamp(
                        target_file_path.stat().st_mtime
                    )
                elif isinstance(info.updated_at, str):
                    study_index.modified_time = datetime.datetime.strptime(
                        info.updated_at, "%Y-%m-%s"
                    )
                elif isinstance(info.updated_at, datetime.datetime):
                    study_index.modified_time = info.updated_at
                else:
                    study_index.modified_time = datetime.datetime.now()

                result = await search_index_management_gateway.index_document(
                    index=index_name,
                    id=study_index.study_id,
                    body=study_index.model_dump_json(by_alias=True),
                )
                result.raw
                if idx % 10 == 0:
                    logger.info("Current study id: %s", study_id)
            except Exception as ex:
                traceback.print_exc()
                logger.error("Failed to index %s: %s", study_id, ex)
    finally:
        if temp_path.exists():
            try:
                shutil.rmtree(str(temp_path))
            except Exception as ex:
                logger.warning("Failed to delete temp folder: %s, %s", temp_path, ex)


async def create_index(
    search_index_management_gateway: SearchIndexManagementGateway,
    index_name: str,
    index_mapping_file: None | str = None,
    index_settings_file: None | str = None,
    recreate_index=True,
):
    exist = await search_index_management_gateway.exists(index=index_name)
    mappings = None
    settings = None
    if index_mapping_file:
        file_content = load_json_file(index_mapping_file)
        mappings = file_content.get("mappings")
    if index_settings_file:
        file_content = load_json_file(index_settings_file)
        settings = file_content.get("settings")

    if not exist:
        await search_index_management_gateway.create_index(
            index=index_name,
            mappings=mappings,
            settings=settings,
            max_retries=2,
        )
        logger.info("%s index is created.", index_name)
    elif recreate_index:
        await search_index_management_gateway.create_index(
            index=index_name,
            mappings=mappings,
            settings=settings,
            max_retries=2,
            delete_before=True,
        )
        logger.info("%s index is recreated.", index_name)
