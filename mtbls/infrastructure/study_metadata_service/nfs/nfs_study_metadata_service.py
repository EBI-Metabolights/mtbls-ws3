import datetime
import logging
import re
import shutil
import uuid
from pathlib import Path
from typing import Any, Union

import jsonpath_ng.ext as jp
from metabolights_utils.common import CamelCaseModel
from metabolights_utils.isatab import Reader, Writer
from metabolights_utils.isatab.reader import (
    InvestigationFileReaderResult,
    IsaTableFileReaderResult,
)
from metabolights_utils.models.isa.common import IsaTableFile
from metabolights_utils.models.metabolights.model import MetabolightsStudyModel
from pydantic.alias_generators import to_camel

from mtbls.application.services.interfaces.repositories.file_object.file_object_write_repository import (  # noqa: E501
    FileObjectWriteRepository,
)
from mtbls.application.services.interfaces.repositories.study.study_read_repository import (  # noqa: E501
    StudyReadRepository,
)
from mtbls.application.services.interfaces.repositories.study_file.study_file_write_repository import (  # noqa: E501
    StudyFileRepository,
)
from mtbls.application.services.interfaces.study_metadata_service import (
    StudyMetadataService,
)
from mtbls.application.services.study_metadata_service.db_metadata_collector import (
    DefaultAsyncDbMetadataCollector,
)
from mtbls.application.services.study_metadata_service.default_study_provider import (
    DataFileIndexMetabolightsStudyProvider,
)
from mtbls.domain.entities.investigation import InvestigationItem
from mtbls.domain.exceptions.repository import (
    StudyResourceError,
    StudyResourceNotFoundError,
)
from mtbls.domain.shared.data_types import JsonPathOperation
from mtbls.domain.shared.repository.study_bucket import StudyBucket

logger = logging.getLogger(__name__)


class FileObjectStudyMetadataService(StudyMetadataService):
    def __init__(
        self,
        resource_id: str,
        study_file_repository: StudyFileRepository,
        metadata_files_object_repository: FileObjectWriteRepository,
        audit_files_object_repository: FileObjectWriteRepository,
        internal_files_object_repository: FileObjectWriteRepository,
        study_read_repository: StudyReadRepository,
        temp_path: Union[None, str] = None,
    ) -> None:
        self.db_metadata_collector = DefaultAsyncDbMetadataCollector(
            study_read_repository=study_read_repository
        )
        self.study_read_repository = study_read_repository
        self.resource_id = resource_id
        self.study_file_repository = study_file_repository
        self.metadata_files_object_repository = metadata_files_object_repository
        self.audit_files_object_repository = audit_files_object_repository
        self.internal_files_object_repository = internal_files_object_repository
        self.temp_path = temp_path if temp_path else "/tmp/study-metadata-service"
        self.transaction_id = str(uuid.uuid4())

        self.staging_path = (
            Path(self.temp_path) / Path(self.resource_id) / Path(self.transaction_id)
        )
        self.staging_path_str = str(self.staging_path)
        self.staging_path.mkdir(parents=True, exist_ok=True)
        self.operation_actions = {
            "get": self._apply_jsonpath_get_action,
            "delete": self._apply_jsonpath_delete_action,
            "insert": self._apply_jsonpath_insert_action,
            "update-object": self._apply_jsonpath_update_object_action,
            "update-string": self._apply_jsonpath_update_str_action,
            "patch": self._apply_jsonpath_patch_action,
        }

    def __enter__(self) -> StudyMetadataService:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        shutil.rmtree(self.staging_path, ignore_errors=True)
        self.transaction_id = None
        self.staging_path = None
        self.staging_path_str = None

    def get_study_metadata_path(
        self, study_id: str, file_relative_path: Union[None, str] = None
    ) -> str:
        if file_relative_path:
            file_path = self.staging_path / Path(file_relative_path)
            return str(file_path)
        return self.staging_path_str

    def exists(
        self, study_id: str, file_relative_path: Union[None, str] = None
    ) -> bool:
        if file_relative_path:
            file_path = self.staging_path / Path(file_relative_path)
            return file_path
        return self.staging_path.exists()

    async def modify_investigation_file(
        self,
        target_jsonpath: str,
        operation: JsonPathOperation,
        output_model_class: type[CamelCaseModel],
        input_data: Union[None, str, CamelCaseModel] = None,
        field_name: Union[None, str] = None,
        object_key: Union[int, None] = None,
    ) -> tuple[Union[list[str], list[CamelCaseModel]], list[int]]:
        if operation not in self.operation_actions:
            raise ValueError(f"Invalid operation: {operation}")
        action_method = self.operation_actions[operation]

        investigation: InvestigationItem = await self.load_investigation_file(
            object_key=object_key
        )
        json_model = investigation.model_dump(by_alias=True)
        expression, result, indices = await self._find_items_with_jsonpath(
            json_model, target_jsonpath
        )

        updated = await action_method(
            json_model, input_data, expression, result, indices, field_name=field_name
        )
        if updated:
            updated_investigation = InvestigationItem.model_validate(json_model)
            await self.save_investigation_file(
                updated_investigation, object_key=object_key
            )
        elif operation != "get":
            return None, []
        data = []
        if operation == "insert" and updated:
            data.append(output_model_class.model_validate(input_data))
            indices = [len(result[0]) - 1]
        else:
            for item in result:
                if issubclass(output_model_class, str):
                    data.append(item)
                else:
                    data.append(output_model_class.model_validate(item))
        return data, indices

    async def _apply_jsonpath_delete_action(
        self,
        json_model,
        input_data,
        expression,
        result,
        indices,
        field_name: Union[None, str] = None,
    ):
        if result:
            expression.filter(lambda x: True, json_model)
            return True
        return False

    async def _apply_jsonpath_insert_action(
        self,
        json_model,
        input_data,
        expression,
        result,
        indices,
        field_name: Union[None, str] = None,
    ):
        if result:
            if not isinstance(result[0], list):
                raise ValueError("Insert failed. Object is not list.")
            elif not input_data:
                raise ValueError("Insert failed. Input is not defined.")

            result[0].append(input_data.model_dump(by_alias=True))

            return True
        return False

    async def _apply_jsonpath_get_action(
        self,
        json_model,
        input_data,
        expression,
        result,
        indices,
        field_name: Union[None, str] = None,
    ):
        return False

    async def _apply_jsonpath_update_object_action(
        self,
        json_model,
        input_data,
        expression,
        result,
        indices,
        field_name: Union[None, str] = None,
    ):
        if result and isinstance(input_data, CamelCaseModel):
            source = input_data.model_dump(exclude_none=True, by_alias=True)
            self._copy_mapped_values(source, result[0], False)
            return True
        return False

    async def _apply_jsonpath_patch_action(
        self,
        json_model,
        input_data,
        expression,
        result,
        indices,
        field_name: Union[None, str] = None,
    ):
        if result and isinstance(input_data, CamelCaseModel):
            source = input_data.model_dump(
                exclude_none=True,
                exclude_defaults=True,
                exclude_unset=True,
                by_alias=True,
            )
            self._copy_mapped_values(source, result[0], True)
            return True
        return False

    async def _apply_jsonpath_update_str_action(
        self,
        json_model,
        input_data,
        expression,
        result,
        indices,
        field_name: Union[None, str] = None,
    ):
        if field_name and result and isinstance(input_data, str):
            camelcase_field_name = to_camel(field_name)
            if camelcase_field_name in result[0]:
                result[0][camelcase_field_name] = input_data
                return True
        return False

    async def _find_items_with_jsonpath(
        self,
        json_model: dict[str, Any],
        target_jsonpath: str,
    ):
        expression = jp.parse(target_jsonpath)
        search_results = expression.find(json_model)
        result = [x.value for x in search_results]
        if not result:
            return expression, [], []
        indices = []

        if search_results and hasattr(search_results[0].path, "index"):
            indices = [x.path.index for x in search_results]
        logger.debug(
            "Found %s items with jsonpath: %s in %s investigation file.",
            len(result),
            str(target_jsonpath),
            self.resource_id,
        )
        return expression, result, indices

    async def process_investigation_file(
        self,
        operation: JsonPathOperation,
        target_jsonpath: str,
        output_model_class: Union[str, type[CamelCaseModel]],
        input_data: Union[None, str, CamelCaseModel] = None,
        field_name: Union[None, str] = None,
        object_key: Union[int, None] = None,
    ) -> tuple[Union[list[str], list[CamelCaseModel]], list[int]]:
        return await self.modify_investigation_file(
            target_jsonpath=target_jsonpath,
            operation=operation,
            output_model_class=output_model_class,
            input_data=input_data,
            object_key=object_key,
            field_name=field_name,
        )

    def _copy_mapped_values(
        self, source: dict[str, Any], target: dict[str, Any], patch_only: bool = False
    ):
        for key, value in source.items():
            if patch_only:
                if key not in target or not value:
                    continue
                target[key] = value
            else:
                if key in target:
                    target[key] = value

    async def _create_metadata_files_audit_folder(
        self,
        parent_object_key: str,
        source_object_keys: Union[None, list[str]] = None,
    ) -> list[str]:
        resources = await self.metadata_files_object_repository.list(self.resource_id)

        selected_files = []
        if not source_object_keys:
            selected_files = [
                x for x in resources if re.match(r"^[isam]_.+", x.basename)
            ]
        else:
            source_set = set(source_object_keys) if source_object_keys else set()
            objects = {file.object_key for file in resources}
            not_exist_files = source_set - objects
            if not_exist_files:
                raise StudyResourceNotFoundError(
                    f"Resource not found: {not_exist_files}"
                )
            selected_files = [x for x in resources if x.object_key in source_set]
        for file in selected_files:
            target_file_path = self.staging_path / Path(file.object_key)
            await self.metadata_files_object_repository.download(
                resource_id=self.resource_id,
                object_key=file.object_key,
                target_path=str(target_file_path),
            )
            target_object_key = f"{parent_object_key.strip('/')}/{file.object_key}"
            await self.audit_files_object_repository.put_object(
                resource_id=self.resource_id,
                object_key=target_object_key,
                source_uri=f"file://{str(target_file_path)}",
                override=True,
            )

    async def load_study_model(  # noqa: PLR0913
        self,
        load_sample_file: bool = False,
        load_assay_files: bool = False,
        load_maf_files: bool = False,
        load_folder_metadata: bool = False,
        load_db_metadata: bool = False,
        samples_sheet_offset: Union[int, None] = None,
        samples_sheet_limit: Union[int, None] = None,
        assay_sheet_offset: Union[int, None] = None,
        assay_sheet_limit: Union[int, None] = None,
        assignment_sheet_offset: Union[int, None] = None,
        assignment_sheet_limit: Union[int, None] = None,
        calculate_data_folder_size: bool = False,
        calculate_metadata_size: bool = False,
    ) -> MetabolightsStudyModel:
        object_key = "i_Investigation.txt"
        object_path = Path(object_key)
        target_file_path = self.staging_path / object_path
        if not target_file_path.exists():
            await self.metadata_files_object_repository.download(
                resource_id=self.resource_id,
                object_key=object_key,
                target_path=str(target_file_path),
            )
        study_objects = await self.metadata_files_object_repository.list(
            resource_id=self.resource_id
        )

        for file in study_objects:
            if re.match(r"^[isam]_.+", file.basename):
                target_file_path = self.staging_path / Path(file.object_key)
                await self.metadata_files_object_repository.download(
                    resource_id=self.resource_id,
                    object_key=file.object_key,
                    target_path=str(target_file_path),
                )

        provider = DataFileIndexMetabolightsStudyProvider(
            resource_id=self.resource_id,
            data_file_index_file_key="DATA_FILES/data_file_index.json",
            internal_files_object_repository=self.internal_files_object_repository,
            study_read_repository=self.study_read_repository,
        )

        return await provider.load_study(
            study_id=self.resource_id,
            study_path=str(self.staging_path),
            connection=self.db_metadata_collector if load_db_metadata else None,
            load_sample_file=load_sample_file,
            load_assay_files=load_assay_files,
            load_maf_files=load_maf_files,
            load_folder_metadata=load_folder_metadata,
            samples_sheet_offset=samples_sheet_offset,
            samples_sheet_limit=samples_sheet_limit,
            assay_sheet_offset=assay_sheet_offset,
            assay_sheet_limit=assay_sheet_limit,
            assignment_sheet_offset=assignment_sheet_offset,
            assignment_sheet_limit=assignment_sheet_limit,
            calculate_data_folder_size=calculate_data_folder_size,
            calculate_metadata_size=calculate_metadata_size,
        )

    async def save_study_model(
        self,
        model: MetabolightsStudyModel,
    ) -> bool:
        save_path_str = str(self.staging_path)

        await self.save_metabolights_study_model(
            model,
            output_dir=save_path_str,
            values_in_quotation_mark=False,
        )
        isa_table_file_path = self.staging_path / Path(model.investigation_file_path)
        isa_table_file_path_str = str(isa_table_file_path)
        await self.metadata_files_object_repository.put_object(
            resource_id=self.resource_id,
            object_key=model.investigation_file_path,
            source_uri=f"file://{isa_table_file_path_str}",
            override=True,
        )
        for isa_table_files in (
            model.samples,
            model.assays,
            model.metabolite_assignments,
        ):
            for file_name in isa_table_files:
                isa_table_file_path = self.staging_path / Path(file_name)
                isa_table_file_path_str = str(isa_table_file_path)
                await self.metadata_files_object_repository.put_object(
                    resource_id=self.resource_id,
                    object_key=file_name,
                    source_uri=f"file://{isa_table_file_path_str}",
                    override=True,
                )

    async def load_investigation_file(
        self,
        object_key: Union[str, None] = None,
    ) -> InvestigationItem:
        if not object_key:
            object_key = "i_Investigation.txt"
        object_path = Path(object_key)
        target_file_path = self.staging_path / object_path
        if not target_file_path.exists():
            await self.metadata_files_object_repository.download(
                resource_id=self.resource_id,
                object_key=object_key,
                target_path=str(target_file_path),
            )
        result: InvestigationFileReaderResult = (
            Reader.get_investigation_file_reader().read(target_file_path)
        )
        return InvestigationItem.get_from_investigation(result.investigation)

    async def save_investigation_file(
        self,
        investigation: InvestigationItem,
        object_key: Union[str, None] = None,
    ):
        study = investigation.studies
        if not study:
            raise ValueError("Study not found")
        if not object_key:
            object_key = "i_Investigation.txt"

        save_path = Path(self.staging_path) / Path(object_key)
        save_path_str = str(save_path)
        inv = investigation.to_investigation()
        Writer.get_investigation_file_writer().write(
            inv,
            save_path_str,
            values_in_quotation_mark=False,
        )
        await self.metadata_files_object_repository.put_object(
            resource_id=self.resource_id,
            object_key=object_key,
            source_uri=f"file://{save_path_str}",
            override=True,
        )

    async def restore_metadata_from_snapshot(
        self, snapshot_name: str
    ) -> tuple[str, str]:
        files = await self.audit_files_object_repository.list(
            resource_id=self.resource_id,
            object_key="audit",
        )
        selected_snapshots = [
            x for x in files if Path(x.object_key).name == snapshot_name
        ]
        if not selected_snapshots:
            raise StudyResourceNotFoundError(
                self.resource_id,
                StudyBucket.AUDIT_FILES.value,
                snapshot_name,
                "Snapshot not found",
            )

        selected_snapshot = selected_snapshots[0]
        await self.create_metadata_snapshot(suffix=f"BEFORE_RESTORE_{snapshot_name}")
        try:
            resources = await self.metadata_files_object_repository.list(
                resource_id=self.resource_id
            )
            snapshot_resources = await self.audit_files_object_repository.list(
                resource_id=self.resource_id,
                object_key=selected_snapshot.object_key,
            )
            selected_files = {
                x.basename: x for x in resources if re.match(r"^[isam]_.+", x.basename)
            }
            snapshot_files = {
                x.basename: x
                for x in snapshot_resources
                if re.match(r"^[isam]_.+", x.basename)
            }
            files_to_be_deleted = set(selected_files.keys()) - set(
                snapshot_files.keys()
            )
            for file in snapshot_files.values():
                await self.study_resource_write_repository.copy_object(
                    resource_id=self.resource_id,
                    source_bucket_name=StudyBucket.AUDIT_FILES.value,
                    source_object_key=file.object_key,
                    target_bucket_name=StudyBucket.PRIVATE_METADATA_FILES.value,
                    target_object_key=Path(file.object_key).name,
                    override=True,
                )
            for file in files_to_be_deleted:
                await self.metadata_files_object_repository.delete_object(
                    resource_id=self.resource_id,
                    object_key=file,
                )
        except Exception as exc:
            raise StudyResourceError(
                self.resource_id, snapshot_name, "Restore failed"
            ) from exc

    def create_audit_folder_name(
        self,
        folder_suffix: Union[None, str] = "BACKUP",
        folder_prefix: Union[None, str] = None,
        timestamp_format: str = "%Y-%m-%d_%H-%M-%S",
    ) -> Union[None, str]:
        if not timestamp_format:
            logger.error("Invalid input parameter.")
            return None
        base = datetime.datetime.now(datetime.timezone.utc).strftime(timestamp_format)
        folder_name = f"{base}_{folder_suffix}" if folder_suffix else base
        return f"{folder_prefix}_{folder_name}" if folder_prefix else folder_name

    async def create_metadata_snapshot(
        self,
        prefix: Union[None, str] = None,
        suffix: Union[None, str] = None,
        timestamp_format: str = "%Y-%m-%d_%H-%M-%S",
    ) -> tuple[str, str]:
        folder_name = self.create_audit_folder_name(
            folder_suffix=suffix,
            folder_prefix=prefix,
            timestamp_format=timestamp_format,
        )
        parent_object_key = f"audit/{folder_name}"
        await self._create_metadata_files_audit_folder(
            parent_object_key=parent_object_key
        )
        return parent_object_key

    async def load_isa_table_file(
        self,
        object_key: str,
        offset: Union[int, None] = None,
        limit: Union[int, None] = None,
    ) -> IsaTableFile:
        object_path = Path(object_key)
        target_file_path = self.staging_path / object_path
        if not target_file_path.exists():
            await self.metadata_files_object_repository.download(
                resource_id=self.resource_id,
                object_key=object_key,
                target_path=str(target_file_path),
            )
        if object_path.name.startswith("s_"):
            reader = Reader.get_sample_file_reader(results_per_page=100000)
        elif object_path.name.startswith("a_"):
            reader = Reader.get_assay_file_reader(results_per_page=100000)
        elif object_path.name.startswith("m_"):
            reader = Reader.get_assignment_file_reader(results_per_page=100000)
        else:
            raise ValueError(f"Invalid isa table file {object_key}")

        result: IsaTableFileReaderResult = reader.read(
            target_file_path,
            offset=offset,
            limit=limit,
        )

        return result.isa_table_file

    async def save_isa_table_file(
        self, resource_id: str, isa_table_file: IsaTableFile, object_key: str
    ):
        save_path = Path(self.staging_path) / Path(object_key)
        save_path_str = str(save_path)
        await self.dump_isa_table(isa_table_file, save_path_str)
        await self.metadata_files_object_repository.put_object(
            resource_id=resource_id,
            object_key=object_key,
            source_uri=f"file://{save_path_str}",
            override=True,
        )
