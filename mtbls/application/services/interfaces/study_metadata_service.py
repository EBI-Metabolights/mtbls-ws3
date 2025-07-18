import abc
from pathlib import Path
from typing import Self, Union

from metabolights_utils.common import CamelCaseModel
from metabolights_utils.isatab import Writer
from metabolights_utils.models.isa.common import IsaTableFile
from metabolights_utils.models.isa.investigation_file import Investigation
from metabolights_utils.models.metabolights.model import MetabolightsStudyModel
from metabolights_utils.provider.study_provider import AbstractMetadataFileProvider

from mtbls.application.services.study_metadata_service.models import (
    IsaTableDataUpdates,
)
from mtbls.domain.entities.investigation import InvestigationItem
from mtbls.domain.entities.isa_table import (
    IsaTableData,
    IsaTableFileObject,
    IsaTableRow,
)
from mtbls.domain.entities.study_file import StudyFileOutput
from mtbls.domain.shared.data_types import JsonPathOperation


class StudyMetadataService(AbstractMetadataFileProvider, abc.ABC):
    def __init__(self, resource_id: str):
        self.resource_id = resource_id

    def get_resource_id(self) -> str:
        return self.resource_id

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None: ...

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
    ) -> MetabolightsStudyModel: ...

    async def save_study_model(
        self,
        model: MetabolightsStudyModel,
    ) -> bool: ...

    async def load_investigation_file(
        self,
        object_key: Union[int, None] = None,
    ) -> InvestigationItem: ...

    async def process_investigation_file(
        self,
        operation: JsonPathOperation,
        target_jsonpath: str,
        output_model_class: type[CamelCaseModel],
        input_data: Union[None, str, CamelCaseModel] = None,
        field_name: Union[None, str] = None,
        object_key: Union[int, None] = None,
    ) -> tuple[Union[list[str], list[CamelCaseModel]], list[int]]: ...

    async def update_isa_table_file(
        self,
        input_data: Union[None, list[IsaTableRow]] = None,
        object_key: Union[int, None] = None,
    ) -> tuple[Union[list[str], list[CamelCaseModel]], list[int]]: ...

    async def save_investigation_file(
        self,
        model: InvestigationItem,
        object_key: Union[int, None] = None,
    ) -> bool: ...

    async def load_isa_table_file(
        self,
        object_key: str,
        offset: Union[int, None] = None,
        limit: Union[int, None] = None,
    ) -> IsaTableData: ...

    async def list_isa_files(
        self,
    ) -> list[StudyFileOutput]: ...

    async def get_isa_table_rows(
        self,
        object_key: str,
        offset: Union[int, None] = None,
        limit: Union[int, None] = None,
    ) -> list[IsaTableRow]: ...

    async def get_isa_table_data_columns(
        self,
        object_key: str,
    ) -> IsaTableFileObject: ...

    async def update_isa_table_rows(
        self,
        object_key: str,
        updates: IsaTableDataUpdates = None,
    ) -> list[IsaTableRow]: ...

    async def save_isa_table_file(
        self, isa_table_file: IsaTableFile, object_key: str
    ) -> None: ...

    async def create_metadata_snapshot(
        self,
        prefix: Union[None, str] = None,
        suffix: Union[None, str] = None,
    ) -> tuple[str, str]: ...

    async def restore_metadata_from_snapshot(
        self, snapshot_name: str
    ) -> tuple[str, str]: ...

    async def dump_investigation_as_json(
        self,
        investigation: Investigation,
        save_path_str: str,
    ):
        with Path(save_path_str).open("w") as f:
            f.write(investigation.model_dump_json(indent=2))

    async def dump_investigation_item_as_json(
        self,
        investigation_item: InvestigationItem,
        save_path_str: str,
    ):
        with Path(save_path_str).open("w") as f:
            f.write(investigation_item.model_dump_json(indent=2, by_alias=True))

    async def dump_study_model_as_json(
        self,
        model: MetabolightsStudyModel,
        save_path_str: str,
    ):
        with Path(save_path_str).open("w") as f:
            f.write(model.model_dump_json(indent=2))

    async def dump_isa_table_as_json(
        self,
        isa_table_file: IsaTableFile,
        save_path_str: str,
    ):
        with Path(save_path_str).open("w") as f:
            f.write(isa_table_file.model_dump_json(indent=2))

    @classmethod
    async def save_metabolights_study_model(
        cls,
        mtbls_model: MetabolightsStudyModel,
        output_dir: str,
        values_in_quotation_mark: bool = False,
        investigation_module_name: Union[None, str] = None,
    ):
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        Writer.get_investigation_file_writer().write(
            mtbls_model.investigation,
            f"{output_dir}/{mtbls_model.investigation_file_path}",
            values_in_quotation_mark=values_in_quotation_mark,
            investigation_module_name=investigation_module_name,
        )
        for isa_table_files in (
            mtbls_model.samples,
            mtbls_model.assays,
            mtbls_model.metabolite_assignments,
        ):
            for isa_table_file in isa_table_files.values():
                await cls.dump_isa_table(
                    isa_table_file,
                    f"{output_dir}/{isa_table_file.file_path}",
                    values_in_quotation_mark=values_in_quotation_mark,
                )

    @classmethod
    async def dump_isa_table(
        cls,
        isa_table_file: IsaTableFile,
        file_path: str,
        values_in_quotation_mark: bool = False,
    ):
        column_order_map: dict[int, str] = {}
        column_header_map: dict[int, str] = {}
        data = isa_table_file.table.data
        for column_model in isa_table_file.table.headers:
            column_order_map[column_model.column_index] = column_model.column_name
            column_header_map[column_model.column_index] = column_model.column_header

        cls.dump_isa_table_columns(
            file_path,
            column_order_map,
            column_header_map,
            data,
            values_in_quotation_mark,
        )

    @classmethod
    async def dump_isa_table_data(
        cls,
        isa_table_data: IsaTableData,
        file_path: str,
        values_in_quotation_mark: bool = False,
    ):
        column_order_map: dict[int, str] = {}
        column_header_map: dict[int, str] = {}

        for column_model in isa_table_data.columns:
            column_order_map[column_model.column_index] = column_model.column_name
            column_header_map[column_model.column_index] = column_model.column_header
        data: dict[str, list[str]] = {}
        for row in isa_table_data.rows:
            for col, val in row.data.items():
                if col not in data:
                    data[col] = []
                data[col].append(val)

        cls.dump_isa_table_columns(
            file_path,
            column_order_map,
            column_header_map,
            data,
            values_in_quotation_mark,
        )

    @staticmethod
    def dump_isa_table_columns(
        file_path: str,
        column_order_map: dict[int, str],
        column_header_map: dict[int, str],
        data: dict[str, list[str]],
        values_in_quotation_mark: bool = False,
    ):
        with Path(file_path).open("w") as f:
            if values_in_quotation_mark:
                header = [
                    f'"{column_header_map[idx]}"'
                    for idx in range(len(column_header_map))
                ]
            else:
                header = [
                    column_header_map[idx].strip('"')
                    for idx in range(len(column_header_map))
                ]
            f.write("\t".join(header) + "\n")

            column_names = [
                column_order_map[idx] for idx in range(len(column_order_map))
            ]
            for row_idx in range(len(data[column_names[0]])):
                row = [data[column_name][row_idx] for column_name in column_names]
                for idx, cell in enumerate(row):
                    cell_value = cell if cell is not None else ""
                    if values_in_quotation_mark:
                        cell_value = f'"{cell_value}"'
                    else:
                        cell_value = cell.strip('"')
                    row[idx] = cell_value
                f.write("\t".join(row) + "\n")
