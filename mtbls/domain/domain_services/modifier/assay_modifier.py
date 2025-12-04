import copy
import logging
import re
from pathlib import Path
from typing import Dict, Tuple

from metabolights_utils.models.isa.assay_file import AssayFile
from metabolights_utils.models.isa.common import IsaTableColumn, IsaTableFile
from metabolights_utils.models.metabolights.model import MetabolightsStudyModel

from mtbls.domain.domain_services.modifier.base_isa_table_modifier import (
    IsaTableModifier,
)
from mtbls.domain.shared.modifier import UpdateLog

logger = logging.getLogger(__name__)


class AssayFileModifier(IsaTableModifier):
    common_protocols = {
        "Data transformation": {"Normalization Name", "Derived Spectral Data File"},
        "Metabolite identification": {
            "Data Transformation Name",
            "Metabolite Assignment File",
        },
    }

    def __init__(
        self,
        model: MetabolightsStudyModel,
        isa_table_file: IsaTableFile,
        templates: dict,
        control_lists: dict,
        max_row_number_limit: int = 10,
    ):
        super().__init__(
            model, isa_table_file, templates, control_lists, max_row_number_limit
        )

    def modify(self) -> list[UpdateLog]:
        self.update_from_parser_messages()
        self.remove_trailing_and_prefix_spaces()
        self.update_ontology_columns()

        self.rule_f_400_090_001_02()
        self.rule_a_200_090_004_01()
        self.update_scan_polarity()
        self.update_protocol_ref_columns()
        return self.update_logs

    def update_protocol_ref_columns(self):
        if (
            not self.model.investigation.studies
            or not self.model.investigation.studies[0]
        ):
            return

        protocols: Dict[str, set[str]] = {}
        for protocol in self.model.investigation.studies[0].study_protocols.protocols:
            parameters = {x.term for x in protocol.parameters}
            protocols[protocol.name] = parameters

        for file_name in self.model.assays:
            protocol_ref_columns = self.get_isa_table_protocol_ref_columns(file_name)
            protocol_ref_values = self.get_protocol_ref_values(
                protocols, protocol_ref_columns
            )
            data = self.model.assays[file_name].table.data

            for protocol_ref, new_val in protocol_ref_values.items():
                old_values: set[str] = set()
                for idx, val in enumerate(data[protocol_ref]):
                    if val != new_val:
                        old_values.add(f"'{val}'")
                        data[protocol_ref][idx] = new_val
                if old_values:
                    limit = self.max_row_number_limit

                    old_values_str = self.get_list_string(list(old_values), limit)
                    self.modifier_update(
                        source=file_name,
                        action=f"Protocol REF [column index: {protocol_ref_columns[protocol_ref][0] + 1}] values are updated.",  # noqa: E501
                        old_value=old_values_str,
                        new_value=new_val,
                    )

    def get_protocol_ref_values(self, protocols, protocol_ref_columns):
        protocol_ref_values: Dict[str, str] = {}
        for protocol_ref in protocol_ref_columns:
            params_in_assay = protocol_ref_columns[protocol_ref][1]
            if params_in_assay:
                for protocol_dict in (protocols, self.common_protocols):
                    for protocol_name, params in protocol_dict.items():
                        if (
                            params
                            and len(params_in_assay) == len(params)
                            and len(params.intersection(params_in_assay)) == len(params)
                        ):
                            protocol_ref_values[protocol_ref] = protocol_name
                            break
                    if protocol_ref in protocol_ref_values:
                        break
        return protocol_ref_values

    def get_isa_table_protocol_ref_columns(self, file_name: str):
        protocol_ref_columns: Dict[str, Tuple[int, set[str]]] = {}
        if file_name not in self.model.assays:
            return protocol_ref_columns
        protocol_ref_column_name = "protocol ref"
        current_protocol_ref = None
        for header in self.model.assays[file_name].table.headers:
            if protocol_ref_column_name == header.column_header.lower().strip():
                current_protocol_ref = header.column_name
                if header.column_name not in protocol_ref_columns:
                    protocol_ref_columns[header.column_name] = (
                        header.column_index,
                        set(),
                    )
            else:
                result = re.search(self.parameter_value_pattern, header.column_name)
                parameter_value = ""
                if result and result.groups():
                    parameter_value = result.groups()[0]
                else:
                    for common in self.common_protocols:
                        if header.column_header in self.common_protocols[common]:
                            parameter_value = header.column_header
                if current_protocol_ref and parameter_value:
                    protocol_ref_columns[current_protocol_ref][1].add(parameter_value)

        return protocol_ref_columns

    def rule_f_400_090_001_02(self):
        content = []
        if (
            self.model.study_folder_metadata.files
            or self.model.study_folder_metadata.folders
        ):
            folders = self.model.study_folder_metadata.folders
            content = list(folders)
            files = self.model.study_folder_metadata.files
            content.extend(files)

        all_files_in_lowercase = {x.lower(): x for x in content}
        all_files_in_case_sensitive = set(content)
        is_file_names_unique = False
        if len(all_files_in_lowercase) == len(content):
            is_file_names_unique = True
        file_name = self.file_path
        files_columns = self.get_valid_data_file_columns(file_name)
        for header in files_columns.values():
            data = self.model.assays[file_name].table.data
            rows = data[header.column_name]
            updates = []
            for idx, item in enumerate(rows):
                if not item or not item.strip():
                    continue
                update = None
                initial_value = item
                item_value = item.strip()
                if not item.startswith("FILES/"):
                    new_relative_path = str(Path("FILES") / Path(item_value.strip("/")))
                    update = (initial_value, new_relative_path)
                    rows[idx] = new_relative_path
                assay_file_ref = rows[idx]

                if (
                    is_file_names_unique
                    and assay_file_ref not in all_files_in_case_sensitive
                ):
                    lower_assay_file_ref = assay_file_ref.lower().strip("/")
                    files_prefixed_assay_file_ref = f"files/{lower_assay_file_ref}"

                    if (
                        assay_file_ref.lower() in all_files_in_lowercase
                        or files_prefixed_assay_file_ref in all_files_in_lowercase
                    ):
                        new_relative_path = all_files_in_lowercase[
                            assay_file_ref.lower()
                        ]
                        update = (initial_value, new_relative_path)
                        rows[idx] = new_relative_path
                if update:
                    updates.append(update)
            if updates:
                limit = self.max_row_number_limit
                old_values = self.get_list_string(
                    [f"'{x[0]}'" for x in updates], limit=limit
                )
                new_values = self.get_list_string(
                    [f"'{x[1]}'" for x in updates], limit=limit
                )

                self.modifier_update(
                    source=file_name,
                    action=f"Update assay file reference on column {header.column_header} (case sensitive)",  # noqa: E501
                    old_value=old_values,
                    new_value=new_values,
                )

    def get_valid_data_file_columns(self, file_name: str):
        files_columns: Dict[str, IsaTableColumn] = {}
        if file_name not in self.model.assays:
            return files_columns
        for header in self.model.assays[file_name].table.headers:
            if " Data File" in header.column_header:
                data = self.model.assays[file_name].table.data
                if header.column_name not in data:
                    logger.warning(
                        "%s is not found on data column %s",
                        header.column_name,
                        file_name,
                    )
                    continue
                files_columns[header.column_name] = header
        return files_columns

    def update_scan_polarity(self):
        assay_file: AssayFile = self.isa_table_file

        if assay_file.table.data:
            scan_polarity_name = "Parameter Value[Scan polarity]"

            data = assay_file.table.data
            if scan_polarity_name in data:
                old_values: set[str] = set()
                updates: list[str] = []
                for idx, val in enumerate(data[scan_polarity_name]):
                    if (
                        val
                        and val.lower().strip()
                        and val.lower().strip()
                        not in {"positive", "negative", "alternating"}
                    ):
                        new_val = ""
                        if val.strip().lower().startswith("neg"):
                            new_val = "negative"
                        elif val.strip().lower().startswith("pos"):
                            new_val = "positive"
                        elif val.strip().lower().startswith("alt"):
                            new_val = "alternating"
                        old_values.add(val)
                        updates.append(f"Row {idx + 1}: '{val}' -> '{new_val}'")
                if updates:
                    old_values_str = self.get_list_string(
                        list(old_values), self.max_row_number_limit
                    )
                    updates_str = self.get_list_string(
                        updates, self.max_row_number_limit
                    )

                    self.modifier_update(
                        source=assay_file.file_path,
                        action=f"{scan_polarity_name} column values are updated.",
                        old_value=old_values_str,
                        new_value=updates_str,
                    )

    def rule_a_200_090_004_01(self):
        for file in self.model.assays:
            assay_file: AssayFile = self.model.assays[file]
            if assay_file.table.data:
                names = [
                    "Data Transformation Name",
                    "Extract Name",
                    "MS Assay Name",
                    "NMR Assay Name",
                    "Normalization Name",
                ]
                sample_name_column = "Sample Name"
                valid_sample_name_column = False
                sample_names = []
                if sample_name_column in assay_file.table.data:
                    sample_names = assay_file.table.data[sample_name_column]
                    unique_names = {x.strip() for x in sample_names if x.strip()}
                    if sample_names and len(unique_names) == len(sample_names):
                        valid_sample_name_column = True
                if not valid_sample_name_column or not sample_names:
                    return
                sample_name_str = self.get_list_string(
                    sample_names, self.max_row_number_limit
                )
                for column_name in names:
                    if column_name in assay_file.table.data:
                        empty = True
                        for cell in assay_file.table.data[column_name]:
                            if cell and cell.strip():
                                empty = False
                                break
                        if empty:
                            new_values = copy.deepcopy(sample_names)
                            assay_file.table.data[column_name] = new_values
                            self.modifier_update(
                                source=assay_file.file_path,
                                action=f"{column_name} column values are filled from Sample Name column values.",  # noqa: E501
                                old_value="",
                                new_value=sample_name_str,
                            )
