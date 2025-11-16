from metabolights_utils.isa_file_utils import IsaFileUtils
from metabolights_utils.models.isa.common import IsaTableFile
from metabolights_utils.models.isa.enums import ColumnsStructure
from metabolights_utils.models.metabolights.model import MetabolightsStudyModel
from metabolights_utils.tsv.model import (
    TsvAddColumnsAction,
    TsvUpdateColumnHeaderAction,
)

from mtbls.domain.domain_services.modifier.assay_modifier import AssayFileModifier
from mtbls.domain.domain_services.modifier.base_isa_modifier import BaseIsaModifier
from mtbls.domain.domain_services.modifier.base_isa_table_modifier import (
    IsaTableModifier,
)
from mtbls.domain.domain_services.modifier.investigation_modifier import (
    InvestigationFileModifier,
)
from mtbls.domain.domain_services.modifier.maf_modifier import MafFileModifier
from mtbls.domain.domain_services.modifier.sample_modifier import SampleFileModifier
from mtbls.domain.entities.validation.validation_configuration import (
    FileTemplates,
    ValidationControls,
)
from mtbls.domain.shared.modifier import UpdateLog


class MetabolightsStudyModelModifier(BaseIsaModifier):
    def __init__(
        self,
        model: MetabolightsStudyModel,
        templates: FileTemplates,
        control_lists: ValidationControls,
    ):
        super().__init__(model, templates, control_lists, "")
        self.new_header_actions: dict[str, list[TsvAddColumnsAction]] = {}
        self.header_update_actions: dict[str, list[TsvUpdateColumnHeaderAction]] = {}
        self.investigation_modifier = InvestigationFileModifier(
            self.model, self.templates, self.control_lists
        )
        self.modifiers_map: dict[str, BaseIsaModifier] = {}
        self.modifiers_map[model.investigation_file_path] = self.investigation_modifier

    def modify(self) -> list[UpdateLog]:
        self.investigation_modifier.modify()
        sample_files: list[SampleFileModifier] = []
        for isa_table_file in self.model.samples.values():
            modifier = SampleFileModifier(
                self.model, isa_table_file, self.templates, self.control_lists
            )
            file_name = isa_table_file.file_path
            self.modifiers_map[file_name] = modifier
            modifier.modify()
            sample_files.append(modifier)

        assay_files: list[AssayFileModifier] = []
        for isa_table_file in self.model.assays.values():
            file_name = isa_table_file.file_path
            modifier = AssayFileModifier(
                self.model, isa_table_file, self.templates, self.control_lists
            )
            self.modifiers_map[file_name] = modifier
            modifier.modify()

            assay_files.append(modifier)

        maf_files: list[MafFileModifier] = []
        for isa_table_file in self.model.metabolite_assignments.values():
            file_name = isa_table_file.file_path
            modifier = MafFileModifier(
                self.model, isa_table_file, self.templates, self.control_lists
            )
            self.modifiers_map[file_name] = modifier
            modifier.modify()
            maf_files.append(modifier)

        for modifier in sample_files:
            modifier.add_factor_value_columns()

        for modifier in maf_files:
            modifier.add_maf_sample_columns()

        isa_table_files: dict[str, IsaTableFile] = {}
        for files in (
            self.model.samples,
            self.model.assays,
            self.model.metabolite_assignments,
        ):
            isa_table_files.update(files)
        for _, modifier in self.modifiers_map.items():
            if isinstance(modifier, IsaTableModifier):
                self.new_header_actions.update(modifier.new_header_actions)
                self.header_update_actions.update(modifier.header_update_actions)

        self.add_new_columns(isa_table_files)
        self.update_column_headers(isa_table_files)
        self.investigation_modifier.update_protocol_parameters()
        self.investigation_modifier.update_ontology_sources()
        for _, modifier in self.modifiers_map.items():
            self.update_logs.extend(modifier.update_logs)
        self.update_logs.sort(key=lambda x: x.source + x.action + x.old_value)
        return self.update_logs

    def update_column_headers(self, isa_table_files: dict[str, IsaTableFile]):
        remove_keys = []
        for file, actions in self.header_update_actions.items():
            updated_actions = [
                x
                for x in actions
                if isinstance(x, TsvUpdateColumnHeaderAction) and len(x.new_headers) > 0
            ]
            if updated_actions:
                self.header_update_actions[file] = updated_actions
                action = updated_actions[0]
                data = isa_table_files[file].table.data
                columns = isa_table_files[file].table.columns
                for idx, current_header in action.current_headers.items():
                    if idx in action.new_headers:
                        for item in isa_table_files[file].table.headers:
                            if (
                                item.column_index == idx
                                and item.column_header == current_header
                            ):
                                current_column_name = item.column_name
                                suffix = item.column_name.replace(
                                    item.column_header, ""
                                )
                                new_header = action.new_headers[idx]
                                new_column_name = f"{new_header}{suffix}"
                                item.column_search_pattern = (
                                    item.column_search_pattern.replace(
                                        current_header, new_header
                                    )
                                )

                                item.column_name = new_column_name
                                item.column_header = new_header
                                if current_column_name in data:
                                    data[new_column_name] = data[current_column_name]
                                    del data[current_column_name]
                                if len(columns) > item.column_index:
                                    columns[item.column_index] = new_column_name

            else:
                remove_keys.append(file)
        for key in remove_keys:
            del self.header_update_actions[key]

    def add_new_columns(self, isa_table_files: dict[str, IsaTableFile]):
        additional_colums = {"Unit", "Term Source REF", "Term Accession Number"}
        remove_keys = []

        for file, actions in self.new_header_actions.items():
            if file not in isa_table_files:
                continue
            updated_actions = [
                x
                for x in actions
                if isinstance(x, TsvAddColumnsAction) and len(x.columns) > 0
            ]
            if not updated_actions:
                remove_keys.append(file)
                continue

            self.new_header_actions[file] = updated_actions
            action = updated_actions[0]
            sorted_header_actions = [
                (x, y)
                for x, y in action.columns.items()
                if y.header_name not in additional_colums
            ]
            sorted_header_actions.sort(key=lambda x: x[0])
            inserted_columns = 0
            for idx, column in sorted_header_actions:
                structure = ColumnsStructure.SINGLE_COLUMN

                if idx + 1 in action.columns:
                    next_column = action.columns[idx + 1]
                    if next_column.header_name == "Unit":
                        structure = ColumnsStructure.SINGLE_COLUMN_AND_UNIT_ONTOLOGY
                    elif next_column.header_name == "Term Source REF":
                        structure = ColumnsStructure.ONTOLOGY_COLUMN

                default_value = self.get_default_value(action, idx)

                new_column_names, _ = IsaFileUtils.add_isa_table_columns(
                    isa_table_files[file].table,
                    column.header_name,
                    column_structure=structure,
                    new_column_index=idx + inserted_columns,
                    default_value=default_value,
                )

                if structure == ColumnsStructure.SINGLE_COLUMN_AND_UNIT_ONTOLOGY:
                    inserted_columns += 4
                elif structure == ColumnsStructure.ONTOLOGY_COLUMN:
                    inserted_columns += 3
                else:
                    inserted_columns += 1
                if default_value:
                    data = isa_table_files[file].table.data
                    row_count = len(data[new_column_names[0]])
                    data[new_column_names[0]] = default_value * row_count

        for key in remove_keys:
            del self.new_header_actions[key]

    def get_default_value(self, action: TsvAddColumnsAction, idx: int):
        default_value = None
        if idx in action.cell_default_values:
            default_value = action.cell_default_values[idx]
            if not default_value:
                default_value = None
        return default_value
