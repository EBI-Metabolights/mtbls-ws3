import re
from typing import OrderedDict, Union

from metabolights_utils.models.isa.common import IsaTable, IsaTableFile
from metabolights_utils.models.isa.enums import ColumnsStructure
from metabolights_utils.models.isa.investigation_file import Factor, Study
from metabolights_utils.models.metabolights.model import MetabolightsStudyModel
from metabolights_utils.tsv.model import (
    TsvAddColumnsAction,
    TsvColumnData,
    TsvUpdateColumnHeaderAction,
)

from mtbls.domain.domain_services.modifier.base_isa_modifier import BaseIsaModifier
from mtbls.domain.domain_services.modifier.base_isa_table_modifier import (
    IsaTableModifier,
)
from mtbls.domain.shared.modifier import UpdateLog


class SampleFileModifier(IsaTableModifier):
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
        self.update_sample_factor_values()
        return self.update_logs

    def update_sample_factor_values(self):
        action: Union[None, TsvUpdateColumnHeaderAction] = None
        if self.isa_table_file.file_path in self.header_update_actions:
            if self.header_update_actions[self.isa_table_file.file_path]:
                action = self.header_update_actions[self.isa_table_file.file_path][0]
        if not action:
            action = TsvUpdateColumnHeaderAction()

        for header in self.isa_table_file.table.headers:
            if header.column_structure not in (
                ColumnsStructure.ONTOLOGY_COLUMN,
                ColumnsStructure.SINGLE_COLUMN_AND_UNIT_ONTOLOGY,
            ):
                continue
            result = re.search(self.factor_value_pattern, header.column_name)
            factor_value = ""
            if result and result.groups():
                factor_value = result.groups()[0]
            if factor_value:
                column_header = header.column_header
                column_index = header.column_index

                cleaned_factor_value = self.first_character_uppercase(factor_value)
                cleaned_column_header = f"Factor Value[{cleaned_factor_value}]"
                if header.column_header != cleaned_column_header:
                    action.new_headers[column_index] = cleaned_column_header
                    action.current_headers[column_index] = column_header
                    self.modifier_update(
                        source=self.isa_table_file.file_path,
                        action=f"Header updated. Column index [{column_index + 1}]",
                        old_value=column_header,
                        new_value=cleaned_column_header,
                    )

        if action.new_headers:
            if self.isa_table_file.file_path not in self.header_update_actions:
                self.header_update_actions[self.isa_table_file.file_path] = []
            if not self.header_update_actions[self.isa_table_file.file_path]:
                self.header_update_actions[self.isa_table_file.file_path].append(action)

    def add_factor_value_columns(self) -> list[str]:
        if not self.model.investigation.studies:
            return
        study = self.model.investigation.studies[0]
        if study.file_name != self.file_path:
            return

        study_factors = self.get_study_factors(study)

        sample_sheet_factors = set()
        action: Union[None, TsvAddColumnsAction] = None
        if study.file_name not in self.new_header_actions:
            action = TsvAddColumnsAction()
            self.new_header_actions[study.file_name] = [action]

        action = self.new_header_actions[study.file_name][0]

        if study.file_name in self.model.samples:
            table = self.model.samples[study.file_name].table
            sample_sheet_factors = self.get_sample_sheet_factors(table)
        for name, factor in study_factors.items():
            if name not in sample_sheet_factors:
                last_index = len(table.columns) + len(action.columns)

                self.add_new_action_for_factor_values_columns(
                    action, name, factor, last_index
                )
                self.modifier_update(
                    source=study.file_name,
                    action=f"A new Factor Value for factor {name}: Factor Value[{name}]",  # noqa: E501
                    old_value="",
                    new_value=f"Factor Value[{name}]",
                )

        if len(action.columns) == 0:
            del self.new_header_actions[study.file_name]

    def add_new_action_for_factor_values_columns(
        self,
        action: TsvAddColumnsAction,
        factor_name: str,
        factor: Factor,
        last_index: int,
    ):
        action.columns[last_index] = TsvColumnData(
            header_name=f"Factor Value[{factor_name}]"
        )
        if factor.type.term_source_ref.lower() == "uo":
            action.columns[last_index + 1] = TsvColumnData(header_name="Unit")
            action.columns[last_index + 2] = TsvColumnData(
                header_name="Term Source REF"
            )
            action.columns[last_index + 3] = TsvColumnData(
                header_name="Term Accession Number"
            )
        else:
            action.columns[last_index + 1] = TsvColumnData(
                header_name="Term Source REF"
            )
            action.columns[last_index + 2] = TsvColumnData(
                header_name="Term Accession Number"
            )

    def get_study_factors(self, study: Study):
        study_factors: OrderedDict[str, Factor] = OrderedDict()
        for factor in study.study_factors.factors:
            new_factor_name = SampleFileModifier.first_character_uppercase(factor.name)
            if new_factor_name not in study_factors:
                study_factors[new_factor_name] = factor
        return study_factors

    def get_sample_sheet_factors(self, table: IsaTable):
        sample_sheet_factors = set()
        for header in table.headers:
            if header.column_structure not in (
                ColumnsStructure.ONTOLOGY_COLUMN,
                ColumnsStructure.SINGLE_COLUMN_AND_UNIT_ONTOLOGY,
            ):
                continue
            result = re.search(BaseIsaModifier.factor_value_pattern, header.column_name)
            factor = ""
            if result and result.groups():
                factor = result.groups()[0]
            if factor:
                cleaned_factor_name = SampleFileModifier.first_character_uppercase(
                    factor
                )
                sample_sheet_factors.add(cleaned_factor_name)
        return sample_sheet_factors
