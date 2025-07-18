from typing import Dict, Union

from metabolights_utils.models.isa.common import IsaTableFile
from metabolights_utils.models.metabolights.model import MetabolightsStudyModel
from metabolights_utils.tsv.model import TsvAddColumnsAction, TsvColumnData

from mtbls.domain.domain_services.modifier.base_isa_table_modifier import (
    IsaTableModifier,
)
from mtbls.domain.shared.modifier import UpdateLog


class MafFileModifier(IsaTableModifier):
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
        return self.update_logs

    def add_maf_sample_columns(self) -> None:
        sample_column_validation: Dict[str, bool] = {}
        maf_column_validation: Dict[str, bool] = {}
        maf_file_references: Dict[str, set[str]] = {}
        if not self.model.investigation.studies:
            return
        sample_file = self.model.investigation.studies[0].file_name
        study_sample_names = set()
        ordered_sample_names = []
        if not sample_file or sample_file not in self.model.samples:
            return
        sample_table = self.model.samples[sample_file]

        study_sample_names = set(sample_table.sample_names)

        if "Sample Name" not in sample_table.table.data:
            return
        ordered_sample_names = sample_table.table.data["Sample Name"]

        if len(sample_table.sample_names) != len(ordered_sample_names):
            return
        maf_column_name = "Metabolite Assignment File"
        for assay_filename, assay in self.model.assays.items():
            if (
                not assay.table.data
                or "Sample Name" not in assay.table.data
                or maf_column_name not in assay.table.data
            ):
                continue

            sample_column_validation[assay_filename] = False
            maf_column_validation[assay_filename] = False
            assay_sample_names = {x for x in assay.sample_names if x}
            if len(assay_sample_names) == len(assay.table.data["Sample Name"]):
                diff = assay_sample_names - study_sample_names
                if not diff:
                    sample_column_validation[assay_filename] = True

            maf_values = list({x for x in assay.table.data[maf_column_name]})
            if (
                len(maf_values) == 1
                and assay.referenced_assignment_files
                and assay.referenced_assignment_files[0] == maf_values[0]
            ):
                maf_column_validation[assay_filename] = True

            for maf in assay.referenced_assignment_files:
                if maf not in maf_file_references:
                    maf_file_references[maf] = set()
                maf_file_references[maf].add(assay_filename)
        maf_filename = self.file_path
        if maf_filename not in maf_file_references:
            return
        assay_lists = maf_file_references[self.file_path]
        if not assay_lists or maf_filename not in self.model.metabolite_assignments:
            return

        valid = True
        for assay_filename in assay_lists:
            if (
                not sample_column_validation[assay_filename]
                or not maf_column_validation[assay_filename]
            ):
                valid = False
                break
        if not valid:
            return

        maf_column_names = {
            x.column_header
            for x in self.model.metabolite_assignments[maf_filename].table.headers
        }
        sample_names = []
        unique_sample_names = set()
        for assay_filename in assay_lists:
            assay = self.model.assays[assay_filename]
            for val in assay.sample_names:
                if val and val not in unique_sample_names:
                    unique_sample_names.add(val)
                    sample_names.append(val)
        assay_names = []
        unique_assay_names = set()
        for assay_filename in assay_lists:
            assay = self.model.assays[assay_filename]
            for header in assay.table.headers:
                if header.column_header.strip().endswith(" Assay Name"):
                    for val in self.model.assays[assay_filename].table.data[
                        header.column_name
                    ]:
                        if val and val not in unique_assay_names:
                            unique_assay_names.add(val)
                            assay_names.append(val)
        assay_names_diff = unique_assay_names - unique_sample_names
        assay_names_referenced = False
        for item in assay_names_diff:
            if item in maf_column_names:
                assay_names_referenced = True
                break

        if assay_names_referenced:
            return

        unreferenced_sample_names = unique_sample_names - maf_column_names
        if not unreferenced_sample_names:
            return
        new_columns = [
            x for x in ordered_sample_names if x in unreferenced_sample_names
        ]
        action: Union[None, TsvAddColumnsAction] = None
        if maf_filename not in self.new_header_actions:
            action = TsvAddColumnsAction()
            self.new_header_actions[maf_filename] = [action]

        action = self.new_header_actions[maf_filename][0]
        table = self.model.metabolite_assignments[maf_filename].table
        last_index = len(table.columns) + len(action.columns) - 1
        for unreferenced_data in new_columns:
            last_index += 1
            action.columns[last_index] = TsvColumnData(header_name=unreferenced_data)
        new_columns = ", ".join(new_columns)
        self.modifier_update(
            source=maf_filename,
            action="Sample name columns referenced in assay files are added.",
            old_value="",
            new_value=new_columns,
        )
