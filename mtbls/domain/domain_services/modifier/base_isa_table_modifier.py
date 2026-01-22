from typing import Union

from metabolights_utils.models.isa.common import (
    IsaTableColumn,
    IsaTableFile,
)
from metabolights_utils.models.isa.enums import ColumnsStructure
from metabolights_utils.models.isa.investigation_file import OntologyAnnotation
from metabolights_utils.models.metabolights.model import MetabolightsStudyModel
from metabolights_utils.tsv.model import (
    TsvAddColumnsAction,
    TsvUpdateColumnHeaderAction,
)

from mtbls.domain.domain_services.modifier.base_isa_modifier import BaseIsaModifier
from mtbls.domain.domain_services.modifier.base_modifier import OntologyItem
from mtbls.domain.domain_services.modifier.column_update_handler import (
    IsaTableColumnUpdateHandler,
)
from mtbls.domain.entities.validation.validation_configuration import (
    FileTemplates,
    ValidationControls,
)


class IsaTableModifier(BaseIsaModifier):
    def __init__(
        self,
        model: MetabolightsStudyModel,
        isa_table_file: IsaTableFile,
        templates: FileTemplates,
        control_lists: ValidationControls,
        max_row_number_limit: int = 10,
    ):
        super().__init__(model, templates, control_lists, isa_table_file.file_path)
        self.isa_table_file = isa_table_file
        self.header_update_actions: Union[
            None, dict[str, list[TsvUpdateColumnHeaderAction]]
        ] = {}
        self.new_header_actions: Union[None, dict[str, list[TsvAddColumnsAction]]] = {}
        self.max_row_number_limit = max_row_number_limit

    def remove_trailing_and_prefix_spaces(self):
        action: TsvUpdateColumnHeaderAction = TsvUpdateColumnHeaderAction()
        for header in self.isa_table_file.table.headers:
            column_name = header.column_name
            column_index = header.column_index
            cleaned_column_name = " ".join(
                [
                    x.strip()
                    for x in column_name.replace("\ufeff", "").strip().split()
                    if x.strip()
                ]
            )
            cleaned_column_header = " ".join(
                [
                    x.strip()
                    for x in header.column_header.replace("\ufeff", "").strip().split()
                    if x.strip()
                ]
            )

            suffix = cleaned_column_name.replace(cleaned_column_header, "")
            if "Protocol REF" in cleaned_column_header:
                cleaned_column_header = "Protocol REF"
            elif "Term Source REF" in cleaned_column_header:
                cleaned_column_header = "Term Source REF"
            elif "Term Accession Number" in cleaned_column_header:
                cleaned_column_header = "Term Accession Number"
            elif cleaned_column_header.startswith("Unit"):
                cleaned_column_header = "Unit"
            cleaned_column_name = f"{cleaned_column_header}{suffix}"
            if header.column_header != cleaned_column_header:
                action.new_headers[column_index] = cleaned_column_header
                action.current_headers[column_index] = header.column_header
                self.modifier_update(
                    source=self.isa_table_file.file_path,
                    action=f"Column header update: index [{column_index + 1}],"
                    f" header '{column_name}' -> '{cleaned_column_name}'",
                    old_value=column_name,
                    new_value=cleaned_column_name,
                )
        if action.new_headers:
            if self.isa_table_file.file_path not in self.header_update_actions:
                self.header_update_actions[self.isa_table_file.file_path] = []
            self.header_update_actions[self.isa_table_file.file_path].append(action)
        updater = IsaTableColumnUpdateHandler(isa_table_file=self.isa_table_file)
        for header in self.isa_table_file.table.headers:
            column = header.column_name
            for row, val in enumerate(self.isa_table_file.table.data[column]):
                stripped_value = val.strip().strip('"').strip("'")
                if val != stripped_value:
                    updater.update_isa_table_cell(header, val, stripped_value, row)
        updates = updater.get_isa_table_update_logs()
        if updates:
            for update in updates:
                self.modifier_update(
                    source=update.source,
                    action=update.action,
                    new_value=update.new_value,
                    old_value=update.old_value,
                )

        return self.update_logs

    def update_ontology_columns(self):
        template_type = self._identify_template_type()

        file_type = self._identify_file_type()
        structure = ColumnsStructure
        for header in self.isa_table_file.table.headers:
            if header.column_structure == structure.SINGLE_COLUMN:
                self.update_single_column(template_type, file_type, header)
            elif header.column_structure in {
                structure.ONTOLOGY_COLUMN,
                structure.SINGLE_COLUMN_AND_UNIT_ONTOLOGY,
            }:
                (
                    control_terms,
                    control_source_refs,
                    control_accessions,
                ) = self._get_header_control_terms(header, template_type, file_type)

                if header.column_structure == structure.ONTOLOGY_COLUMN:
                    term_column_index = header.column_index
                    term_source_ref_column_index = header.column_index + 1
                    term_accession_number_column_index = header.column_index + 2
                else:
                    term_column_index = header.column_index + 1
                    term_source_ref_column_index = header.column_index + 2
                    term_accession_number_column_index = header.column_index + 3
                columns = self.isa_table_file.table.columns
                data = self.isa_table_file.table.data
                term_column_name = columns[term_column_index]
                term_source_ref_column_name = columns[term_source_ref_column_index]
                term_accession_number_column_name = columns[
                    term_accession_number_column_index
                ]
                current_ontologies: dict[str, OntologyAnnotation] = {}
                updated_rows: dict[
                    str, dict[int, tuple[OntologyAnnotation, OntologyAnnotation]]
                ] = {}
                for idx, term in enumerate(data[term_column_name]):
                    onto = None
                    source_ref = data[term_source_ref_column_name][idx]
                    accession = data[term_accession_number_column_name][idx]
                    current_source_ref = source_ref
                    current_accession = accession
                    source_ref_lower = source_ref.lower()
                    accession_lower = accession.lower()
                    term_lower = term.lower()

                    if source_ref and source_ref_lower in control_source_refs:
                        current_source_ref = control_source_refs[source_ref_lower]
                    if accession and accession_lower in control_accessions:
                        current_accession = control_accessions[
                            accession_lower
                        ].term_accession_number
                    if term in current_ontologies:
                        onto = current_ontologies[term]
                    elif term_lower in control_terms:
                        key = term_lower
                        if len(control_terms[key]) == 1:
                            ontology = list(control_terms[key].values())
                            onto = ontology[0] if ontology else None
                        elif len(control_terms[key]) > 1:
                            if source_ref_lower in control_terms[key]:
                                onto = control_terms[key][source_ref_lower]
                        if onto:
                            current_ontologies[term] = onto

                    if term not in updated_rows:
                        updated_rows[term] = {}

                    if idx not in updated_rows[term]:
                        current = OntologyItem(
                            term=term,
                            term_source_ref=source_ref,
                            term_accession_number=accession,
                        )
                        updated_rows[term][idx] = (current, current.model_copy())

                    if not term_lower and accession_lower in control_accessions:
                        item: OntologyAnnotation = control_accessions[accession_lower]
                        if source_ref_lower in control_source_refs:
                            if item.term_source_ref.lower() == source_ref_lower:
                                onto = item
                        else:
                            onto = item
                    if not term_lower and source_ref_lower and not accession_lower:
                        current_source_ref = ""

                    if onto:
                        if term != onto.term:
                            cells = self.isa_table_file.table.data[term_column_name]
                            cells[idx] = onto.term
                            updated_rows[term][idx][0].term = term
                            updated_rows[term][idx][1].term = onto.term
                        if source_ref != onto.term_source_ref:
                            cells = self.isa_table_file.table.data[
                                term_source_ref_column_name
                            ]
                            cells[idx] = onto.term_source_ref
                            updated_rows[term][idx][0].term_source_ref = source_ref
                            updated_rows[term][idx][
                                1
                            ].term_source_ref = onto.term_source_ref
                        if accession != onto.term_accession_number:
                            cells = self.isa_table_file.table.data[
                                term_accession_number_column_name
                            ]
                            cells[idx] = onto.term_accession_number
                            updated_rows[term][idx][0].term_accession_number = accession
                            new_onto = updated_rows[term][idx][1]
                            new_onto.term_accession_number = onto.term_accession_number
                    else:
                        if current_source_ref != source_ref:
                            cells = self.isa_table_file.table.data[
                                term_source_ref_column_name
                            ]
                            cells[idx] = current_source_ref
                            updated_rows[term][idx][0].term_source_ref = source_ref
                            new_onto = updated_rows[term][idx][1]
                            new_onto.term_source_ref = current_source_ref
                        if current_accession != accession:
                            cells = self.isa_table_file.table.data[
                                term_accession_number_column_name
                            ]
                            cells[idx] = current_accession
                            update = updated_rows[term][idx]
                            update[0].term_accession_number = accession
                            update[1].term_accession_number = current_accession
                for term, updated_row_terms in updated_rows.items():
                    updates: dict[str, dict[str, set[int]]] = {}
                    if updated_row_terms:
                        for idx, updated_row_term in updated_row_terms.items():
                            old_str = str(updated_row_term[0])
                            new_str = str(updated_row_term[1])
                            if old_str == new_str:
                                continue
                            if old_str not in updates:
                                updates[old_str] = {}
                            if new_str not in updates[old_str]:
                                updates[old_str][new_str] = set()
                            updates[old_str][new_str].add(idx + 1)
                        for old_str, old_str_updates in updates.items():
                            for new_str, update_values in old_str_updates.items():
                                rows = list(update_values)
                                rows.sort()
                                rows_str = self.get_list_string(
                                    rows, self.max_row_number_limit
                                )
                                action = (
                                    f"Row update: Column [{header.column_index + 1}] "
                                    f"{header.column_header}, rows {rows_str}"
                                )

                                self.modifier_update(
                                    source=self.isa_table_file.file_path,
                                    action=action,
                                    old_value=old_str,
                                    new_value=new_str,
                                )

    def _get_header_control_terms(
        self, header: IsaTableColumn, technique: str, file_type: str
    ):
        structure = ColumnsStructure
        rule, terms = self.get_related_rule(file_type, technique, header.column_header)
        # terms = self.get_control_list_terms(category, header.column_header, technique)

        is_unit_column = (
            header.column_structure == structure.SINGLE_COLUMN_AND_UNIT_ONTOLOGY
        )
        _, unit_terms = self.get_related_rule(file_type, technique, "Unit")
        # unit_terms = self.get_control_list_terms("unitColumns", "Unit", None)
        unit_term_sources: dict[str, str] = {}
        unit_term_accession_numbers: dict[str, str] = {}
        unit_terms = unit_terms or {}
        terms = terms or {}
        for x in unit_terms:
            unit_term_sources.update(
                {
                    x.term_source_ref.lower(): x.term_source_ref
                    for x in unit_terms[x].values()
                }
            )
            unit_term_accession_numbers.update(
                {x.term_accession_number.lower(): x for x in unit_terms[x].values()}
            )
        control_terms = terms if not is_unit_column else unit_terms
        if not control_terms:
            return {}, {}, {}

        term_sources: dict[str, str] = {}
        term_accession_numbers: dict[str, OntologyAnnotation] = {}

        for x in control_terms:
            term_sources.update(
                {
                    term.term_source_ref.lower(): term.term_source_ref
                    for term in control_terms[x].values()
                }
            )
            term_accession_numbers.update(
                {
                    term.term_accession_number.lower(): term
                    for term in control_terms[x].values()
                }
            )

        control_source_refs = term_sources if not is_unit_column else unit_term_sources
        control_accessions = (
            term_accession_numbers
            if not is_unit_column
            else unit_term_accession_numbers
        )

        return control_terms, control_source_refs, control_accessions

    def _identify_file_type(self):
        category = ""
        if self.isa_table_file.file_path.startswith("a_"):
            category = "assay"
        elif self.isa_table_file.file_path.startswith("s_"):
            category = "sample"
        return category

    def _identify_template_type(self):
        file_type = self._identify_file_type()
        if file_type == "assay":
            technique = None
            if self.isa_table_file.file_path in self.model.assays:
                technique = self.model.assays[
                    self.isa_table_file.file_path
                ].assay_technique.name
            elif self.isa_table_file.file_path in self.model.metabolite_assignments:
                technique = self.model.metabolite_assignments[
                    self.isa_table_file.file_path
                ].assay_technique.name

            return technique
        elif file_type == "sample":
            return self.model.study_db_metadata.sample_template
        return ""

    def update_single_column(
        self,
        template_type: str,
        file_type: str,
        header: IsaTableColumn,
    ):
        # control_terms = self.get_control_list_terms(
        #     category, header.column_header, technique
        # )
        rule, control_lists = self.get_related_rule(
            file_type, template_type, header.column_header
        )
        if not control_lists:
            return
        term_column_name = header.column_name
        current_ontologies: dict[str, OntologyAnnotation] = {}
        data = self.isa_table_file.table.data
        updater = IsaTableColumnUpdateHandler(isa_table_file=self.isa_table_file)
        for idx, term in enumerate(data[term_column_name]):
            onto = None
            if term in current_ontologies:
                onto = current_ontologies[term]
            else:
                key = term.lower()
                controls = list(control_lists.get(key, {}).values())
                if controls:
                    onto: OntologyAnnotation = controls[0]
                if onto:
                    current_ontologies[key] = onto
            if onto:
                if onto.term != term:
                    updater.update_isa_table_cell(header, term, onto.term, idx)
        updates = updater.get_isa_table_update_logs()
        if updates:
            self.update_logs.extend(updates)
