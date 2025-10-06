import logging
import re
from typing import Any, Dict

from metabolights_utils.models.isa.assay_file import AssayFile
from metabolights_utils.models.isa.common import IsaTableColumn, IsaTableFile
from metabolights_utils.models.isa.enums import ColumnsStructure
from metabolights_utils.models.isa.investigation_file import (
    Assay,
    Factor,
    IsaAbstractModel,
    OntologyAnnotation,
    OntologySourceReference,
    Person,
    Study,
)
from metabolights_utils.models.metabolights.model import MetabolightsStudyModel
from metabolights_utils.provider.utils import assay_technique_labels, assay_techniques
from pydantic import BaseModel

from mtbls.domain.domain_services.modifier.base_isa_modifier import BaseIsaModifier
from mtbls.domain.domain_services.modifier.base_modifier import OntologyItem
from mtbls.domain.shared.modifier import UpdateLog

logger = logging.getLogger(__name__)


class InvestigationFileModifier(BaseIsaModifier):
    def __init__(
        self,
        model: MetabolightsStudyModel,
        templates: dict,
        control_lists: dict,
    ):
        super().__init__(model, templates, control_lists, model.investigation_file_path)

        self.db_metadata = model.study_db_metadata
        self.update_methods = [
            self.rule___100_300_001_10,
            self.rule_i_100_340_009_01,
            self.rule_i_100_000_000_00,
            self.update_study_factor_names,
            self.update_ontologies,
            self.rule_i_100_200_003_01,
            self.rule_i_100_200_004_01,
            self.rule_i_100_300_001_01,
            self.rule_i_100_300_001_02,
            self.rule_i_100_300_002_01,
            self.rule_i_100_300_003_01,
            self.rule_i_100_300_003_02,
            self.rule_i_100_300_005_01,
            self.rule_i_100_300_006_01,
            self.rule_i_100_320_002_01,
            self.rule_i_100_320_007_01,
            self.rule_i_100_350_007_01,
            self.rule_i_100_360_004_01,
            self.update_assay_defaults,
        ]

    def modify(self) -> list[UpdateLog]:
        self.update_from_parser_messages()
        for method in self.update_methods:
            method()
        return self.update_logs

    def rule___100_300_001_10(self):
        investigation = self.model.investigation
        valid_assays = []
        if investigation.studies and investigation.studies[0]:
            study = investigation.studies[0]
            for assay in study.study_assays.assays:
                if assay.file_name:
                    valid_assays.append(assay)
            if len(valid_assays) != len(study.study_assays.assays):
                study.study_assays.assays = valid_assays
                self.modifier_update(
                    source=self.model.investigation_file_path,
                    action="Empty assay definition is removed.",
                    old_value="",
                    new_value="",
                )
        if "" in self.model.assays:
            del self.model.assays[""]

    def update_isa_table_ontology_sources(
        self, isa_table: IsaTableFile, ontology_sources: set[str]
    ):
        if not isa_table:
            return
        ontology_columns = {}
        for header in isa_table.table.headers:
            self.update_column_term_sources(isa_table, header, ontology_columns)

        for column_name, term_source_column_name in ontology_columns.items():
            if (
                column_name in isa_table.table.data
                and term_source_column_name in isa_table.table.data
            ):
                term_source_data = isa_table.table.data[term_source_column_name]
                for idx, val in enumerate(isa_table.table.data[column_name]):
                    if val and len(term_source_data) > idx:
                        if term_source_data[idx]:
                            ontology_sources.add(term_source_data[idx])

    def update_column_term_sources(
        self,
        isa_table: IsaTableFile,
        header: IsaTableColumn,
        ontology_columns: dict[str, str],
    ):
        if header.column_structure == ColumnsStructure.ONTOLOGY_COLUMN:
            term_source_column_name = ""
            if len(isa_table.table.columns) > header.column_index + 1:
                term_source_column_name = isa_table.table.columns[
                    header.column_index + 1
                ]
            if term_source_column_name:
                ontology_columns[header.column_name] = term_source_column_name
        elif (
            header.column_structure == ColumnsStructure.SINGLE_COLUMN_AND_UNIT_ONTOLOGY
        ):
            unit_column_name = ""
            if len(isa_table.table.columns) > header.column_index + 1:
                unit_column_name = isa_table.table.columns[header.column_index + 1]
            term_source_column_name = ""
            if len(isa_table.table.columns) > header.column_index + 2:
                term_source_column_name = isa_table.table.columns[
                    header.column_index + 2
                ]
            if term_source_column_name and unit_column_name:
                ontology_columns[unit_column_name] = term_source_column_name
        return ontology_columns

    def update_ontology_sources(self):
        referenced_ontology_sources: set[str] = set()
        self.update_ontology_source_set(
            [self.model.investigation], referenced_ontology_sources
        )
        for files in (self.model.assays, self.model.samples):
            for isa_table_file in files.values():
                self.update_isa_table_ontology_sources(
                    isa_table_file, referenced_ontology_sources
                )
        current_ontology_source_references: dict[str, OntologySourceReference] = {}
        remove_list: list[OntologySourceReference] = []
        references = self.model.investigation.ontology_source_references.references
        for source_reference in references:
            val = source_reference.source_name.strip()
            if val:
                if val not in current_ontology_source_references:
                    current_ontology_source_references[val] = source_reference
                else:
                    remove_list.append(source_reference)
            else:
                remove_list.append(source_reference)
        current_set = set(current_ontology_source_references.keys())
        unreferenced_set = current_set - referenced_ontology_sources
        missing_set = referenced_ontology_sources - current_set
        exist_set = referenced_ontology_sources.intersection(current_set)
        if unreferenced_set:
            remove_list.extend(
                [current_ontology_source_references[x] for x in unreferenced_set]
            )

        if remove_list:
            removed_items = ", ".join({f"'{x.source_name}'" for x in remove_list})
            for item in remove_list:
                references.remove(item)
            logger.debug(
                "%s: Ontology Source Reference names are removed: %s ",
                self.model.investigation_file_path,
                removed_items,
            )
            self.modifier_update(
                source=self.model.investigation_file_path,
                action="Ontology Source Reference names are removed."
                f" Removed terms: {removed_items}",
                old_value=removed_items,
                new_value="",
            )

        self.update_ontology_sources_from_templates(
            current_ontology_source_references, references, missing_set, exist_set
        )
        order = ", ".join([x.source_name for x in references])
        new_list = references.copy()
        new_list.sort(key=lambda x: x.source_name)
        new_order = ", ".join([x.source_name for x in new_list])
        if order != new_order:
            self.modifier_update(
                source=self.model.investigation_file_path,
                action="Ontology source reference list is ordered.",
                old_value=order,
                new_value=new_order,
            )
            references.sort(key=lambda x: x.source_name)

    def update_ontology_sources_from_templates(
        self, current_ontology_source_references, references, missing_set, exist_set
    ):
        templates = {}
        if self.templates and "ontologySourceReferenceTemplates" in self.templates:
            for item in self.templates["ontologySourceReferenceTemplates"]:
                if "sourceName" in item:
                    name = item["sourceName"]
                    templates[name] = OntologySourceReference.model_validate(
                        item, from_attributes=True
                    )

        if exist_set:
            for ref in exist_set:
                if ref in templates:
                    template = templates[ref]
                    self.override_ontology_source(
                        source=template,
                        target=current_ontology_source_references[ref],
                    )
        if missing_set:
            missing_list = list(missing_set)
            missing_list.sort()
            for ref in missing_set:
                template: OntologySourceReference = (
                    templates[ref]
                    if ref in templates
                    else OntologySourceReference(source_name=ref)
                )

                self.modifier_update(
                    source=self.model.investigation_file_path,
                    action=f"New ontology source reference is added: {ref}",
                    old_value="",
                    new_value=ref,
                )
                references.append(template)

    def update_study_factor_names(self):
        if self.model.investigation.studies:
            study = self.model.investigation.studies[0]
            study_factors = set()
            remove_list: list[Factor] = []
            new_factor_names = set()
            factors = study.study_factors.factors
            old_values = ", ".join([x.name for x in factors])
            for factor in study.study_factors.factors:
                if not factor.name.strip() and not factor.type.term.strip():
                    remove_list.append(factor)
                new_factor_name = self.first_character_uppercase(factor.name)
                if new_factor_name not in new_factor_names:
                    new_factor_names.add(new_factor_name)
                    self.modifier_update(
                        source=self.model.investigation_file_path,
                        old_value=factor.name,
                        new_value=new_factor_name,
                        action="Factor name is updated",
                    )
                    factor.name = new_factor_name
                else:
                    remove_list.append(factor)
                study_factors.add(new_factor_name)
            for item in remove_list:
                study.study_factors.factors.remove(item)
            if remove_list:
                new_values = ", ".join([x.name for x in factors])
                self.modifier_update(
                    source=self.model.investigation_file_path,
                    action="Empty and duplicate study factor definitions are removed.",
                    old_value=old_values,
                    new_value=new_values,
                )

            sample_sheet_factors = []

            if study.file_name in self.model.samples:
                table = self.model.samples[study.file_name].table
                for header in table.headers:
                    is_factor_column = header.column_header.strip().startswith(
                        "Factor Value["
                    )
                    if is_factor_column:
                        factor = (
                            header.column_header.strip()
                            .replace("Factor Value[", "")
                            .replace("]", "")
                        )
                        cleaned_factor_name = self.first_character_uppercase(factor)
                        sample_sheet_factors.append(cleaned_factor_name)
            for name in sample_sheet_factors:
                if name not in study_factors:
                    self.modifier_update(
                        source=self.model.investigation_file_path,
                        action=f"Factor Value in the sample file "
                        f"is added as a new study factor: {name}",
                        old_value="",
                        new_value=name,
                    )
                    new_factor = Factor(name=name, type=OntologyAnnotation(term=name))
                    study.study_factors.factors.append(new_factor)

    def update_protocol_parameters(self):
        default_protocols = self.get_study_default_protocol_parameters()
        if not default_protocols:
            return
        all_protocol_parameters: Dict[str, set[str]] = {}
        for file in self.model.assays:
            assay_file: AssayFile = self.model.assays[file]
            protocols = self.get_protocol_parameters_in_assay(assay_file)
            if not protocols:
                return
            for column_name in protocols:
                _, name, params = protocols[column_name]

                if name not in all_protocol_parameters:
                    all_protocol_parameters[name] = set()
                if params:
                    all_protocol_parameters[name] = all_protocol_parameters[name].union(
                        params
                    )

        study_protocols = {
            x.name: x
            for x in self.model.investigation.studies[0].study_protocols.protocols
        }
        unique_params = {}
        for name, protocol in study_protocols.items():
            unique_params[name] = {}
            removed_params: list[OntologyAnnotation] = []
            for x in protocol.parameters:
                if not x.term.strip() or x.term.strip() in unique_params[name]:
                    removed_params.append(x)
                else:
                    unique_params[name][x.term.strip()] = x
            for item in removed_params:
                protocol.parameters.remove(item)
            if removed_params:
                msg = (
                    f"Invalid and duplicate parameters are "
                    f"removed from protocol '{name}'"
                )
                self.modifier_update(
                    source=self.model.investigation_file_path,
                    action=f"{msg} {name}: "
                    f"{', '.join([x.term for x in removed_params])}",
                    old_value=", ".join(
                        [
                            f"{idx + 1}: '{x.term}'"
                            for idx, x in enumerate(removed_params)
                        ]
                    ),
                    new_value="",
                )

        for name, protocol_params in all_protocol_parameters.items():
            if name in study_protocols:
                study_protocol_params = {
                    x.term for x in study_protocols[name].parameters
                }
                assay_protocol_params = protocol_params

                missing_set = assay_protocol_params - study_protocol_params
                extra_set = study_protocol_params - assay_protocol_params
                default_params = (
                    set(default_protocols[name]) if name in default_protocols else set()
                )
                missing_set = (
                    assay_protocol_params.union(default_params) - study_protocol_params
                )
                extra_set = study_protocol_params - assay_protocol_params.union(
                    default_params
                )
                for item in extra_set:
                    if item.strip() in unique_params[name]:
                        param = unique_params[name][item.strip()]
                        study_protocols[name].parameters.remove(param)
                if extra_set:
                    msg = f"Unreferenced parameters are removed from protocol '{name}'"
                    self.modifier_update(
                        source=self.model.investigation_file_path,
                        action=msg,
                        old_value=f"{name}: {', '.join(extra_set)}",
                        new_value="",
                    )
                for item in missing_set:
                    study_protocols[name].parameters.append(OntologyItem(term=item))

                if missing_set:
                    msg = (
                        "Default protocol paremters and additional protocol parameters"
                        " referenced in assay file are added to protocol"
                    )
                    self.modifier_update(
                        source=self.model.investigation_file_path,
                        action=msg,
                        old_value="",
                        new_value=f"{name}: {', '.join(missing_set)}",
                    )

        for protocol_name, protocol_item in study_protocols.items():
            default_protocol_params = {}
            if protocol_name in default_protocols:
                default_protocol_params = default_protocols[protocol_name]
            new_list: list[OntologyAnnotation] = []
            additional_params: list[OntologyAnnotation] = []
            params = {x.term: x for x in protocol_item.parameters}
            added_params: set[str] = set()
            old_param_names = ", ".join([x.term for x in protocol_item.parameters])
            for item in default_protocol_params:
                if item in params:
                    new_list.append(params[item])
                    added_params.add(item)
            for item in params:
                if item not in added_params:
                    additional_params.append(params[item])
            if additional_params:
                additional_params.sort(key=lambda x: x.term)
            new_list.extend(additional_params)
            new_param_names = ", ".join([x.term for x in new_list])
            if new_param_names != old_param_names:
                msg = f"Parameters of the protocol '{protocol_name}' are sorted"
                self.modifier_update(
                    source=self.model.investigation_file_path,
                    action=f"{msg}: '{old_param_names} -> {new_param_names}",
                    old_value=old_param_names,
                    new_value=new_param_names,
                )
                protocol_item.parameters = new_list

    def update_study_ontology_items(
        self,
        item: IsaAbstractModel,
        name: str,
        label: str,
    ):
        if isinstance(item, OntologyAnnotation):
            terms = self.get_control_list_terms("investigationFile", parameter=name)
            key = item.term.lower().strip()

            if key in terms and terms[key]:
                # select first item
                onto = list(terms[key].values())[0]
                self.override_ontology_term(
                    source=onto,
                    target=item,
                    source_label=label,
                )
            elif not item.term.strip():
                if (
                    not item.term_accession_number.strip()
                    and item.term_source_ref.strip()
                ):
                    self.override_ontology_term(
                        source=OntologyItem(),
                        target=item,
                        source_label=label,
                    )
                elif (
                    not item.term_source_ref.strip()
                    and item.term_accession_number.strip()
                ):
                    acc = item.term_accession_number
                    onto = self.get_term_by_accession_number(
                        acc,
                        "investigationFile",
                        parameter=name,
                    )
                    if onto:
                        self.override_ontology_term(
                            source=onto,
                            target=item,
                            source_label=label,
                        )
        else:
            prefix = item.isatab_config.section_prefix
            name_parts = [name]
            name_parts.append(prefix)
            label_parts = [label]
            label_parts.append(prefix)
            label_name = " ".join([x for x in label_parts if x and x.strip()])
            section_name = " ".join([x for x in name_parts if x and x.strip()])
            for field in item.__class__.model_fields:
                val = getattr(item, field)

                extra = item.__class__.model_fields[field].json_schema_extra
                suffix = (
                    extra["header_name"] if extra and "header_name" in extra else ""
                )

                control_name = f"{section_name} {suffix}" if suffix else section_name
                control_name = control_name.strip()
                label_value = f"{label_name} {suffix}" if suffix else label_name
                label_value = label_value.strip()
                if isinstance(val, IsaAbstractModel):
                    self.update_study_ontology_items(
                        val,
                        control_name,
                        label_value,
                    )
                elif (
                    val
                    and isinstance(val, list)
                    and isinstance(val[0], IsaAbstractModel)
                ):
                    for idx, term in enumerate(val):
                        label_idx = f"{label_name} [{idx + 1}]"
                        self.update_study_ontology_items(
                            term,
                            control_name,
                            label_idx,
                        )

    def update_trailing_spaces(
        self,
        item_list: list[Any],
        label: str,
        list_items: bool = False,
    ):
        for idx, obj in enumerate(item_list):
            if isinstance(obj, BaseModel):
                for k in obj.__class__.model_fields:
                    new_label_items = [label, f"{idx if list_items else ''}", f" {k}"]
                    new_label_items = [x.strip() for x in new_label_items if x]
                    new_label = " ".join(new_label_items).upper()
                    v = getattr(obj, k)
                    if v and isinstance(v, list) and isinstance(v[0], BaseModel):
                        self.update_trailing_spaces(v, new_label, list_items=True)
                    elif isinstance(v, BaseModel):
                        self.update_trailing_spaces([v], new_label)
                    elif isinstance(v, str):
                        new_val = v
                        result = re.match(r"['\"\s]*([^'\"]*)['\"\s]*$", v)
                        if result:
                            new_val = result.groups()[0].strip()
                        if new_val != v:
                            self.modifier_update(
                                source=self.model.investigation_file_path,
                                old_value=v,
                                new_value=new_val,
                                action=f"remove trailing spaces from '{new_label}'",
                            )
                            setattr(obj, k, new_val)
            elif obj and isinstance(obj, list) and isinstance(obj[0], BaseModel):
                self.update_trailing_spaces(obj, label, list_items=True)

    def rule_i_100_000_000_00(self):
        self.update_trailing_spaces(
            [self.model.investigation], "Investigation", list_items=True
        )

    def update_ontologies(self):
        if self.model.investigation and self.model.investigation.studies:
            study = self.model.investigation.studies[0]
            if not study:
                return
            self.update_study_ontology_items(study, "", "")

    def rule_i_100_200_003_01(self):
        if not self.db_metadata.submission_date:
            return
        investigation = self.model.investigation
        if investigation.submission_date != self.db_metadata.submission_date:
            self.modifier_update(
                source=self.model.investigation_file_path,
                old_value=investigation.submission_date,
                new_value=self.db_metadata.submission_date,
                action="update_investigation_submission_date",
            )
            investigation.submission_date = self.db_metadata.submission_date

    def rule_i_100_200_004_01(self):
        if not self.db_metadata.release_date:
            return
        investigation = self.model.investigation
        if investigation.public_release_date != self.db_metadata.release_date:
            self.modifier_update(
                action="Investigation Public Release Date",
                old_value=investigation.public_release_date,
                new_value=self.db_metadata.release_date,
                source=self.model.investigation_file_path,
            )
            investigation.public_release_date = self.db_metadata.release_date

    def rule_i_100_300_001_01(self):
        investigation = self.model.investigation
        if not investigation.studies:
            self.modifier_update(
                action="Empty study is defined in i_Investigation.txt file",
                old_value="",
                new_value="New enpty study",
                source=self.model.investigation_file_path,
            )
            investigation.studies = [Study()]

    def rule_i_100_300_001_02(self):
        investigation = self.model.investigation
        if len(investigation.studies) > 1:
            self.modifier_update(
                action="First study will be used in i_Investigation.txt file."
                " Other studies are deleted.",
                old_value="Multiple study",
                new_value="One study",
                source=self.model.investigation_file_path,
            )
            investigation.studies = [investigation.studies[0]]

    def rule_i_100_300_002_01(self):
        if not self.db_metadata.study_id:
            return
        investigation = self.model.investigation
        if investigation.studies and investigation.studies[0]:
            if self.db_metadata.study_id != investigation.studies[0].identifier:
                self.modifier_update(
                    action="Study Identifier is updated.",
                    old_value=investigation.studies[0].identifier,
                    new_value=self.db_metadata.study_id,
                    source=self.model.investigation_file_path,
                )
                investigation.studies[0].identifier = self.db_metadata.study_id

        if investigation.identifier != self.db_metadata.study_id:
            self.modifier_update(
                action="Investigation Identifier is updated.",
                old_value=investigation.identifier,
                new_value=self.db_metadata.study_id,
                source=self.model.investigation_file_path,
            )
            investigation.identifier = self.db_metadata.study_id

    def rule_i_100_300_003_01(self):
        investigation = self.model.investigation

        if investigation.studies and investigation.studies[0]:
            title = " ".join(
                [
                    x.strip()
                    for x in investigation.studies[0].title.strip().split()
                    if x.strip()
                ]
            )
            if title != investigation.studies[0].title:
                self.modifier_update(
                    action="Study Title is updated.",
                    old_value=investigation.studies[0].title,
                    new_value=title,
                    source=self.model.investigation_file_path,
                )
                investigation.studies[0].title = title

    def rule_i_100_300_003_02(self):
        investigation = self.model.investigation
        if investigation.studies and investigation.studies[0]:
            title = " ".join(
                [
                    x.strip()
                    for x in investigation.studies[0].title.strip().split()
                    if x.strip()
                ]
            )

            if title and title != investigation.title:
                self.modifier_update(
                    action="Investigation is updated.",
                    old_value=investigation.title,
                    new_value=title,
                    source=self.model.investigation_file_path,
                )
                investigation.title = title

    def rule_i_100_300_005_01(self):
        if not self.db_metadata.submission_date:
            return
        investigation = self.model.investigation
        if investigation.studies and investigation.studies[0]:
            study = investigation.studies[0]
            if study.submission_date != self.db_metadata.submission_date:
                self.modifier_update(
                    action="Study Submission Date is updated.",
                    old_value=study.submission_date,
                    new_value=self.db_metadata.submission_date,
                    source=self.model.investigation_file_path,
                )
                study.submission_date = self.db_metadata.submission_date

    def rule_i_100_300_006_01(self):
        if not self.db_metadata.release_date:
            return
        investigation = self.model.investigation
        if investigation.studies:
            study = investigation.studies[0]
            if study.public_release_date != self.db_metadata.release_date:
                self.modifier_update(
                    action="Study Public Release Date is updated.",
                    old_value=study.public_release_date,
                    new_value=self.db_metadata.release_date,
                    source=self.model.investigation_file_path,
                )
                study.public_release_date = self.db_metadata.release_date

    def rule_i_100_320_002_01(self):
        investigation = self.model.investigation
        if investigation.studies and investigation.studies[0]:
            study = investigation.studies[0]
            if study.study_publications and study.study_publications.publications:
                publications = study.study_publications.publications
                for idx, publication in enumerate(publications):
                    if publication.doi:
                        new_val = publication.doi.strip().removeprefix(
                            "https://doi.org/"
                        )
                        if publication.doi == new_val:
                            new_val = publication.doi.strip().removeprefix(
                                "http://doi.org/"
                            )
                        if publication.doi != new_val:
                            self.modifier_update(
                                action=f"Study Publication [{idx}] DOI is updated.",
                                old_value=publication.doi,
                                new_value=new_val,
                                source=self.model.investigation_file_path,
                            )
                            publication.doi = new_val

    def rule_i_100_320_007_01(self):
        investigation = self.model.investigation

        if investigation.studies and investigation.studies[0]:
            study = investigation.studies[0]
            if study.study_publications and study.study_publications.publications:
                control_name = "Study Publication Status"
                terms = self.get_control_list_terms(
                    "investigationFile",
                    parameter=control_name,
                )
                ontologies = {}
                for term in terms:
                    for source in terms[term]:
                        ontologies[term] = terms[term][source]

                if ontologies:
                    publications = study.study_publications.publications
                    for idx, publication in enumerate(publications):
                        search = publication.status.term.lower().strip()
                        onto = None
                        if not search:
                            if publication.title.strip():
                                onto = ontologies.get("in preparation", None)
                        else:
                            onto = ontologies.get(search, None)
                        if onto:
                            self.override_ontology_term(
                                source=onto,
                                target=publication.status,
                                source_label=f"Study Publication [{idx}] Status",
                            )

    def rule_i_100_340_009_01(self):
        investigation = self.model.investigation
        if investigation.studies and investigation.studies[0]:
            for assay in investigation.studies[0].study_assays.assays:
                assay_name_parts = assay.file_name.split("_")
                if assay_name_parts and len(assay_name_parts) > 2:
                    technique_name = assay_name_parts[2]
                    assay_technique = assay_techniques.get(technique_name)
                    if assay_technique:
                        if assay.file_name in self.model.assays:
                            assay_file = self.model.assays[assay.file_name]
                            if assay_file.assay_technique.name != assay_technique.name:
                                self.modifier_update(
                                    source=assay.file_name,
                                    action="Assay technique update",
                                    old_value=assay_file.assay_technique.name,
                                    new_value=assay_technique.name,
                                )
                                assay_file.assay_technique = assay_technique

    def rule_i_100_350_007_01(self):
        investigation = self.model.investigation
        if investigation.studies and investigation.studies[0]:
            techniques: set[str] = set()
            study = investigation.studies[0]
            for assay in study.study_assays.assays:
                if (
                    assay.file_name in self.model.assays
                    and self.model.assays[assay.file_name].assay_technique
                    and self.model.assays[assay.file_name].assay_technique.name
                ):
                    techniques.add(
                        self.model.assays[assay.file_name].assay_technique.name
                    )
            protocol_params: Dict[str, list[str]] = self.get_protocol_parameters(
                techniques
            )
            if not protocol_params:
                return
            protocols = investigation.studies[0].study_protocols.protocols
            for protocol in protocols:
                if protocol.name in protocol_params:
                    default_params = set(protocol_params[protocol.name])
                    missing_params = default_params - {
                        x.term for x in protocol.parameters
                    }
                    if missing_params:
                        self.modifier_update(
                            action=f"Missing '{protocol.name}' protocol "
                            "parameters are added: {', '.join(missing_params)}.",
                            old_value="",
                            new_value=", ".join(missing_params),
                            source=self.model.investigation_file_path,
                        )
                        for param in missing_params:
                            protocol.parameters.append(OntologyItem(term=param))

    def rule_i_100_360_004_01(self):
        investigation = self.model.investigation
        if investigation.studies and investigation.studies[0]:
            default_role = OntologyItem(
                term="Author",
                term_source_ref="NCIT",
                term_accession_number="http://purl.obolibrary.org/obo/NCIT_C42781",
            )
            if not investigation.studies[0].study_contacts.people:
                investigation.studies[0].study_contacts.people.append(Person())
            contacts = investigation.studies[0].study_contacts.people
            if contacts and contacts[0]:
                contact = contacts[0]
                if not contact.first_name.strip() and not contact.last_name.strip():
                    submitters = self.db_metadata.submitters
                    if submitters and submitters[0]:
                        name = self.upper_case_name(submitters[0].first_name)
                        if contact.first_name != name:
                            self.modifier_update(
                                action="Study Contacts [1] First Name is updated.",
                                old_value=contact.first_name,
                                new_value=name,
                                source=self.model.investigation_file_path,
                            )
                            contact.first_name = name

                        if not contact.mid_initials:
                            self.modifier_update(
                                action="Study Contacts [1] Mid Initials are updated.",
                                old_value=contact.mid_initials,
                                new_value="",
                                source=self.model.investigation_file_path,
                            )
                            contact.mid_initials = ""
                        last_name = self.upper_case_name(submitters[0].last_name)
                        if contact.last_name != last_name:
                            self.modifier_update(
                                action="Study Contacts [1] Last Name is updated.",
                                old_value=contact.last_name,
                                new_value=last_name,
                                source=self.model.investigation_file_path,
                            )
                            contact.last_name = last_name
                        contact_email = submitters[0].user_name.lower()
                        if contact.email != contact_email:
                            self.modifier_update(
                                action="Study Contacts [1] email is updated.",
                                old_value=contact.email,
                                new_value=contact_email,
                                source=self.model.investigation_file_path,
                            )
                            contact.email = contact_email
                        affiliation = submitters[0].affiliation.strip()
                        if affiliation != contact.affiliation:
                            self.modifier_update(
                                action="Study Contacts [1] Affiliation is updated.",
                                old_value=contact.affiliation,
                                new_value=affiliation,
                                source=self.model.investigation_file_path,
                            )
                            contact.affiliation = affiliation

            remove_list = []
            for idx, contact in enumerate(contacts):
                if idx > 0:
                    if not contact.first_name.strip() and not contact.last_name.strip():
                        remove_list.append((idx, contact))
            remove_list.sort(key=lambda x: x[0], reverse=True)
            for idx, contact in remove_list:
                self.modifier_update(
                    action=f"Study Contacts [{idx + 1}] "
                    "without first name and last name is removed.",
                    old_value="Empty contact",
                    new_value="",
                    source=self.model.investigation_file_path,
                )
                contacts.remove(contact)

            for idx, contact in enumerate(contacts):
                if self.db_metadata.submitters:
                    for submitter in self.db_metadata.submitters:
                        if (
                            contact.first_name.strip().lower()
                            == submitter.first_name.lower()
                            and contact.last_name.strip().lower()
                            == submitter.last_name.lower()
                            and (not contact.email or not contact.email.strip())
                        ):
                            self.modifier_update(
                                action=f"Study Contacts {idx + 1} "
                                f"({contact.first_name} {contact.last_name}): "
                                "Email is updated",
                                old_value=contact.email,
                                new_value=submitter.user_name,
                                source=self.model.investigation_file_path,
                            )
                            contact.email = submitter.user_name

                if len(contact.first_name) == 1 and contact.first_name.isupper():
                    new_val = f"{contact.first_name}."
                    self.modifier_update(
                        action=f"Study Contacts [{idx + 1}] "
                        f"({contact.first_name} {contact.last_name}) "
                        "First Name is updated.",
                        old_value=contact.first_name,
                        new_value=new_val,
                        source=self.model.investigation_file_path,
                    )
                    contact.first_name = new_val

                if len(contact.last_name) == 1 and contact.last_name.isupper():
                    new_val = f"{contact.last_name}."
                    self.modifier_update(
                        action=f"Study Contacts [{idx + 1}] "
                        f"({contact.first_name} {contact.last_name}) "
                        "Last Name is updated.",
                        old_value=contact.last_name,
                        new_value=new_val,
                        source=self.model.investigation_file_path,
                    )
                    contact.last_name = new_val
                if not contact.roles:
                    contact.roles.append(OntologyItem())

                if not contact.roles[0].term:
                    self.override_ontology_term(
                        source=default_role,
                        target=contact.roles[0],
                        source_label=f"Study Contacts [{idx + 1}] "
                        f"({contact.first_name} {contact.last_name}) Role [1]",
                    )

    def update_assay_defaults(self):
        investigation = self.model.investigation
        if investigation.studies and investigation.studies[0]:
            for idx, assay in enumerate(investigation.studies[0].study_assays.assays):
                item: OntologyAnnotation = OntologyItem(
                    term="metabolite profiling",
                    term_source_ref="OBI",
                    term_accession_number="http://purl.obolibrary.org/obo/OBI_0000366",
                )
                self.override_ontology_term(
                    source=item,
                    target=assay.measurement_type,
                    source_label=f"Study Assay [{idx + 1}] Measurement Type",
                )

                if assay.file_name in self.model.assays:
                    main_technique = self.model.assays[
                        assay.file_name
                    ].assay_technique.main_technique

                    if main_technique == "MS":
                        ms_item: OntologyAnnotation = OntologyItem(
                            term="mass spectrometry",
                            term_source_ref="OBI",
                            term_accession_number="http://purl.obolibrary.org/obo/OBI_0000470",
                        )
                        self.override_ontology_term(
                            source=ms_item,
                            target=assay.technology_type,
                            source_label=f"Study Assay [{idx + 1}] Technology Type",
                        )
                    elif main_technique == "NMR":
                        nmr_item: OntologyAnnotation = OntologyItem(
                            term="NMR spectroscopy",
                            term_source_ref="OBI",
                            term_accession_number="http://purl.obolibrary.org/obo/OBI_0000623",
                        )
                        self.override_ontology_term(
                            source=nmr_item,
                            target=assay.technology_type,
                            source_label=f"Study Assay [{idx + 1}] Technology Type",
                        )
                    technique_name = self.model.assays[
                        assay.file_name
                    ].assay_technique.name

                    self.update_technology_platform(assay, technique_name)

    def update_technology_platform(self, assay: Assay, technique_name: str):
        scan_polarity = ""
        column_type = ""
        if technique_name in assay_technique_labels:
            label = assay_technique_labels[technique_name]
            if assay.technology_platform.startswith(label):
                return
        if assay.file_name in self.model.assays:
            assay_file = self.model.assays[assay.file_name]
            column_type_header = "Parameter Value[Column type]"
            scan_polarity_header = "Parameter Value[Scan polarity]"
            if column_type_header in assay_file.table.data:
                column_type_values = set(assay_file.table.data[column_type_header])
                column_type_values.discard("")
                column_type_values.discard(None)
                if len(column_type_values) == 1:
                    column_type = list(column_type_values)[0]
            if scan_polarity_header in assay_file.table.data:
                scan_polarity_values = set(assay_file.table.data[scan_polarity_header])
                scan_polarity_values.discard("")
                scan_polarity_values.discard(None)
                if len(scan_polarity_values) == 1:
                    scan_polarity = list(scan_polarity_values)[0]

        new_val = f"{label} - {scan_polarity} - {column_type}"
        platform = assay.technology_platform
        if platform:
            new_val += f" - {platform}"
        self.modifier_update(
            action="Study Assay Technology Platform is updated.",
            old_value=assay.technology_platform,
            new_value=new_val,
            source=self.model.investigation_file_path,
        )
        assay.technology_platform = new_val

    def override_ontology_term(
        self,
        source: OntologyAnnotation,
        target: OntologyAnnotation,
        source_label: str,
    ):
        current = (
            f"{target.term_source_ref}:{target.term}:{target.term_accession_number}"
        )
        if target.term != source.term:
            target.term = source.term

        if target.term_accession_number != source.term_accession_number:
            target.term_accession_number = source.term_accession_number

        if target.term_source_ref != source.term_source_ref:
            target.term_source_ref = source.term_source_ref
        updated = (
            f"{target.term_source_ref}:{target.term}:{target.term_accession_number}"
        )
        if current != updated:
            self.modifier_update(
                action=f"{source_label} is updated.",
                old_value=str(current),
                new_value=str(updated),
                source=self.model.investigation_file_path,
            )

    def upper_case_name(self, name: str):
        if not name:
            return ""
        name_parts = name.strip().split()
        upper_case_parts = [
            x.upper() if len(x) == 1 else x[0].upper() + x[1:]
            for x in name_parts
            if x.strip()
        ]
        return " ".join(upper_case_parts)

    def update_ontology_source_set(
        self, item_list: list[Any], ontology_sources: set[str]
    ):
        if not item_list:
            return
        for obj in item_list:
            if isinstance(obj, OntologyAnnotation):
                item: OntologyAnnotation = obj
                if item.term and item.term_source_ref:
                    ontology_sources.add(item.term_source_ref.strip())
            elif isinstance(obj, IsaAbstractModel):
                for k in obj.__class__.model_fields:
                    v = getattr(obj, k)
                    if isinstance(v, IsaAbstractModel):
                        self.update_ontology_source_set([v], ontology_sources)
                    elif (
                        v and isinstance(v, list) and isinstance(v[0], IsaAbstractModel)
                    ):
                        self.update_ontology_source_set(v, ontology_sources)
            elif obj and isinstance(obj, list) and isinstance(obj[0], IsaAbstractModel):
                self.update_ontology_source_set(obj, ontology_sources)
