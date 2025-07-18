import abc
import logging
import re
from abc import abstractmethod
from functools import lru_cache
from typing import Any, Literal, Union

from metabolights_utils.models.isa.assay_file import AssayFile
from metabolights_utils.models.isa.investigation_file import (
    OntologyAnnotation,
    OntologySourceReference,
)
from metabolights_utils.models.metabolights.model import MetabolightsStudyModel

from mtbls.domain.domain_services.modifier.base_modifier import (
    BaseModifier,
    OntologyItem,
)
from mtbls.domain.shared.modifier import UpdateLog

logger = logging.getLogger(__name__)


class BaseIsaModifier(abc.ABC, BaseModifier):
    characteristics_pattern = r"^[ ]*Characteristics[ ]*\[[ ]*(\w[ -~]*)[ ]*\](\.\d+)?$"
    parameter_value_pattern = (
        r"^[ ]*Parameter[ ]+Value[ ]*\[[ ]*(\w[ -~]*)[ ]*\](\.\d+)?$"
    )
    factor_value_pattern = r"^[ ]*Factor[ ]+Value[ ]*\[[ ]*(\w[ -~]*)[ ]*\](\.\d+)?$"

    @abstractmethod
    def modify(self) -> list[UpdateLog]: ...

    def __init__(
        self,
        model: MetabolightsStudyModel,
        templates: dict,
        control_lists: dict,
        file_path: str,
    ):
        self.model = model
        self.templates = templates
        self.control_lists = control_lists
        self._term_cache = {}
        self.protocol_parameters: dict[str, dict[str, list[str]]] = {}
        self.protocol_parameters_cache: dict[str, list[str]] = {}
        self.update_logs: list[UpdateLog] = []
        self.file_path = file_path

    def modifier_update(
        self,
        source: str,
        old_value: str,
        new_value: str,
        action: str = "",
    ):
        if new_value != old_value:
            logger.debug(
                "%s: action: %s, old_value: '%s', new value: '%s'",
                source,
                action,
                old_value,
                new_value,
            )
            self.update_logs.append(
                UpdateLog(
                    source=source,
                    action=action,
                    old_value=old_value,
                    new_value=new_value,
                )
            )

    def override_ontology_source(
        self,
        source: OntologySourceReference,
        target: OntologySourceReference,
    ):
        source_str = [None, None]
        for idx, x in enumerate([source, target]):
            source_str[idx] = (
                f"Name: {x.source_name}, File: {x.source_file}, Version: {x.source_version}, Description: {x.source_description}"
            )

        if target.source_name != source.source_name:
            target.source_name = source.source_name
        if target.source_file != source.source_file:
            target.source_file = source.source_file
        if target.source_version != source.source_version:
            target.source_version = source.source_version
        if target.source_description != source.source_description:
            target.source_description = source.source_description

        if source_str[0] != source_str[1]:
            self.modifier_update(
                source=self.model.investigation_file_path,
                action="Update ontology source reference",
                old_value=source_str[1],
                new_value=source_str[0],
            )

    @lru_cache(1)
    def get_control_list_terms(
        self,
        category: Union[
            None,
            Literal[
                "sampleColumns", "investigationFile", "unitColumns", "assayColumns"
            ],
        ],
        parameter: str,
        technique_name: Union[None, str] = None,
    ):
        key = f"{category}:{parameter}:{technique_name}"
        if key in self._term_cache:
            return self._term_cache[key]
        simple_control_list = category in (
            "sampleColumns",
            "investigationFile",
            "unitColumns",
        )
        configuration = self.control_lists
        control_terms: dict[str, dict[str, OntologyAnnotation]] = {}
        ontology_items = []
        if simple_control_list:
            if (
                category in configuration
                and parameter in configuration[category]
                and "terms" in configuration[category][parameter]
                and configuration[category][parameter]["terms"]
            ):
                terms = configuration[category][parameter]["terms"]
                ontology_items = [
                    OntologyItem.model_validate(x, from_attributes=True) for x in terms
                ]

        elif (
            category in configuration
            and parameter in configuration[category]
            and "controlList" in configuration[category][parameter]
            and configuration[category][parameter]["controlList"]
        ):
            control_lists = configuration[category][parameter]["controlList"]
            for item in control_lists:
                if "techniques" in item and technique_name in item["techniques"]:
                    if "values" in item and item["values"]:
                        ontology_items = [
                            OntologyItem.model_validate(x, from_attributes=True)
                            for x in item["values"]
                        ]
                    break
        for item in ontology_items:
            key = item.term.lower()
            if key not in control_terms:
                control_terms[key] = {}
                second_key = item.term_source_ref.lower()
                if item.term_source_ref not in control_terms[key]:
                    control_terms[key][second_key] = {}
                control_terms[key][second_key] = item
        self._term_cache[key] = control_terms
        return control_terms

    def get_protocol_parameters(self, techniques: set[str]) -> dict[str, list[str]]:
        if not techniques:
            return {}
        key = "_".join(list(techniques))
        if key in self.protocol_parameters_cache:
            return self.protocol_parameters_cache[key]
        techniques_list = list(techniques)
        techniques_list.sort()

        ordered_protocol_params: dict[str, list[str]] = {}
        self.protocol_parameters[key] = ordered_protocol_params
        for technique_name in techniques:
            result = self.get_protocol_template(technique_name)
            if result:
                for protocol in result:
                    name = ""
                    if "name" in protocol and protocol["name"]:
                        name = protocol["name"]
                        if name not in ordered_protocol_params:
                            ordered_protocol_params[name] = []
                    if name and "parameters" in protocol and protocol["parameters"]:
                        params = protocol["parameters"]
                        for item in params:
                            if item not in ordered_protocol_params[name]:
                                ordered_protocol_params[name].append(item)
        if not self.protocol_parameters[key]:
            del self.protocol_parameters[key]
            return {}
        return self.protocol_parameters[key]

    def get_protocol_template(self, technique_name: str) -> list[dict[str, Any]]:
        if (
            not technique_name
            or not self.templates
            or "studyProtocolTemplates" not in self.templates
        ):
            return []
        templates = {
            x.upper(): self.templates["studyProtocolTemplates"][x]
            for x in self.templates["studyProtocolTemplates"]
        }
        if (
            technique_name.upper() in templates
            and "protocols" in templates[technique_name.upper()]
        ):
            return templates[technique_name.upper()]["protocols"]
        return []

    def get_ordered_protocol_names(self, technique_name: str) -> list[str]:
        if not technique_name:
            return []
        ordered_protocol_names: list[str] = []
        result = self.get_protocol_template(technique_name)
        if result:
            for protocol in result:
                name = ""
                if "name" in protocol and protocol["name"]:
                    name = protocol["name"]
                    if name not in ordered_protocol_names:
                        ordered_protocol_names.append(name)
        return ordered_protocol_names

    def get_protocol_parameters_in_assay(
        self, isa_table_file: AssayFile
    ) -> dict[str, tuple[int, str, list[str]]]:
        protocols: dict[str, tuple[int, str, list[str]]] = {}
        current_protocol_header = None
        ordered_protocol_names = self.get_ordered_protocol_names(
            isa_table_file.assay_technique.name.upper()
        )

        # sample collection is referenced in sample file so filter it.
        assigned_protocol_index = -1
        if (
            ordered_protocol_names
            and ordered_protocol_names[0].lower() == "sample collection"
        ):
            assigned_protocol_index = 0
        additional_protocol = 0
        for header in isa_table_file.table.headers:
            if header.column_header == "Protocol REF":
                current_protocol_header = header
                assigned_protocol_index += 1

                ordered_protocol_name = ""
                if assigned_protocol_index < len(ordered_protocol_names):
                    ordered_protocol_name = ordered_protocol_names[
                        assigned_protocol_index
                    ]
                elif header.column_name in isa_table_file.table.data:
                    values = isa_table_file.table.data[header.column_name]
                    for i in range(len(values)):
                        if values[i] and values[i].strip():
                            ordered_protocol_name = values[i].strip()
                            break
                if not ordered_protocol_name:
                    additional_protocol += 1
                    ordered_protocol_name = (
                        f"Unnamed protocol.{str(additional_protocol)}"
                    )

                protocols[header.column_name] = (
                    header.column_index,
                    ordered_protocol_name,
                    [],
                )
            else:
                result = re.search(self.parameter_value_pattern, header.column_name)
                parameter_value = ""
                if current_protocol_header and result and result.groups():
                    parameter_value = result.groups()[0]
                    if (
                        parameter_value
                        not in protocols[current_protocol_header.column_name][2]
                    ):
                        protocols[current_protocol_header.column_name][2].append(
                            parameter_value
                        )
        return protocols

    def get_study_default_protocol_parameters(self) -> dict[str, set[str]]:
        investigation = self.model.investigation
        if investigation.studies and investigation.studies[0]:
            techniques: set[str] = set()
            for assay in investigation.studies[0].study_assays.assays:
                if (
                    assay.file_name in self.model.assays
                    and self.model.assays[assay.file_name].assay_technique
                    and self.model.assays[assay.file_name].assay_technique.name
                ):
                    techniques.add(
                        self.model.assays[assay.file_name].assay_technique.name
                    )
            protocol_params: dict[str, list[str]] = self.get_protocol_parameters(
                techniques
            )
            return protocol_params
        return {}

    def get_term_by_accession_number(
        self,
        accession_number: str,
        category: Literal[
            "sampleColumns", "investigationFile", "unitColumns", "assayColumns"
        ],
        parameter: str,
        technique_name: Union[None, str] = None,
    ):
        terms = self.get_control_list_terms(category, parameter, technique_name)
        accession_dict: dict[str, OntologyAnnotation] = {}
        for key in terms:
            for second_key in terms[key]:
                term = terms[key][second_key]
                new_key = term.term_accession_number.lower()
                accession_dict[new_key] = terms[key][second_key]
        if accession_number.lower() in accession_dict:
            return accession_dict[accession_number.lower()]
        return None

    def update_from_parser_messages(self):
        file_path = self.file_path
        if (
            not self.model.parser_messages
            or file_path not in self.model.parser_messages
        ):
            return
        empty_rows = False
        new_lines_in_cells = False
        for message in self.model.parser_messages[file_path]:
            if not empty_rows and "Removed empty lines" in message.short:
                self.modifier_update(
                    source=file_path,
                    action=message.detail,
                    old_value="",
                    new_value="",
                )
                empty_rows = True
            if (
                not new_lines_in_cells
                and "Removed new line characters" in message.short
            ):
                self.modifier_update(
                    source=file_path,
                    action=message.detail,
                    old_value="",
                    new_value="",
                )
                new_lines_in_cells = True
            if new_lines_in_cells and empty_rows:
                break
