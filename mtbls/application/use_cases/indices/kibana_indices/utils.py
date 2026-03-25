import datetime
import json
import logging
from collections import OrderedDict
from pathlib import Path
from re import sub
from typing import Dict, List, Optional, Set, Tuple

from metabolights_utils.models.isa.common import (
    IsaTableColumn,
    IsaTableFile,
    OntologyItem,
    OrganismAndOrganismPartPair,
)
from metabolights_utils.models.isa.enums import ColumnsStructure
from metabolights_utils.models.metabolights.model import MetabolightsStudyModel

from mtbls.application.services.interfaces.repositories.study.study_read_repository import (
    StudyReadRepository,
)
from mtbls.application.services.interfaces.search_index_management_gateway import (
    SearchIndexManagementGateway,
)
from mtbls.application.use_cases.indices.kibana_indices.models.common import (
    BaseIsaTableIndexItem,
    Country,
    IndexPrimitiveValueList,
    IndexUnitOntologyValue,
    IndexValue,
    IndexValueList,
    ProtocolFields,
)
from mtbls.application.use_cases.indices.kibana_indices.models.study_index import (
    StudyAssayFileItem,
    StudyAssignmentFileItem,
    StudyIndexItem,
    StudySampleFileItem,
)
from mtbls.application.utils.sort_utils import (
    sort_by_study_id,
)
from mtbls.domain.enums.filter_operand import FilterOperand
from mtbls.domain.enums.study_status import StudyStatus
from mtbls.domain.shared.models import COUNTRIES
from mtbls.domain.shared.repository.entity_filter import EntityFilter
from mtbls.domain.shared.repository.query_options import QueryFieldOptions

logger = logging.getLogger(__name__)

EMPTY_VALUE_KEYWORDS = {"-", "na", "n/a", "null", "none", "unknown", "none"}


async def get_study_ids(
    study_read_repository: StudyReadRepository,
    target_study_status_list: None | list[StudyStatus] = None,
    exclude_studies: None | list[str] = None,
    min_last_update_date: datetime.datetime = None,
    max_last_update_date: datetime.datetime = None,
    db_update_field: str = "update_date",
) -> List[str]:
    try:
        excluded_set = set(exclude_studies) if exclude_studies else set()
        if not exclude_studies:
            exclude_studies = set()
        if not target_study_status_list:
            target_study_status_list = [StudyStatus.PUBLIC]
        filters = [
            EntityFilter(
                key="status",
                operand=FilterOperand.IN,
                value=target_study_status_list,
            )
        ]
        if min_last_update_date:
            filters.append(
                EntityFilter(
                    key=db_update_field,
                    operand=FilterOperand.GE,
                    value=min_last_update_date,
                )
            )
        if max_last_update_date:
            filters.append(
                EntityFilter(
                    key=db_update_field,
                    operand=FilterOperand.LE,
                    value=max_last_update_date,
                )
            )
        result = await study_read_repository.select_fields(
            query_field_options=QueryFieldOptions(
                filters=filters, selected_fields=["accession_number"]
            )
        )

        study_ids = [x[0] for x in result.data if x and x[0] not in excluded_set]
        study_ids.sort(key=sort_by_study_id, reverse=True)
        logger.info("%s studies are selected.", len(study_ids))
        return study_ids
    except Exception as ex:
        raise ex


async def find_studies_will_be_processed(
    study_read_repository: StudyReadRepository,
    search_index_management_gateway: SearchIndexManagementGateway,
    index_name: str,
    index_update_field: str = "lastUpdateDatetime",
    db_update_field: str = "update_date",
    target_study_status_list: None | list[StudyStatus] = None,
) -> Tuple[List[str], List[str], List[str]]:
    filters = [
        EntityFilter(
            key="status",
            operand=FilterOperand.IN,
            value=target_study_status_list,
        )
    ]
    if not target_study_status_list:
        target_study_status_list = [StudyStatus.PUBLIC]
    result = await study_read_repository.select_fields(
        query_field_options=QueryFieldOptions(
            filters=filters, selected_fields=["accession_number", db_update_field]
        )
    )

    all_db_study_ids = {
        x[0]: datetime.datetime.fromtimestamp(
            x[1].timestamp(), tz=datetime.timezone.utc
        )
        for x in result.data
        if x
    }

    db_study_ids = {x for x in all_db_study_ids}

    query = (
        '{ "from": 0, "size": 10000, "query": {  "match_all": {} }, '
        f'"fields": ["{index_update_field}"], '
        '"_source": false }'
    )

    result = await search_index_management_gateway.search(
        index=index_name, body=query, _source=False
    )
    es_study_ids = {
        x["_id"]: datetime.datetime.fromisoformat(x["fields"][index_update_field][0])
        for x in result.raw["hits"]["hits"]
    }
    study_ids = {x for x in es_study_ids}

    new_studies = db_study_ids - study_ids
    deleted_studies = study_ids - db_study_ids
    current_studies = study_ids.intersection(db_study_ids)
    updated_studies = {
        x: f"{es_study_ids[x]} -> {all_db_study_ids[x]}"
        for x in current_studies
        if all_db_study_ids[x] - es_study_ids[x] > datetime.timedelta(seconds=0.1)
    }

    updated_study_ids = [x for x in updated_studies]
    updated_study_ids.sort(key=sort_by_study_id)
    updated_study_id_dict = OrderedDict(
        [(x, updated_studies[x]) for x in updated_study_ids]
    )
    logger.info("Deleted studies: %s", len(deleted_studies))
    deleted_study_ids = list(deleted_studies)
    if deleted_study_ids:
        deleted_study_ids.sort(key=sort_by_study_id)
        logger.info("Deleted studies: %s", deleted_study_ids)

    logger.info("New studies: %s", len(new_studies))
    new_study_ids = list(new_studies)
    if new_studies:
        new_study_ids.sort(key=sort_by_study_id)
        logger.info("New studies: %s", new_studies)

    logger.info("Updated studies: %s", len(updated_studies))
    if updated_study_id_dict:
        logger.info(
            "\n".join([f"{x}\t{updated_studies[x]}" for x in updated_study_id_dict])
        )

    return new_study_ids, updated_study_ids, deleted_study_ids


def camel_case(s):
    if not s:
        return s
    s = sub(r"[^a-zA-Z0-9]", " ", s).title().replace(" ", "")
    if not s:
        return s
    return "".join([s[0].lower(), s[1:]])


def load_json_file(file_path):
    with Path(file_path).open("r", encoding="utf-8") as sf:
        return json.load(sf)


def get_ontology_item(field_name: str, fields: ProtocolFields) -> OntologyItem:
    ontology = OntologyItem()
    if field_name in fields and fields[field_name]:
        field_item = fields[field_name][0]
        ontology.term = field_item.value
        if hasattr(field_item, "term_source_ref"):
            ontology.term_source_ref = field_item.term_source_ref

        if hasattr(field_item, "term_accession_number"):
            ontology.term_accession_number = field_item.term_accession_number
    return ontology


def get_text_value(field_name: str, fields: ProtocolFields) -> str:
    if field_name in fields and fields[field_name]:
        field_item = fields[field_name][0]
        return field_item.value if field_item.value else ""
    return ""


def get_isa_table_index_items(
    study_model: MetabolightsStudyModel,
    identifier_prefix: str,
    table_file: IsaTableFile,
    default_fields: List[str],
    skip_additional_fields=False,
    skip_fields=None,
    skip_additional_ontology_fields=False,
    int_fields: Optional[Set[str]] = None,
    float_fields: Optional[Set[str]] = None,
) -> List[BaseIsaTableIndexItem]:
    table = table_file.table
    table_rows: List[BaseIsaTableIndexItem] = []
    if not table:
        return table_rows

    if not int_fields:
        int_fields = set()
    if not float_fields:
        float_fields = set()
    db_meta = study_model.study_db_metadata
    study_id = db_meta.study_id

    headers: Dict[str, IsaTableColumn] = {}

    table_columns: Dict[str, IsaTableColumn] = {}

    for header_item in table.headers:
        header: IsaTableColumn = header_item
        headers[header.column_name] = header
        if header.column_category != "Protocol" and header.column_structure in (
            ColumnsStructure.SINGLE_COLUMN_AND_UNIT_ONTOLOGY,
            ColumnsStructure.SINGLE_COLUMN,
            ColumnsStructure.ONTOLOGY_COLUMN,
        ):
            table_columns[header.column_name] = header
    total_rows = table.total_row_count
    if table.columns and table.columns[0] in table.data:
        data_row_length = len(table.data[table.columns[0]])
        if data_row_length != total_rows:
            logger.error(
                "%s %s: Expected data row count %s but found %s",
                study_id,
                table_file.file_path,
                data_row_length,
                total_rows,
            )
            raise ValueError()
    indexed_datetime = datetime.datetime.now()
    for row_index in range(total_rows):
        item = BaseIsaTableIndexItem()
        item.status = db_meta.status.name
        item.numeric_study_id = db_meta.numeric_study_id

        item.indexed_datetime = indexed_datetime
        item.last_update_datetime = db_meta.status_date
        item.study_id = study_id
        if db_meta.submitters:
            country_code = db_meta.submitters[0].address
            country_name = COUNTRIES.get(country_code, "")
            country = Country(code=country_code, name=country_name)
            item.country = country
        item.row_index = row_index + 1
        item.identifier = f"{identifier_prefix}-{(row_index + 1):04}"
        if row_index > 0 and row_index % 1000 == 0:
            logger.debug(
                "%s %s processed rows: %s", study_id, item.identifier, row_index
            )

        table_rows.append(item)
        table_headers: Dict[str, IndexValueList] = {}
        invalid_table_column_headers: Dict[str, IndexValueList] = {}
        visited_columns = [x for x in table_columns]
        if skip_additional_fields:
            visited_columns = [x for x in default_fields if x in table_columns]
        if skip_fields:
            visited_columns = [x for x in visited_columns if x not in skip_fields]
        for column_name in visited_columns:
            invalid_column_value = False
            header: IsaTableColumn = table_columns[column_name]
            header_name = header.column_header
            column_values = table.data[column_name]
            cleared_header_name = header_name
            # cleared_header_name = camel_case(header_name)
            if not cleared_header_name:
                logger.error(
                    "%s, column %s, column index %s is empty",
                    study_id,
                    column_name,
                    header.column_index,
                )
                continue
            index_value = IndexValue()
            try:
                if column_values[row_index]:
                    cleared_data = column_values[row_index].strip()
                    if cleared_data.lower() in EMPTY_VALUE_KEYWORDS:
                        cleared_data = ""
                else:
                    cleared_data = ""
                if cleared_data and header.column_header in int_fields:
                    index_value.value = int(cleared_data)
                elif cleared_data and header.column_header in float_fields:
                    index_value.value = float(cleared_data)
                else:
                    index_value.value = cleared_data
            except Exception as ex:
                invalid_column_value = True
                index_value.value = (
                    str(column_values[row_index])
                    if len(column_values) > row_index and column_values[row_index]
                    else ""
                )
                if len(column_values) > row_index:
                    logger.debug(
                        "%s Conversion Error: %s %s %s: value '%s' %s ",
                        study_id,
                        item.identifier,
                        header.column_header,
                        row_index,
                        index_value.value,
                        str(ex),
                    )
                else:
                    logger.debug(
                        "%s Conversion Error: invalid row index %s (rows : %s): %s %s %s",
                        study_id,
                        row_index,
                        len(column_values),
                        item.identifier,
                        header.column_header,
                        str(ex),
                    )
            if header.column_structure == ColumnsStructure.ONTOLOGY_COLUMN:
                prev_val = index_value.value
                index_value = IndexUnitOntologyValue()
                index_value.value = prev_val
                index_value.unit = ""
                ref_source_data = table.data[table.columns[header.column_index + 1]]
                accession_no = table.data[table.columns[header.column_index + 2]]
                index_value.term_source_ref = (
                    ref_source_data[row_index] if ref_source_data[row_index] else ""
                )
                index_value.term_accession_number = (
                    accession_no[row_index] if accession_no[row_index] else ""
                )
            elif (
                header.column_structure
                == ColumnsStructure.SINGLE_COLUMN_AND_UNIT_ONTOLOGY
            ):
                prev_val = index_value.value
                index_value = IndexUnitOntologyValue()
                index_value.value = prev_val
                unit = table.data[table.columns[header.column_index + 1]]
                ref_source_data = table.data[table.columns[header.column_index + 2]]
                accession_no = table.data[table.columns[header.column_index + 3]]
                index_value.unit = unit[row_index] if unit[row_index] else ""
                index_value.term_source_ref = (
                    ref_source_data[row_index] if ref_source_data[row_index] else ""
                )
                index_value.term_accession_number = (
                    accession_no[row_index] if accession_no[row_index] else ""
                )

            if not index_value:
                continue
            # if (isinstance(index_value.value, str) or isinstance(index_value.value, list)) and not index_value.value:
            #     continue

            if cleared_header_name not in table_headers:
                if header_name in default_fields:
                    value_list = []
                else:
                    if skip_additional_ontology_fields:
                        value_list = IndexPrimitiveValueList()
                    else:
                        value_list = IndexValueList()
                    value_list.header_name = header_name
                if invalid_column_value:
                    invalid_table_column_headers[cleared_header_name] = value_list
                    item.invalid_values[cleared_header_name] = value_list
                else:
                    table_headers[cleared_header_name] = value_list
                    if header_name in default_fields:
                        item.fields[camel_case(cleared_header_name)] = value_list
                    else:
                        item.additional_fields.append(value_list)
            try:
                if header_name in default_fields:
                    if invalid_column_value:
                        value_list_item: List = invalid_table_column_headers[
                            cleared_header_name
                        ]
                    else:
                        value_list_item: List = table_headers[cleared_header_name]

                    if skip_additional_ontology_fields:
                        value_list_item.append(IndexValue(value=index_value.value))
                    else:
                        value_list_item.append(index_value)
                else:
                    if skip_additional_ontology_fields:
                        value_list_item: IndexPrimitiveValueList = table_headers[
                            cleared_header_name
                        ]
                        value_list_item.data.append(index_value.value)
                    else:
                        value_list_item: IndexValueList = table_headers[
                            cleared_header_name
                        ]
                        value_list_item.data.append(index_value)
            except Exception as exc:
                message = f"{study_id} {table_file.file_path} {item.identifier} header: {header_name} {type(exc)} - {str(exc)}"
                logger.error(message)
                raise ValueError(message)
    return table_rows


async def create_study_index(study_model: MetabolightsStudyModel) -> StudyIndexItem:
    item = StudyIndexItem()
    item.indexed_datetime = datetime.datetime.now()
    item.ontology_source_references = (
        study_model.investigation.ontology_source_references.references
    )
    db_meta = study_model.study_db_metadata
    item.curation_request = db_meta.curation_request

    item.numeric_study_id = db_meta.numeric_study_id
    item.status = db_meta.status
    item.tags = db_meta.study_types
    study_id = db_meta.study_id
    item.db_id = db_meta.db_id
    item.first_public_date = db_meta.first_public_date
    item.first_private_date = db_meta.first_private_date
    item.created_at = db_meta.created_at
    item.revision_date = db_meta.revision_date
    item.revision_number = db_meta.revision_number
    item.dateset_licence = db_meta.dataset_license
    item.mhd_accession = db_meta.reserved_mhd_accession
    item.mhd_model_version = db_meta.mhd_model_version
    item.template_name = db_meta.study_template
    item.template_version = db_meta.template_version
    item.sample_template = db_meta.sample_template

    submission_date = db_meta.submission_date
    public_release_date = db_meta.release_date
    update_date = db_meta.update_date
    # status_datetime = db_meta.status_date
    item.submission_date = (
        datetime.datetime.strptime(submission_date, "%Y-%m-%d")
        if submission_date
        else None
    )
    item.reserved_accession = (
        study_id if study_id and study_id.startswith("MTBLS") else ""
    )
    item.reserved_request_id = (
        "REQ" + item.submission_date.strftime("%Y%m%d") + str(item.db_id)
        if item.submission_date
        else ""
    )
    item.public_release_date = (
        datetime.datetime.strptime(public_release_date, "%Y-%m-%d")
        if public_release_date
        else None
    )
    item.last_update_datetime = update_date
    item.update_date = update_date
    item.study_size_in_bytes = db_meta.study_size
    item.study_size_in_str = byte_size_to_str(item.study_size_in_bytes)

    item.submitters = db_meta.submitters
    order = 0
    country_codes = set()
    for submitter in item.submitters:
        order += 1
        country_code = submitter.address
        country_name = COUNTRIES.get(country_code, "")
        country = Country(code=country_code, name=country_name)
        if order == 1:
            item.country = country
            item.first_submitter = submitter
        if country_code not in country_codes:
            item.all_countries.append(country)
            country_codes.add(country_code)

    item.assay_files = []
    referenced_file_extensions = set()
    referenced_raw_file_extensions = set()
    referenced_derived_file_extensions = set()
    assay_techniques = set()

    if not study_model.investigation.studies:
        print(f"Study is empty. {db_meta.study_id}")
        return item

    study = study_model.investigation.studies[0]
    if study.identifier != db_meta.study_id:
        print(
            f"Study id is diffferent in db and i_Investigation.txt. db: {db_meta.study_id}"
            + f" I_Investigation.txt : {study.identifier}"
        )
    item.identifier = db_meta.study_id
    item.title = study.title
    item.description = study.description
    item.sample_file_name = study.file_name
    study = study_model.investigation.studies[0]
    item.study_design_descriptors = study.study_design_descriptors.design_types
    item.publications = study.study_publications.publications
    item.factors = study.study_factors.factors

    factor_name_set = set()
    factor_term_set = set()

    for factor in study.study_factors.factors:
        if factor.name:
            factor_name_set.add(factor.name)

        if factor.type.term_source_ref:
            factor_term_set.add(f"{factor.type.term_source_ref}:{factor.type.term}")
        elif factor.type.term:
            factor_term_set.add(f"{factor.type.term}")

    item.factor_header_names = list(factor_name_set)
    item.factor_terms = list(factor_term_set)

    item.protocols = study.study_protocols.protocols
    item.contacts = study.study_contacts.people
    assays = study.study_assays.assays

    item.number_of_samples = study_model.samples[
        item.sample_file_name
    ].table.total_row_count
    sample_file_content = study_model.samples[item.sample_file_name].model_copy()

    item.sample_file = StudySampleFileItem.model_validate(
        sample_file_content, from_attributes=True
    )
    for assay in assays:
        file = study_model.assays[assay.file_name].model_copy()
        assay_file: StudyAssayFileItem = StudyAssayFileItem.model_validate(
            file, from_attributes=True
        )
        assay_file.measurement_type = assay.measurement_type
        assay_file.technology_type = assay.technology_type
        assay_file.technology_platform = assay.technology_platform
        assay_file.number_of_assay_rows = study_model.assays[
            assay.file_name
        ].table.total_row_count
        item.assay_files.append(assay_file)

        item.number_of_assay_rows += study_model.assays[
            assay.file_name
        ].table.total_row_count
        item.number_of_raw_files += len(
            study_model.assays[assay.file_name].referenced_raw_files
        )
        item.number_of_derived_files += len(
            study_model.assays[assay.file_name].referenced_derived_files
        )
        technique = study_model.assays[assay.file_name].assay_technique
        if technique.name not in assay_techniques:
            assay_techniques.add(technique.name)
            item.assay_techniques.append(technique)
        item.number_of_all_referenced_files += len(
            study_model.assays[assay.file_name].referenced_raw_files
        ) + len(study_model.assays[assay.file_name].referenced_derived_files)
        for extension in file.referenced_raw_file_extensions:
            if extension not in referenced_raw_file_extensions:
                referenced_raw_file_extensions.add(extension)
                item.referenced_raw_file_extensions.append(extension)
            if extension not in referenced_file_extensions:
                referenced_file_extensions.add(extension)
                item.referenced_file_extensions.append(extension)

        for extension in file.referenced_derived_file_extensions:
            if extension not in referenced_derived_file_extensions:
                referenced_derived_file_extensions.add(extension)
                item.referenced_derived_file_extensions.append(extension)
            if extension not in referenced_file_extensions:
                referenced_file_extensions.add(extension)
                item.referenced_file_extensions.append(extension)

    item.number_of_assay_files = len(item.assay_files)

    item.metabolite_assignment_files = []
    identified_metabolite_names = set()
    all_chebi_ids = set()
    for assignment_file in study_model.metabolite_assignments:
        file = study_model.metabolite_assignments[assignment_file].model_copy()
        file_item: StudyAssignmentFileItem = StudyAssignmentFileItem.model_validate(
            file, from_attributes=True
        )
        # item.metabolite_assignments.update(file.metabolite_assignments)
        for names in file.identified_metabolite_names:
            name_list = [names]
            if "|" in names:
                name_list = names.split("|")
            if name_list:
                identified_metabolite_names.update(name_list)
        chebi_ids = set()
        for assignment in file.metabolite_assignments:
            chebi_names = file.metabolite_assignments[assignment]
            chebi_list = [chebi_names]
            if "|" in chebi_names:
                chebi_list = chebi_names.split("|")
            for name in chebi_list:
                if name.upper().startswith("CHEBI:"):
                    chebi_ids.add(name)
                    all_chebi_ids.add(name)
        item.metabolite_assignment_files.append(file_item)
        item.number_of_total_metabolite_assignment_rows += (
            study_model.metabolite_assignments[assignment_file].number_of_rows
        )
        item.number_of_metabolite_unassigned_rows += study_model.metabolite_assignments[
            assignment_file
        ].number_of_unassigned_rows
        item.number_of_metabolite_assigned_rows += study_model.metabolite_assignments[
            assignment_file
        ].number_of_assigned_rows
    item.identified_metabolite_chebi_ids = list(all_chebi_ids)

    item.number_of_metabolite_assignment_files = len(study_model.metabolite_assignments)
    metabolites = list(identified_metabolite_names)
    metabolites.sort()
    item.identified_metabolite_names = metabolites
    item.number_of_metabolites = len(item.identified_metabolite_names)

    return item


def byte_size_to_str(total_folder_size_in_bytes: int) -> str:
    if total_folder_size_in_bytes / (1024**3) >= 1:
        return str(round(total_folder_size_in_bytes / (1024**3), 2)) + "GB"
    return str(round(total_folder_size_in_bytes / (1024**2), 2)) + "MB"


def merge_column_values(
    fields: List[str], target_field: str, column_values: Dict[str, IndexValueList]
):
    sets = []
    for field in fields:
        if field in column_values:
            sets.append(set(column_values[field]))

    unique_values = set()

    for set_item in sets:
        unique_values = unique_values.union(set_item)
    if unique_values:
        column_values[target_field] = list(unique_values)

    for field in fields:
        if (not unique_values or field != target_field) and field in column_values:
            del column_values[field]


ORGANISM_TERM_EXCLUDE_PREFIX = {"reference", "blank", "quality", "experimental"}


def is_selected_sample(sample: OrganismAndOrganismPartPair):
    if sample.organism.term:
        for prefix in ORGANISM_TERM_EXCLUDE_PREFIX:
            if sample.organism.term.lower().startswith(prefix):
                return False
        return True
    return False


def sanitize_string(data):
    data = (
        data.lower()
        .replace("\n", " ")
        .replace("\t", " ")
        .replace("<p>", "")
        .replace("</p>", "")
        .replace("</strong>", "")
        .replace("</strong>", "")
        .replace("<i>", "")
        .replace("</i>", "")
        .replace("</em>", "")
        .replace("<em>", "")
        .replace("  ", " ")
        .strip()
    )
    for _ in range(5):
        data = data.replace("  ", " ")
    return data
