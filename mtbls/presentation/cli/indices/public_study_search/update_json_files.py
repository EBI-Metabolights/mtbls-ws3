import datetime
import pathlib
import re
from logging import getLogger
from typing import Dict, List, Set, Tuple, Union

import pandas as pd
from metabolights_utils.isatab import (
    InvestigationFileReader,
    IsaTableFileReader,
    Reader,
)
from metabolights_utils.isatab.reader import (
    InvestigationFileReaderResult,
    IsaTableFileReaderResult,
)
from metabolights_utils.models.isa.common import IsaTable
from metabolights_utils.models.isa.enums import ColumnsStructure
from metabolights_utils.models.metabolights.model import CurationRequest
from metabolights_utils.models.parser.common import ParserMessage
from metabolights_utils.models.parser.enums import ParserMessageType
from pydantic import BaseModel
from pydantic.alias_generators import to_camel

from mtbls.application.services.interfaces.http_client import HttpClient
from mtbls.application.services.interfaces.repositories.file_object.file_object_write_repository import (  # noqa: E501
    FileObjectWriteRepository,
)
from mtbls.application.services.interfaces.repositories.study.study_read_repository import (  # noqa: E501
    StudyReadRepository,
)
from mtbls.application.utils.size_utils import get_size_in_str
from mtbls.domain.entities.study import StudyOutput
from mtbls.domain.enums.http_request_type import HttpRequestType
from mtbls.domain.enums.study_status import StudyStatus
from mtbls.domain.shared.repository.entity_filter import EntityFilter
from mtbls.presentation.cli.indices.public_study_search import schemas
from mtbls.presentation.cli.indices.public_study_search.assay_classifier import (
    AssayClassifier,
    AssayTechnique,
)
from mtbls.presentation.cli.indices.public_study_search.sort_utils import (
    sort_by_study_id,
)

ISA_DATE_FORMAT = "%Y-%m-%d"
COMMON_DATE_FORMAT = "%d/%m/%Y"
FUNDER_COMMENT_NAMES = {"Study Funding Agency", "Funder"}
FUNDER_REF_ID_COMMENT_NAMES = {"FundRef ID"}
GRANT_IDENTIFIER_NAMES = {"Study Grant Number", "Grant Identifier"}
parameter_value_pattern = r"(\bParameter Value\b)\[\b(.*)\b\](\.\d)?"
characteristic_value_pattern = r"(\bCharacteristics\b)\[\b(.*)\b\](\.\d)?"
factor_value_pattern = r"(\bFactor Value\b)\[\b(.*)\b\](\.\d)?"

logger = getLogger(__name__)


class StudyQueryResult(BaseModel):
    study_id: str = ""
    study_size: int = 0
    study_type: str = ""
    curation_request: CurationRequest = CurationRequest.MANUAL_CURATION
    submitters: Dict[str, Dict[str, str]] = {}


async def get_public_studies(
    read_study_repository: StudyReadRepository,
) -> Dict[str, StudyQueryResult]:
    studies: list[StudyOutput] = await read_study_repository.get_studies(
        filters=[EntityFilter(key="status", value=StudyStatus.PUBLIC)],
        include_submitters=True,
    )

    study_map: Dict[str, StudyQueryResult] = {}
    if studies:
        for study in studies:
            if study.accession_number not in study_map:
                study_map[study.accession_number] = StudyQueryResult(
                    study_id=study.accession_number,
                    study_size=int(study.study_size) if study.study_size else 0,
                    study_type=study.study_type or "",
                    curation_request=CurationRequest.get_from_int(
                        study.curation_type.value
                    ),
                    submitters={
                        f"{x.first_name} {x.last_name}": {
                            "email": x.email or "",
                            "address": x.address or "",
                            "orcid": x.orcid or "",
                            "affiliation": x.affiliation or "",
                        }
                        for x in study.submitters
                        if x
                    },
                )

    return study_map


async def update_study_metadata_json_files(
    read_study_repository: StudyReadRepository,
    http_client: HttpClient,
    index_cache_files_object_repository: FileObjectWriteRepository,
    metadata_files_object_repository: FileObjectWriteRepository,
    temp_folder: None | str = None,
    ncbi_taxonomy_url: None | str = None,
    reindex_all: bool = True,
):
    if not temp_folder:
        temp_folder_path = pathlib.Path(".temp/indexed-data").resolve()
    else:
        temp_folder_path = pathlib.Path(temp_folder).resolve()

    ontology_categories: Dict[str, Dict[str, str]] = {}

    if not ncbi_taxonomy_url:
        ncbi_taxonomy_url = (
            "https://www.ebi.ac.uk/metabolights/resources/ontologies/ncbi-taxonomy.tsv"
        )

    temp_folder_path.mkdir(parents=True, exist_ok=True)
    temp_ncbi_filename = "ncbi-taxonomy.tsv"
    temp_ncbi_file_path = temp_folder_path / pathlib.Path(temp_ncbi_filename)

    if not temp_ncbi_file_path.exists():
        with temp_ncbi_file_path.open("wb") as buffered_writer:
            await http_client.stream(
                buffered_writer,
                method=HttpRequestType.GET,
                url=ncbi_taxonomy_url,
                follow_redirects=True,
            )

    ncbi_taxonomy_df: pd.DataFrame = pd.read_csv(
        temp_ncbi_file_path,
        delimiter="\t",
        index_col=["tax_id"],
        header=0,
        dtype=str,
    )
    ncbi_taxonomy_df["tax_id"] = ncbi_taxonomy_df.index
    ncbi_taxonomy_df.replace(pd.NA, "", regex=True, inplace=True)
    filtered_columns = [
        "tax_id",
        "domain",
        "phylum",
        "class",
        "order",
        "family",
        "genus",
        "species",
        "kingdom",
    ]

    study_map = await get_public_studies(read_study_repository)
    studies = list(study_map.keys())

    studies.sort(key=sort_by_study_id)
    timestamp = int(datetime.datetime.now().timestamp())

    investigation_reader: InvestigationFileReader = (
        Reader.get_investigation_file_reader()
    )
    sample_file_reader = Reader.get_sample_file_reader(results_per_page=100000)
    assay_reader: IsaTableFileReader = Reader.get_assay_file_reader(
        results_per_page=100000
    )

    lookup: schemas.PublicStudyLiteIndexReferences = (
        schemas.PublicStudyLiteIndexReferences()
    )
    indexed_study_count = 0
    for study_id in studies:
        file_exist = await index_cache_files_object_repository.exists(
            study_id, f"{study_id}.json"
        )
        if not reindex_all and file_exist:
            logger.info(
                "%s is skipped. File exists: %s, Force reindex: %s",
                study_id,
                file_exist,
                reindex_all,
            )
            continue
        logger.info("%s is being processed.", study_id)
        inv_file_name = "i_Investigation.txt"
        tmp_metadata_root_path = (
            pathlib.Path(temp_folder_path)
            / pathlib.Path("metadata")
            / pathlib.Path(study_id)
        )
        inv_file_exists = await metadata_files_object_repository.exists(
            study_id, inv_file_name
        )
        if inv_file_exists:
            tmp_metadata_root_path.mkdir(parents=True, exist_ok=True)
            tmp_inv_file_path = tmp_metadata_root_path / pathlib.Path(inv_file_name)
            if not tmp_inv_file_path.exists() or reindex_all:
                await metadata_files_object_repository.download(
                    study_id, inv_file_name, tmp_inv_file_path
                )

            result: InvestigationFileReaderResult = investigation_reader.read(
                file_buffer_or_path=tmp_inv_file_path
            )
            studies = result.investigation.studies
            errors: list[ParserMessage] = [
                x
                for x in result.parser_report.messages
                if x.type in {ParserMessageType.ERROR, ParserMessageType.CRITICAL}
            ]
            for error in errors:
                logger.error("%s %s", study_id, error.detail)
            if errors or len(studies) < 1:
                logger.warning("Study file is not processed %s", study_id)
                continue

            references = result.investigation.ontology_source_references.references
            source_references_set = {
                schemas.OntologySourceReferenceModel.model_validate(x)
                for x in references
            }

            factors_set: Set[schemas.OntologyModel] = set()
            study_factors_list = [x.study_factors for x in result.investigation.studies]

            for study_factors in study_factors_list:
                for study_factor in study_factors.factors:
                    model = schemas.OntologyModel.model_validate(study_factor.type)
                    factors_set.add(model)

            design_descriptors_set: Set[schemas.OntologyModel] = set()
            study_design_descriptors_list = [
                x.study_design_descriptors for x in result.investigation.studies
            ]

            for descriptors in study_design_descriptors_list:
                for design_type in descriptors.design_types:
                    model = schemas.OntologyModel.model_validate(design_type)
                    design_descriptors_set.add(model)

            sample_files = [x.file_name for x in result.investigation.studies]
            for sample_file in sample_files:
                sample_file_exists = await metadata_files_object_repository.exists(
                    study_id, sample_file
                )
                if not sample_file_exists:
                    logger.warning("%s %s file does not exist.", study_id, sample_file)
                    continue
                tmp_sample_file_path: pathlib.Path = (
                    tmp_metadata_root_path / sample_file
                )
                if not tmp_sample_file_path.exists() or reindex_all:
                    await metadata_files_object_repository.download(
                        study_id, sample_file, tmp_sample_file_path
                    )
                samples_result = sample_file_reader.get_headers(tmp_sample_file_path)
                table: IsaTable = samples_result.isa_table_file.table
                selected_column_names = []
                selected_column_structures = {}
                factor_column_names = []
                for header in table.headers:
                    column_name = header.column_name

                    if column_name.startswith("Factor Value"):
                        selected_column_names.append(column_name)
                        factor_column_names.append(column_name)
                        selected_column_structures[column_name] = (
                            header.column_structure
                        )
                    elif column_name.startswith("Characteristics"):
                        selected_column_names.append(column_name)
                        selected_column_structures[column_name] = (
                            header.column_structure
                        )

                samples_result = sample_file_reader.read(
                    file_buffer_or_path=tmp_sample_file_path,
                    offset=None,
                    limit=None,
                    selected_columns=selected_column_names.copy(),
                )
                default_characteristics = {
                    "Characteristics[Organism]",
                    "Characteristics[Organism part]",
                    "Characteristics[Variant]",
                    "Characteristics[Sample type]",
                }
                additional_characteristics_column_names = [
                    x
                    for x in selected_column_names
                    if x not in factor_column_names and x not in default_characteristics
                ]
                table: IsaTable = samples_result.isa_table_file.table
                sample_file_ontology_column_indices: Dict[str, int] = {}
                sample_file_ontology_column_values: Dict[
                    str, Set[schemas.OntologyModel]
                ] = {}
                for column_name in selected_column_names:
                    idx = table.columns.index(column_name)
                    sample_file_ontology_column_indices[column_name] = idx
                    sample_file_ontology_column_values[column_name] = set()

                for row_id in range(table.total_row_count):
                    for column_name in selected_column_names:
                        idx = sample_file_ontology_column_indices[column_name]
                        value_set = sample_file_ontology_column_values[column_name]
                        structure = selected_column_structures[column_name]
                        if structure == ColumnsStructure.ONTOLOGY_COLUMN:
                            update_ontology_set(value_set, table, row_id, idx)
                        elif (
                            structure
                            == ColumnsStructure.SINGLE_COLUMN_AND_UNIT_ONTOLOGY
                        ):
                            update_unit_ontology_set(value_set, table, row_id, idx)
                        elif structure == ColumnsStructure.SINGLE_COLUMN:
                            value = rm_quotations(table.data[column_name][row_id])
                            if value:
                                value_set.add(value)
            study_index_model: schemas.PublicStudyLiteIndexModel = (
                schemas.PublicStudyLiteIndexModel()
            )
            for submitter in study_map[study_id].submitters:
                country_code = study_map[study_id].submitters[submitter]["address"]
                country = schemas.COUNTRIES.get(country_code, "")
                study_index_model.submitters.append(
                    schemas.Submitter(
                        fullname=submitter,
                        email=study_map[study_id].submitters[submitter]["email"],
                        country=country,
                        roles=["submitter"],
                        affiliation=study_map[study_id].submitters[submitter][
                            "affiliation"
                        ],
                        orcid=study_map[study_id]
                        .submitters[submitter]
                        .get("orcid", "")
                        .strip('"'),
                    )
                )
            curation_tags = study_map[study_id].study_type
            if curation_tags:
                for tag in curation_tags.split(";"):
                    if tag.strip():
                        study_index_model.curator_annotations.append(tag.strip())

            study = result.investigation.studies[0]
            study_index_model.study_id = study_id
            study_index_model.title = study.title
            study_index_model.description = study.description
            study_index_model.sample_file_path = study.file_name
            study_index_model.curation_request = study_map[study_id].curation_request
            try:
                study_index_model.public_release_date = datetime.datetime.strptime(
                    study.public_release_date, ISA_DATE_FORMAT
                )
            except Exception:
                try:
                    study_index_model.public_release_date = datetime.datetime.strptime(
                        study.public_release_date, COMMON_DATE_FORMAT
                    )
                except Exception as sub_exc:
                    logger.warning(str(sub_exc))
            try:
                study_index_model.submission_date = datetime.datetime.strptime(
                    study.submission_date, ISA_DATE_FORMAT
                )
            except Exception:
                try:
                    study_index_model.submission_date = datetime.datetime.strptime(
                        study.submission_date, COMMON_DATE_FORMAT
                    )
                except Exception as sub_exc:
                    logger.warning(str(sub_exc))
            study_index_model.factors = factors_set
            study_index_model.design_descriptors = design_descriptors_set
            if "Characteristics[Organism]" in sample_file_ontology_column_values:
                study_index_model.organisms = sample_file_ontology_column_values[
                    "Characteristics[Organism]"
                ]
            if "Characteristics[Organism part]" in sample_file_ontology_column_values:
                study_index_model.organism_parts = sample_file_ontology_column_values[
                    "Characteristics[Organism part]"
                ]
            if "Characteristics[Sample type]" in sample_file_ontology_column_values:
                study_index_model.sample_types = sample_file_ontology_column_values[
                    "Characteristics[Sample type]"
                ]

            if "Characteristics[Variant]" in sample_file_ontology_column_values:
                study_index_model.variants = sample_file_ontology_column_values[
                    "Characteristics[Variant]"
                ]

            study_index_model.size_in_bytes = study_map[study_id].study_size
            study_index_model.size_in_text = get_size_in_str(
                study_map[study_id].study_size
            )
            study = result.investigation.studies[0]

            orcid_ids = []
            for comment in study.study_contacts.comments:
                if comment == "Study Person ORCID":
                    orcid_ids = comment.value
            contact_index = 0

            for contact in study.study_contacts.people:
                contact_model = schemas.ContactModel.model_validate(contact)
                if len(orcid_ids) > contact_index:
                    contact_model.orcid = orcid_ids[contact_index]
                study_index_model.contacts.add(contact_model)
                contact_index += 1

            for protocol in study.study_protocols.protocols:
                item = schemas.StudyProtocolModel.model_validate(protocol)
                study_index_model.protocols.add(item)
                lookup.protocol_names.add(item.protocol_type)

            for publication in study.study_publications.publications:
                publication.author_list = publication.author_list.strip('"')
                publication.doi = publication.doi.strip('"')
                publication.pub_med_id = publication.pub_med_id.strip('"')
                publication.title = publication.title.strip('"')
                study_index_model.publications.add(
                    schemas.StudyPublicationModel.model_validate(publication)
                )

            funding_comments: Dict[str, List[str]] = {}
            funding_count = 0
            for comment in study.comments:
                if not comment.name or not comment.value:
                    continue
                values = comment.value
                if (
                    comment.name in FUNDER_COMMENT_NAMES
                    or comment.name in FUNDER_REF_ID_COMMENT_NAMES
                    or comment.name in GRANT_IDENTIFIER_NAMES
                ) and len(values) > funding_count:
                    funding_count = len(values)

                if comment.name in FUNDER_COMMENT_NAMES:
                    funding_comments["funder"] = values

                if comment.name in FUNDER_REF_ID_COMMENT_NAMES:
                    funding_comments["funder_ref_id"] = values

                if comment.name in GRANT_IDENTIFIER_NAMES:
                    funding_comments["grant_identifier"] = values

            for i in range(funding_count):
                model = schemas.FundingModel()
                for key in funding_comments:
                    if (
                        funding_comments[key]
                        and len(funding_comments[key]) > i
                        and funding_comments[key][i]
                    ):
                        setattr(model, key, funding_comments[key][i])
                if ";" in model.grant_identifier:
                    grant_ids = model.grant_identifier.split(";")
                    for grant_id in grant_ids:
                        new_grant_id = grant_id.strip()
                        if new_grant_id:
                            model = schemas.FundingModel(
                                funder=model.funder,
                                fund_ref_id=model.fund_ref_id,
                                grant_identifier=grant_id.strip(),
                            )
                            study_index_model.fundings.add(model)
                else:
                    study_index_model.fundings.add(model)

            assignment_files_set: Set[str] = set()
            for assay in result.investigation.studies[0].study_assays.assays:
                assay_file_exists = await metadata_files_object_repository.exists(
                    study_id, assay.file_name
                )
                if not assay_file_exists:
                    logger.warning(
                        "%s %s file does not exist.", study_id, assay.file_name
                    )
                    continue
                tmp_assay_file_path: pathlib.Path = (
                    tmp_metadata_root_path / assay.file_name
                )
                if not tmp_assay_file_path.exists() or reindex_all:
                    await metadata_files_object_repository.download(
                        study_id, assay.file_name, tmp_assay_file_path
                    )

                model = schemas.AssayModel.model_validate(assay)
                model.file_path = assay.file_name
                technique: AssayTechnique = AssayClassifier.find_assay_technique(
                    investigation=result.investigation,
                    assay_path=tmp_assay_file_path,
                    manual_assignment=study_map[study_id].study_type,
                )

                model.technique = schemas.AssayTechniqueModel.model_validate(technique)
                study_index_model.assays.add(model)
                study_index_model.assay_techniques.add(model.technique)
                study_index_model.technology_types.add(model.technology_type)
                assay_reader: IsaTableFileReader = Reader.get_assay_file_reader(
                    results_per_page=100000
                )

                try:
                    column_name = "Metabolite Assignment File"
                    assignment_files_result: IsaTableFileReaderResult = (
                        assay_reader.read(
                            file_buffer_or_path=tmp_assay_file_path,
                            offset=0,
                            limit=None,
                            selected_columns=[column_name],
                        )
                    )
                    data = assignment_files_result.isa_table_file.table.data[
                        column_name
                    ]
                    assignment_files_set.update(data)
                    assignment_files_set.discard(None)
                    assignment_files_set.discard("")
                    model.assignment_files = assignment_files_set
                except Exception:
                    logger.error(
                        "%s does not have Metabolite Assignment File column",
                        assay.file_name,
                    )

            lookup.source_references.update(source_references_set)
            lookup.organisms.update(study_index_model.organisms)
            lookup.organism_parts.update(study_index_model.organism_parts)
            lookup.variants.update(study_index_model.variants)
            lookup.sample_types.update(study_index_model.sample_types)
            lookup.factors.update(study_index_model.factors)
            lookup.design_descriptors.update(study_index_model.design_descriptors)
            lookup.technology_types.update(study_index_model.technology_types)
            lookup.assay_techniques.update(study_index_model.assay_techniques)

            new_index_item = update_new_index_item(
                study_id,
                assay_reader,
                tmp_assay_file_path,
                study_index_model,
                additional_characteristics_column_names,
                factor_column_names,
                sample_file_ontology_column_values,
                ontology_categories,
                ncbi_taxonomy_df,
                filtered_columns,
            )

            new_index_item.annotations.sort()
            study_index_model.annotations = new_index_item.annotations
            study_index_model.modified_time = datetime.datetime.now(
                datetime.timezone.utc
            )
            temp_output_path = temp_folder_path / pathlib.Path(f"{study_id}.json")
            with pathlib.Path(temp_output_path).open("w") as f:
                f.write(study_index_model.model_dump_json(indent=4))
            await index_cache_files_object_repository.put_object(
                study_id,
                object_key=f"{study_id}.json",
                source_uri=f"file://{temp_output_path}",
                override=True,
            )

            indexed_study_count += 1

    temp_lookup_path = temp_folder_path / f"__lookup_{timestamp}.json"
    with pathlib.Path(temp_lookup_path).open("w") as f:
        f.write(lookup.model_dump_json(indent=4))

    await index_cache_files_object_repository.put_object(
        study_id,
        object_key=f"__lookup_{timestamp}.json",
        source_uri=f"file://{temp_lookup_path}",
        override=True,
    )


def convert_name(val: str):
    if not val:
        return ""
    result = re.sub("[^a-zA-Z0-9]", "_", val.strip()).strip("_")
    return result


def update_new_index_item(
    study_id: str,
    assay_reader: IsaTableFileReader,
    assay_file_path: str,
    study_index_model: schemas.PublicStudyLiteIndexModel,
    additional_characteristics_column_names: List[str],
    factor_column_names: List[str],
    sample_file_ontology_column_values: Dict[
        str, Set[Union[schemas.OntologyModel, schemas.ValueAndUnitModel]]
    ],
    ontology_categories: Dict[str, Dict[str, str]],
    ncbi_taxonomy_df: pd.DataFrame,
    filtered_columns: List[str],
):
    protocol_parameters: Dict[str, str] = {}
    item = create_study_item(
        study_id,
        study_index_model,
        ontology_categories,
        ncbi_taxonomy_df,
        filtered_columns,
    )
    technology_platform_set = set()
    measurement_type_set = set()
    for assay in study_index_model.assays:
        technology_platform_set.add(assay.technology_platform)
        measurement_type_set.add(assay.measurement_type)

    for tp in technology_platform_set:
        item.annotations.append(f"assay.technologyPlatform::{tp}")
    for mt in measurement_type_set:
        item.annotations.append(f"assay.measurementType::{mt}")

    for protocol in study_index_model.protocols:
        item.annotations.append(f"assay.protocol::{protocol.name}")
        protocol_name = to_camel(convert_name(protocol.name))
        for param in protocol.parameters:
            param_name = to_camel(convert_name(param.term))
            protocol_parameters[param.term] = (
                f"assay.protocol.{protocol_name}.{param_name}"
            )

    headers_result: IsaTableFileReaderResult = assay_reader.get_headers(
        file_buffer_or_path=assay_file_path,
    )
    column_name_parameter_map: Dict[str, str] = {}
    selected_columns = []
    headers = headers_result.isa_table_file.table.headers
    for header in headers:
        match = re.match(parameter_value_pattern, header.column_name)
        param = ""
        if match:
            param = match.groups()[1]

        if param in protocol_parameters:
            selected_columns.append(header.column_name)
            column_name_parameter_map[header.column_name] = param

    protocol_parameter_values: Dict[str, Set[str]] = {}
    try:
        assay_parameter_values: IsaTableFileReaderResult = assay_reader.read(
            file_buffer_or_path=assay_file_path,
            offset=0,
            limit=None,
            selected_columns=selected_columns,
        )
        table = assay_parameter_values.isa_table_file.table
        table_data = assay_parameter_values.isa_table_file.table.data
        for header in assay_parameter_values.isa_table_file.table.headers:
            column_name = header.column_name
            if column_name not in column_name_parameter_map:
                continue

            unique_values = set()
            if header.column_structure == ColumnsStructure.ONTOLOGY_COLUMN:
                idx = table.columns.index(column_name)

                for i in range(len(table.data[column_name])):
                    (
                        term,
                        term_source_ref,
                        term_accession_number,
                    ) = read_ontology_columns(table, i, idx)
                    if term or term_source_ref or term_accession_number:
                        ontology = schemas.OntologyModel(
                            term=term,
                            term_source_ref=term_source_ref,
                            term_accession_number=term_accession_number,
                        )
                        unique_values.add(ontology)
            if (
                header.column_structure
                == ColumnsStructure.SINGLE_COLUMN_AND_UNIT_ONTOLOGY
            ):
                idx = table.columns.index(column_name)

                for i in range(len(table.data[column_name])):
                    (
                        term_value,
                        unit_term,
                        term_source_ref,
                        term_accession_number,
                    ) = read_unit_ontology_columns(table, i, idx)
                    if (
                        term_value
                        or unit_term
                        or term_source_ref
                        or term_accession_number
                    ):
                        value_model = schemas.ValueAndUnitModel(
                            value=term_value,
                            unit_term=unit_term,
                            term_source_ref=term_source_ref,
                            term_accession_number=term_accession_number,
                        )
                        unique_values.add(value_model)
            elif header.column_structure == ColumnsStructure.SINGLE_COLUMN:
                unique_values = {
                    x for x in table_data[column_name] if x and rm_quotations(x)
                }

            parameter_name = column_name_parameter_map[column_name]
            protocol_parameter_values[parameter_name] = unique_values

    except Exception as exc:
        logger.error("%s does not have column: %s", assay_file_path, exc)
    for column_name in protocol_parameters:
        param_value_key = protocol_parameters[column_name]
        if (
            column_name in protocol_parameter_values
            and len(protocol_parameter_values[column_name]) < 10
        ):
            add_annotation(
                item, param_value_key, protocol_parameter_values[column_name]
            )
    for column_name in additional_characteristics_column_names:
        match = re.match(characteristic_value_pattern, column_name)
        param = ""
        if match:
            param = match.groups()[1]
        if param:
            sub_namespace = to_camel(convert_name(param))
            item.annotations.append(f"study.characteristic::{sub_namespace}")
            add_annotation(
                item,
                f"study.characteristic.{sub_namespace}",
                sample_file_ontology_column_values[column_name],
            )

    factor_annotations = set()
    for anno in item.annotations:
        if anno.startswith("study.factor::"):
            factor_annotations.add(anno.lower())

    for column_name in factor_column_names:
        match = re.match(factor_value_pattern, column_name)
        param = ""
        if match:
            param = match.groups()[1]
        if param:
            sub_namespace = to_camel(convert_name(param))
            annotation = f"study.factor::{param}"
            found = False
            search = annotation.lower()
            for factor in factor_annotations:
                if search in factor:
                    found = True
                    break
            if not found:
                item.annotations.append(annotation)
            add_annotation(
                item,
                f"study.factor.{sub_namespace}",
                sample_file_ontology_column_values[column_name],
            )
    for annotation in study_index_model.curator_annotations:
        if annotation:
            item.annotations.append(f"study.type::{annotation}")
    return item


def create_study_item(
    study_id,
    study_index_model: schemas.PublicStudyLiteIndexModel,
    ontology_categories: Dict[str, Dict[str, str]],
    ncbi_taxonomy_df: pd.DataFrame,
    filtered_columns: List[str],
) -> schemas.MetabolightsIndexItem:
    study_index_item = schemas.MetabolightsIndexItem(
        item_id=study_id, item_type="study"
    )

    funders_set: Set[str] = set()
    funding_ref_id_set: Set[str] = set()
    grant_set: Set[str] = set()
    for funding in study_index_model.fundings:
        if funding.funder:
            funders_set.add(funding.funder)
        if funding.fund_ref_id:
            funding_ref_id_set.add(funding.fund_ref_id)
        if funding.grant_identifier:
            grant_set.add(funding.grant_identifier)
    model = study_index_model
    item = study_index_item
    # study_index_item.data.append(KeyValueModel(key="title",
    #       value=model.title))
    # study_index_item.data.append(KeyValueModel(key="description",
    #       value=model.title))
    # for protocol in model.protocols:
    #     protocol_name = to_camel(convert_name(protocol.name))
    #     study_index_item.data.append(
    #         KeyValueModel(
    #             key=f"protocol.{protocol_name}.description",
    #               value=protocol.description
    #         )
    #     )

    add_annotation(item, "study.funding.funder", funders_set)
    add_annotation(item, "study.funding.fundingRefId", funding_ref_id_set)
    add_annotation(item, "study.funding.grantId", grant_set)
    add_annotation(item, "study.characteristic.organism", model.organisms)
    add_annotation(item, "study.characteristic.organismPart", model.organism_parts)
    add_annotation(item, "study.characteristic.sampleType", model.sample_types)
    add_annotation(item, "study.characteristic.variant", model.variants)
    add_annotation(item, "study.factor", model.factors, max_item_count=100)
    add_annotation(item, "study.designDescriptor", model.design_descriptors)
    add_annotation(item, "assay.technologyType", model.technology_types)

    assay_technique_names = {x.name for x in model.assay_techniques if x.name}
    add_annotation(item, "assay.technique.name", assay_technique_names)

    assay_techniques = {x.technique for x in model.assay_techniques if x.technique}
    add_annotation(item, "assay.technique", assay_techniques)

    assay_main_techniques = {x.main for x in model.assay_techniques if x.main}
    add_annotation(item, "assay.technique.main", assay_main_techniques)

    assay_sub_techniques = {x.sub for x in model.assay_techniques if x.sub}
    add_annotation(item, "assay.technique.sub", assay_sub_techniques)
    all_categories = set()
    for organism in model.organisms:
        categories = find_organism_categories(
            organism.term_source_ref,
            organism.term_accession_number,
            ontology_categories,
            ncbi_taxonomy_df,
            filtered_columns,
        )
        if categories:
            all_categories.update(categories)

    if all_categories:
        item.annotations.extend(categories)

    return study_index_item


def add_annotation(
    item: schemas.MetabolightsIndexItem,
    key: str,
    value: Union[None, str, Set[str]],
    max_item_count: int = 20,
):
    if isinstance(value, str):
        values = [value]
    elif isinstance(value, set):
        values: List[str] = list(value)
    else:
        values = []
    values.sort()
    if len(values) > max_item_count:
        return
    for val in values:
        if val:
            if isinstance(val, str):
                sanitized_val = rm_quotations(val)
                item.annotations.append(
                    str(schemas.Annotation(key=key, value=sanitized_val))
                )
            elif isinstance(val, schemas.OntologyModel):
                sanitized_val: schemas.OntologyModel = schemas.OntologyModel()
                sanitized_val.term = rm_quotations(val.term)
                sanitized_val.term_source_ref = rm_quotations(val.term_source_ref)
                sanitized_val.term_accession_number = rm_quotations(
                    val.term_accession_number
                )
                if (
                    sanitized_val.term
                    or sanitized_val.term_source_ref
                    or sanitized_val.term_accession_number
                ):
                    item.annotations.append(
                        str(schemas.Annotation(key=key, value=sanitized_val))
                    )
            elif isinstance(val, schemas.ValueAndUnitModel):
                sanitized_val: schemas.ValueAndUnitModel = schemas.ValueAndUnitModel()
                sanitized_val.value = rm_quotations(val.value)
                sanitized_val.unit_term = rm_quotations(val.unit_term)
                sanitized_val.term_source_ref = rm_quotations(val.term_source_ref)
                sanitized_val.term_accession_number = rm_quotations(
                    val.term_accession_number
                )
                if (
                    sanitized_val.value
                    or sanitized_val.unit_term
                    or sanitized_val.term_source_ref
                    or sanitized_val.term_accession_number
                ):
                    item.annotations.append(
                        str(schemas.Annotation(key=key, value=sanitized_val))
                    )


def find_accession_id(source_ref: str, accession_number: str):
    ncbi_id = ""
    if "NCBITAXON" in source_ref.upper() or "NCBITAXON" in accession_number.upper():
        result = re.match("[^0-9]*([0-9]+)", accession_number)
        if result:
            ncbi_id = result.groups()[0]
    return ncbi_id if ncbi_id else ""


# ontology_categories: Dict[str, Dict[str, str]] = {}


def find_organism_categories(
    source_ref: str,
    accession_number: str,
    ontology_categories: Dict[str, Dict[str, str]],
    ncbi_taxonomy_df: pd.DataFrame,
    filtered_columns: List[str],
) -> List[str]:
    if accession_number in ontology_categories:
        return ontology_categories[accession_number]
    accession_id = find_accession_id(source_ref, accession_number)

    categories: List[str] = []
    if accession_id:
        if int(accession_id) in ncbi_taxonomy_df.index:
            idx = int(accession_id)
            data = ncbi_taxonomy_df.loc[idx, :]
            data_dict = data.to_dict()
            for column_name in filtered_columns:
                categories.append(
                    f"study.characteristic.organism.{column_name}::{data_dict[column_name]}"
                )
        ontology_categories[accession_number] = categories
    return categories


def update_unit_ontology_set(
    current_set: Set[schemas.ValueAndUnitModel],
    table: IsaTable,
    row_id: int,
    term_column_index: int,
):
    (
        value,
        unit_term,
        term_source_ref,
        term_accession_number,
    ) = read_unit_ontology_columns(table, row_id, term_column_index)
    ontology = schemas.ValueAndUnitModel(
        value=rm_quotations(value),
        unit_term=rm_quotations(unit_term),
        term_source_ref=rm_quotations(term_source_ref),
        term_accession_number=rm_quotations(term_accession_number),
    )
    if (
        ontology.value
        or ontology.unit_term
        or ontology.term_accession_number
        or ontology.term_source_ref
    ):
        current_set.add(ontology)


def update_ontology_set(
    current_set: Set[schemas.OntologyModel],
    table: IsaTable,
    row_id: int,
    term_column_index: int,
):
    term, term_source_ref, term_accession_number = read_ontology_columns(
        table, row_id, term_column_index
    )
    ontology = schemas.OntologyModel(
        term=rm_quotations(term),
        term_source_ref=rm_quotations(term_source_ref),
        term_accession_number=rm_quotations(term_accession_number),
    )
    if ontology.term or ontology.term_accession_number or ontology.term_source_ref:
        current_set.add(ontology)


def read_ontology_columns(
    table: IsaTable, row_id: int, term_column_index: int
) -> Tuple[str, str, str]:
    next_header_name = table.columns[term_column_index]
    next_column = table.data[next_header_name]
    term = next_column[row_id] if next_column[row_id] else ""

    next_header_name = table.columns[term_column_index + 1]
    next_column = table.data[next_header_name]
    term_source_ref = next_column[row_id] if next_column[row_id] else ""

    next_header_name = table.columns[term_column_index + 2]
    next_column = table.data[next_header_name]
    term_accession_number = next_column[row_id] if next_column[row_id] else ""

    return (
        rm_quotations(term),
        rm_quotations(term_source_ref),
        rm_quotations(term_accession_number),
    )


def read_unit_ontology_columns(
    table: IsaTable, row_id: int, value_column_index: int
) -> Tuple[str, str, str, str]:
    next_header_name = table.columns[value_column_index]
    next_column = table.data[next_header_name]
    value = next_column[row_id] if next_column[row_id] else ""

    next_header_name = table.columns[value_column_index + 1]
    next_column = table.data[next_header_name]
    unit_term = next_column[row_id] if next_column[row_id] else ""

    next_header_name = table.columns[value_column_index + 2]
    next_column = table.data[next_header_name]
    term_source_ref = next_column[row_id] if next_column[row_id] else ""

    next_header_name = table.columns[value_column_index + 3]
    next_column = table.data[next_header_name]
    term_accession_number = next_column[row_id] if next_column[row_id] else ""

    return (
        rm_quotations(value),
        rm_quotations(unit_term),
        rm_quotations(term_source_ref),
        rm_quotations(term_accession_number),
    )


def rm_quotations(val: str):
    if not val:
        return val
    return val.strip().replace("'", "").strip().replace('"', "").strip()
