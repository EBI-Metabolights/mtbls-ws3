import asyncio
import datetime
import time
from logging import getLogger
from typing import Any, Dict, List, Set, Tuple

from metabolights_utils.models.enums import GenericMessageType
from metabolights_utils.models.isa.common import OrganismAndOrganismPartPair
from metabolights_utils.models.metabolights.model import MetabolightsStudyModel
from metabolights_utils.models.parser.enums import ParserMessageType
from pydantic import BaseModel

from mtbls.application.services.interfaces.repositories.study.study_read_repository import (
    StudyReadRepository,
)
from mtbls.application.services.interfaces.search_index_management_gateway import (
    IndexClientResponse,
    SearchIndexManagementGateway,
)
from mtbls.application.services.interfaces.study_metadata_service_factory import (
    StudyMetadataServiceFactory,
)
from mtbls.application.use_cases.indices.kibana_indices.models.assay_index import (
    ASSAY_FLOAT_VALUE_FIELDS,
    ASSAY_INT_VALUE_FIELDS,
    DEFAULT_ASSAY_COLUMN_NAMES,
    AssayIndexItem,
)
from mtbls.application.use_cases.indices.kibana_indices.models.assignment_index import (
    ASSIGNMENT_FLOAT_VALUE_FIELDS,
    ASSIGNMENT_INT_VALUE_FIELDS,
    DEFAULT_MAF_COLUMN_NAMES,
    DERIVED_ASSAY_COLUMN_NAMES,
    AssignmentIndexItem,
    MatchCategory,
)
from mtbls.application.use_cases.indices.kibana_indices.models.common import (
    BaseIsaTableIndexItem,
    IndexValueList,
)
from mtbls.application.use_cases.indices.kibana_indices.models.sample_index import (
    DEFAULT_SAMPLE_COLUMN_NAMES,
    FactorDescription,
    FactorIndexItem,
    SampleIndexItem,
)
from mtbls.application.use_cases.indices.kibana_indices.models.study_index import (
    StudyIndexItem,
)
from mtbls.application.use_cases.indices.kibana_indices.utils import (
    camel_case,
    create_study_index,
    find_studies_will_be_processed,
    get_isa_table_index_items,
    get_ontology_item,
    get_study_ids,
    get_text_value,
    is_selected_sample,
    load_json_file,
    merge_column_values,
)
from mtbls.application.utils.sort_utils import (
    sort_by_study_id,
)
from mtbls.domain.enums.filter_operand import FilterOperand
from mtbls.domain.enums.study_status import StudyStatus
from mtbls.domain.shared.repository.entity_filter import EntityFilter
from mtbls.domain.shared.repository.query_options import QueryFieldOptions

logger = getLogger(__name__)


class ActionPair(BaseModel):
    action: Dict[str, Any] = {}
    data: Dict[str, Any] = {}


class DataIndexConfiguration(BaseModel):
    sample_index_name: str = "sample-kibana-public"
    study_index_name: str = "study-kibana-public"
    assignment_index_name: str = "assignment-kibana-public"
    assay_index_name: str = "assay-kibana-public"

    study_index_mapping_file: str = (
        "resources/es/mappings/study_kibana_public_mappings.json"
    )
    assay_index_mapping_file: str = (
        "resources/es/mappings/assay_kibana_public_mappings.json"
    )
    sample_index_mapping_file: str = (
        "resources/es/mappings/sample_kibana_public_mappings.json"
    )
    assignment_index_mapping_file: str = (
        "resources/es/mappings/assignment_kibana_public_mappings.json"
    )
    target_study_status_list: list[StudyStatus] = [StudyStatus.PUBLIC]


class StudyModelEsIndexManager:
    def __init__(
        self,
        search_index_management_gateway: SearchIndexManagementGateway,
        study_metadata_service_factory: StudyMetadataServiceFactory,
        study_read_repository: StudyReadRepository,
        data_index_configuration: None | DataIndexConfiguration = None,
        reindex_studies: bool = False,
        reindex_samples: bool = True,
        reindex_assays: bool = False,
        reindex_assignments: bool = False,
        recreate_study_index: bool = False,
        recreate_sample_index: bool = False,
        recreate_assay_index: bool = False,
        recreate_assignment_index: bool = False,
        study_index_max_bulk_size: int = 10,
        assay_index_max_bulk_size: int = 500,
        sample_index_max_bulk_size: int = 500,
        assignment_index_max_bulk_size: int = 500,
        delete_ignore_status: List[int] = None,
        create_ignore_status: List[int] = None,
        concurrent: int = 10,
    ) -> None:
        if not data_index_configuration:
            data_index_configuration = DataIndexConfiguration()

        self.search_index_management_gateway = search_index_management_gateway
        self.study_read_repository = study_read_repository
        self.index_config = data_index_configuration
        self.study_metadata_service_factory = study_metadata_service_factory
        self.reindex_studies = reindex_studies
        self.reindex_assays = reindex_assays
        self.reindex_samples = reindex_samples
        self.reindex_assignments = reindex_assignments
        self.recreate_study_index = recreate_study_index
        self.recreate_sample_index = recreate_sample_index
        self.recreate_assay_index = recreate_assay_index
        self.recreate_assignment_index = recreate_assignment_index
        self.delete_ignore_status = delete_ignore_status
        self.create_ignore_status = create_ignore_status
        if not delete_ignore_status:
            self.delete_ignore_status = [404]
        if not create_ignore_status:
            self.create_ignore_status = [400]
        self.study_index_max_bulk_size = study_index_max_bulk_size
        self.assay_index_max_bulk_size = assay_index_max_bulk_size
        self.sample_index_max_bulk_size = sample_index_max_bulk_size
        self.assignment_index_max_bulk_size = assignment_index_max_bulk_size
        self.concurrent = concurrent if concurrent > 0 else 10

        self.successful_studies: asyncio.Queue = asyncio.Queue()
        self.failed_studies: asyncio.Queue = asyncio.Queue()

    async def update_indices(self):
        config = self.index_config
        for recreate_index, index_name, index_mapping_file in [
            (
                self.recreate_study_index,
                config.study_index_name,
                config.study_index_mapping_file,
            ),
            (
                self.recreate_sample_index,
                config.sample_index_name,
                config.sample_index_mapping_file,
            ),
            (
                self.recreate_assay_index,
                config.assay_index_name,
                config.assay_index_mapping_file,
            ),
            (
                self.recreate_assignment_index,
                config.assignment_index_name,
                config.assignment_index_mapping_file,
            ),
        ]:
            if recreate_index:
                await self.search_index_management_gateway.delete_index(
                    index_name, ignore_status=True
                )
                await asyncio.sleep(1)
            exist = await self.search_index_management_gateway.exists(index=index_name)

            if recreate_index or not exist:
                await self.search_index_management_gateway.create_index(
                    index=index_name,
                    mappings=load_json_file(index_mapping_file),
                    max_retries=2,
                )
                logger.info("%s index is recreated.", index_name)

    async def filter_selected_ids(
        self,
        selected_study_ids: Set[str],
        target_study_status_list: None | list[StudyStatus] = None,
    ) -> List[str]:
        try:
            if not selected_study_ids:
                return []
            if not target_study_status_list:
                target_study_status_list = [StudyStatus.PUBLIC]
            result = await self.study_read_repository.select_fields(
                query_field_options=QueryFieldOptions(
                    filters=[
                        EntityFilter(
                            key="status",
                            operand=FilterOperand.IN,
                            value=target_study_status_list,
                        )
                    ],
                    selected_fields=["accession_number"],
                )
            )

            filtered_study_ids = [
                x[0] for x in result.data if x and x[0] in selected_study_ids
            ]
            not_selected_studies = [
                x for x in selected_study_ids if x and x not in filtered_study_ids
            ]
            filtered_study_ids.sort(key=sort_by_study_id, reverse=True)
            logger.info("%s studies are selected.", len(filtered_study_ids))
            if not_selected_studies:
                logger.info(
                    "%s studies are removed from list: %s.",
                    len(filtered_study_ids),
                    not_selected_studies,
                )
            return filtered_study_ids
        except Exception as ex:
            raise ex

    async def get_study_model(self, study_id: str):
        metadata_service = await self.study_metadata_service_factory.create_service(
            study_id
        )
        with metadata_service:
            study_model = await metadata_service.load_study_model(
                load_assay_files=True,
                load_db_metadata=True,
                load_maf_files=self.reindex_assignments,
                load_sample_file=True,
            )
            if not study_model.investigation.studies:
                raise ValueError(
                    f"{study_id} definition (investigation.studies)"
                    " not found in investigation file."
                )
            errors = [
                x.detail
                for x in study_model.db_reader_messages
                if x.type in (GenericMessageType.CRITICAL, GenericMessageType.ERROR)
            ]
            if errors or study_model.study_db_metadata.study_id != study_id:
                raise ValueError(f"{study_id} db metadata is not valid.")

            for metadata_files in [
                study_model.assays,
                study_model.samples,
                study_model.metabolite_assignments,
            ]:
                for name in metadata_files:
                    messages = study_model.parser_messages.get(name)
                    if messages:
                        errors.extend(
                            [
                                x.detail
                                for x in messages
                                if x.type
                                in (
                                    ParserMessageType.CRITICAL,
                                    ParserMessageType.ERROR,
                                )
                            ]
                        )
                    if errors:
                        raise ValueError(
                            f"{study_id} {name} metadata file parse errors: {errors}"
                        )

        logger.debug("%s is loaded.", study_id)
        return study_model

    async def reindex_study_item(self, study_index_item: StudyIndexItem) -> None:
        study_id = study_index_item.identifier

        if self.reindex_studies:
            action = {
                "index": {
                    "_index": self.index_config.study_index_name,
                    "_id": study_id,
                }
            }
            data = study_index_item.model_dump(by_alias=True)
            action_pairs = []
            action_pairs.append(ActionPair(action=action, data=data))
            logger.info(
                "%s is updated on index %s",
                study_id,
                self.index_config.study_index_name,
            )
            await self.bulk_index(
                self.index_config.study_index_name,
                actions=action_pairs,
                batch_size=self.study_index_max_bulk_size,
            )

    async def create_study_item(
        self, study_model: MetabolightsStudyModel
    ) -> Tuple[StudyIndexItem, Dict[str, FactorDescription]]:
        if not study_model:
            return {}

        logger.debug("%s starting study index", study_model.study_db_metadata.study_id)
        study_index_item = await create_study_index(study_model)
        study_index_item.object_type = "STUDY"

        factor_fields = {}
        if self.reindex_samples or self.reindex_assays or self.reindex_assignments:
            for factor in study_index_item.factors:
                factor_desc = FactorDescription()
                factor_desc.term = factor.type.term
                factor_desc.term_accession_number = factor.type.term_accession_number
                factor_desc.term_source_ref = factor.type.term_source_ref
                factor_desc.name = factor.name
                factor_fields[f"Factor Value[{factor.name}]"] = factor_desc
        return study_index_item, factor_fields

    async def bulk_index(
        self, index_name: str, actions: list[ActionPair], batch_size: int = 1000
    ):
        action_list = []
        if not actions or not index_name or batch_size <= 1:
            return
        for action in actions:
            action_list.append(action.action)
            action_list.append(action.data)
        for idx, batch in enumerate(
            self.chunk_list(action_list, batch_size=batch_size * 2)
        ):
            result = await self.search_index_management_gateway.bulk(
                index=index_name, operations=batch
            )
            self.evaluate_results(result)
            logger.debug(
                "'%s' index: bulk index task %s is done. # of the indexed items: %s",
                index_name,
                idx + 1,
                int(len(batch) / 2),
            )
        logger.debug(
            "'%s' index: bulk index task done. Total indexed items: %s",
            index_name,
            actions,
        )

    def chunk_list(self, data, batch_size):
        for i in range(0, len(data), batch_size):
            yield data[i : i + batch_size]

    def evaluate_results(self, result: IndexClientResponse):
        if not result.raw["errors"]:
            return

        for item in result.body["items"]:
            if "index" in item and "status" in item["index"]:
                if item["index"]["status"] >= 400:
                    logger.warning(
                        "Index is failed for %s - %s ",
                        item["index"]["_id"],
                        item["index"]["error"],
                    )
                    raise Exception(f"{item['index']['_id']}, {item['index']['error']}")

    async def reindex_sample_items(
        self,
        study_model: MetabolightsStudyModel,
        factor_fields: Dict[str, FactorDescription] = None,
    ) -> Dict[str, OrganismAndOrganismPartPair]:
        samples_map: Dict[str, OrganismAndOrganismPartPair] = {}
        if not (
            self.reindex_samples or self.reindex_assays or self.reindex_assignments
        ):
            return samples_map
        action_pairs = []
        if not factor_fields:
            factor_fields = {}
        study = study_model.investigation.studies[0]
        table_file = study_model.samples[study.file_name]
        study_id = study_model.study_db_metadata.study_id
        logger.debug("%s starting sample index", study_id)

        prefix = f"{study_id}-S"
        sample_items: List[BaseIsaTableIndexItem] = get_isa_table_index_items(
            study_model, prefix, table_file, DEFAULT_SAMPLE_COLUMN_NAMES
        )

        for idx, item in enumerate(sample_items):
            if idx % 100 == 0 and idx > 0:
                logger.debug(
                    "%s %s processes samples: %s", study_id, study.file_name, idx
                )
            sample_item = SampleIndexItem.model_validate(item, from_attributes=True)
            sample_item.object_type = "SAMPLE"
            sample_item.file_name = study.file_name
            sample_item.file_id = prefix
            selected_factor_values = []
            factor_name_set = set()
            factor_term_set = set()
            for additinal_item in sample_item.additional_fields:
                if additinal_item.header_name in factor_fields:
                    factor_desc = factor_fields[additinal_item.header_name]
                    factor_index = FactorIndexItem(
                        data=additinal_item.data, desc=factor_desc
                    )
                    sample_item.factors.append(factor_index)
                    if factor_desc.name:
                        factor_name_set.add(factor_desc.name)
                    if factor_desc.term_source_ref:
                        factor_term_set.add(
                            f"{factor_desc.term_source_ref}:{factor_desc.term}"
                        )
                    elif factor_desc.term:
                        factor_term_set.add(f"{factor_desc.term}")

                    selected_factor_values.append(additinal_item)
            sample_item.factor_header_names = list(factor_name_set)
            sample_item.factor_terms = list(factor_term_set)
            for val in selected_factor_values:
                sample_item.additional_fields.remove(val)

            if self.reindex_samples:
                action = {
                    "index": {
                        "_index": self.index_config.sample_index_name,
                        "_id": sample_item.identifier,
                    }
                }
                data = sample_item.model_dump(by_alias=True)
                action_pairs.append(ActionPair(action=action, data=data))

            if self.reindex_samples or self.reindex_assays or self.reindex_assignments:
                organism = get_ontology_item(
                    "characteristicsOrganism", sample_item.fields
                )
                organism_part = get_ontology_item(
                    "characteristicsOrganismPart", sample_item.fields
                )
                variant = get_ontology_item(
                    "characteristicsVariant", sample_item.fields
                )
                sample_type = get_ontology_item(
                    "characteristicsSampleType]", sample_item.fields
                )
                sample_name = get_text_value("sampleName", sample_item.fields)
                if sample_name:
                    samples_map[sample_name] = OrganismAndOrganismPartPair(
                        organism=organism,
                        organismPart=organism_part,
                        variant=variant,
                        sampleType=sample_type,
                    )

        if self.reindex_samples and action_pairs:
            logger.info("%s sample documents: %s", study_id, len(sample_items))
            await self.bulk_index(
                self.index_config.sample_index_name,
                actions=action_pairs,
                batch_size=self.sample_index_max_bulk_size,
            )
        return samples_map

    async def reindex_assay_items(
        self,
        study_model: MetabolightsStudyModel,
        samples_map: Dict[str, OrganismAndOrganismPartPair],
    ) -> Dict[str, Dict[str, Dict[str, List[AssayIndexItem]]]]:
        assays: Dict[str, Dict[str, Dict[str, List[AssayIndexItem]]]] = {}
        if not (self.reindex_assays or self.reindex_assignments):
            return assays

        study_id = study_model.study_db_metadata.study_id
        logger.info("%s starting assay reindex", study_id)
        assay_order = 0
        for assay_description in study_model.investigation.studies[
            0
        ].study_assays.assays:
            action_pairs = []
            assay_order += 1
            assay_file_name = assay_description.file_name
            table_file = study_model.assays[assay_description.file_name]
            assay_table = study_model.assays[assay_description.file_name].table
            prefix = f"{study_id}-A-{assay_order:02}"
            assay_items: List[BaseIsaTableIndexItem] = get_isa_table_index_items(
                study_model,
                prefix,
                table_file,
                DEFAULT_ASSAY_COLUMN_NAMES,
                int_fields=ASSAY_INT_VALUE_FIELDS,
                float_fields=ASSAY_FLOAT_VALUE_FIELDS,
                skip_additional_fields=True,
            )
            file_headers = [
                camel_case(f.column_header)
                for f in assay_table.headers
                if f.column_header.endswith(" File")
            ]
            for idx, item in enumerate(assay_items):
                if idx % 100 == 0 and idx > 0:
                    logger.debug(
                        "%s %s processes assay rows: %s", study_id, assay_file_name, idx
                    )
                assay_item = AssayIndexItem.model_validate(item, from_attributes=True)
                assay_item.fields = item.fields
                assay_item.file_id = prefix
                assay_item.file_name = assay_file_name
                assay_item.additional_fields = item.additional_fields
                assay_item.invalid_values = item.invalid_values
                assay_item.object_type = "ASSAY"
                assay_item.technique = study_model.assays[
                    assay_description.file_name
                ].assay_technique
                assay_item.sample_name = get_text_value("sampleName", assay_item.fields)
                if not assay_item.sample_name.strip():
                    logger.warning(
                        "%s Empty sample name in assay file Row: %s %s - %s",
                        study_id,
                        idx + 1,
                        assay_description.file_name,
                        assay_item.identifier,
                    )
                elif assay_item.sample_name in samples_map:
                    assay_item.sample = samples_map[assay_item.sample_name]
                else:
                    logger.warning(
                        "%s Unreferenced Sample %s in assay file Row: %s %s - %s",
                        study_id,
                        assay_item.sample_name,
                        idx + 1,
                        assay_description.file_name,
                        assay_item.identifier,
                    )

                for field in file_headers:
                    if (
                        field in assay_item.fields
                        and assay_item.fields[field]
                        and assay_item.fields[field][0]
                        and assay_item.fields[field][0].value
                    ):
                        assay_item.files[field] = assay_item.fields[field]
                if self.reindex_assignments:
                    maf_file = get_text_value(
                        "metaboliteAssignmentFile", assay_item.fields
                    )
                    if maf_file not in assays:
                        assays[maf_file] = {}
                    if assay_file_name not in assays[maf_file]:
                        assays[maf_file][assay_file_name] = {}
                    if assay_item.sample_name not in assays[maf_file][assay_file_name]:
                        assays[maf_file][assay_file_name][assay_item.sample_name] = []
                    assays[maf_file][assay_file_name][assay_item.sample_name].append(
                        assay_item
                    )

                if self.reindex_assays:
                    action = {
                        "index": {
                            "_index": self.index_config.assay_index_name,
                            "_id": assay_item.identifier,
                        }
                    }
                    data = assay_item.model_dump(by_alias=True)
                    action_pairs.append(ActionPair(action=action, data=data))

            if self.reindex_assays and action_pairs:
                await self.bulk_index(
                    self.index_config.assay_index_name,
                    actions=action_pairs,
                    batch_size=self.assay_index_max_bulk_size,
                )
                logger.info(
                    "%s assay documents is created for %s items: %s",
                    study_id,
                    assay_file_name,
                    len(assay_items),
                )
        return assays

    async def reindex_assignment_items(
        self,
        study_model: MetabolightsStudyModel,
        samples_map: Dict[str, OrganismAndOrganismPartPair],
        assays: Dict[str, Dict[str, Dict[str, List[AssayIndexItem]]]],
    ) -> None:
        assignment_order = 0

        for maf_filename in study_model.metabolite_assignments:
            action_pairs = []
            assignment_order += 1
            study_id = study_model.study_db_metadata.study_id
            table_file = study_model.metabolite_assignments[maf_filename]
            assignment_table = table_file.table
            skip_fields = set()
            for assay_file_ref in assays[table_file.file_path]:
                assay_file_item = study_model.assays[assay_file_ref]
                skip_fields.update(assay_file_item.assay_names or [])
                skip_fields.update(assay_file_item.sample_names or [])

            prefix = f"{study_id}-M-{assignment_order:02}"
            assignment_items: List[BaseIsaTableIndexItem] = get_isa_table_index_items(
                study_model,
                prefix,
                table_file,
                DEFAULT_MAF_COLUMN_NAMES,
                skip_additional_fields=True,
                skip_fields=skip_fields,
                skip_additional_ontology_fields=True,
                int_fields=ASSIGNMENT_INT_VALUE_FIELDS,
                float_fields=ASSIGNMENT_FLOAT_VALUE_FIELDS,
            )
            row_index = 0

            sample_names = set(samples_map.keys())
            sample_names_in_maf_file = {
                f.column_header
                for f in assignment_table.headers
                if f.column_header in sample_names
            }

            column_unique_values = {}
            for assay_file_ref in assays[table_file.file_path]:
                assay_file_item = study_model.assays[assay_file_ref]

                selected_headers = {
                    f.column_header
                    for f in assay_file_item.table.headers
                    if f.column_header in DERIVED_ASSAY_COLUMN_NAMES
                }
                for derived_assay_column_name in selected_headers:
                    field_name = camel_case(derived_assay_column_name)
                    value_list = IndexValueList()
                    unique_values = set()
                    for sample in assays[table_file.file_path][assay_file_ref]:
                        assay_items: List[AssayIndexItem] = assays[
                            table_file.file_path
                        ][assay_file_ref][sample]
                        for assay_item in assay_items:
                            if field_name in assay_item.fields:
                                field_values = assay_item.fields[field_name]
                                for field_value in field_values:
                                    if field_value and field_value.value:
                                        unique_values.add(field_value)
                    if unique_values:
                        value_list = list(unique_values)
                        column_unique_values[field_name] = value_list

                merge_column_values(
                    [
                        "parameterValueColumnModel",
                        "parameterValueColumnModel1",
                        "parameterValueColumnModel2",
                    ],
                    "parameterValueColumnModel",
                    column_unique_values,
                )
                merge_column_values(
                    [
                        "parameterValueColumnType",
                        "parameterValueColumnType1",
                        "parameterValueColumnType2",
                    ],
                    "parameterValueColumnType",
                    column_unique_values,
                )
            for idx, item in enumerate(assignment_items):
                if idx % 1000 == 0 and idx > 0:
                    logger.debug(
                        "%s %s processes assignment rows: %s",
                        study_id,
                        table_file.file_path,
                        idx,
                    )
                row_index += 1
                organisms = set()
                assignment_item = AssignmentIndexItem.model_validate(
                    item, from_attributes=True
                )
                assignment_item.object_type = "ASSIGNMENT"
                assignment_item.technique = table_file.assay_technique
                assignment_item.row_index = row_index
                assignment_item.file_name = maf_filename
                assignment_item.file_id = prefix
                assignment_item.meta = column_unique_values
                assignment_item.invalid_values = item.invalid_values

                for sample_name_column in sample_names_in_maf_file:
                    sample_value = assignment_table.data[sample_name_column][
                        row_index - 1
                    ]

                    if sample_value and sample_value != "0":
                        assignment_item.sample_match_category = (
                            MatchCategory.QUANTITATIVE
                        )

                    for assay_file_ref in assays[table_file.file_path]:
                        if (
                            sample_name_column
                            in assays[table_file.file_path][assay_file_ref]
                        ):
                            samples = assays[table_file.file_path][assay_file_ref][
                                sample_name_column
                            ]
                            if samples:
                                first_assay_item: AssayIndexItem = samples[0]
                                organism_term = first_assay_item.sample.organism.term
                                if (
                                    organism_term not in organisms
                                    and is_selected_sample(first_assay_item.sample)
                                ):
                                    organisms.add(organism_term)
                                    assignment_item.samples.append(
                                        first_assay_item.sample
                                    )
                        else:
                            pass

                if self.reindex_assignments:
                    action = {
                        "index": {
                            "_index": self.index_config.assignment_index_name,
                            "_id": assignment_item.identifier,
                        }
                    }
                    data = assignment_item.model_dump(by_alias=True)
                    action_pairs.append(ActionPair(action=action, data=data))

            if self.reindex_assignments and action_pairs:
                await self.bulk_index(
                    self.index_config.assignment_index_name,
                    actions=action_pairs,
                    batch_size=self.assignment_index_max_bulk_size,
                )
                logger.info(
                    "%s assignment documents are created for %s items: %s",
                    study_id,
                    maf_filename,
                    len(assignment_items),
                )

    async def reindex_task(self, study_id: str):
        try:
            logger.info("%s starting study index", study_id)

            study_model = await self.get_study_model(study_id)
            if self.reindex_studies:
                await self.delete_index_documents([study_id])
            study_index_item, factor_fields = await self.create_study_item(
                study_model=study_model
            )
            samples_map = await self.reindex_sample_items(
                study_model=study_model, factor_fields=factor_fields
            )
            assays = await self.reindex_assay_items(
                study_model=study_model, samples_map=samples_map
            )
            await self.reindex_assignment_items(
                study_model=study_model, samples_map=samples_map, assays=assays
            )
            await self.reindex_study_item(study_index_item=study_index_item)
        except Exception as ex:
            self.failed_studies.put_nowait((study_id, str(ex)))
            import traceback

            traceback.print_exc()
            return f"{study_id} indexing task failed with error {ex}"

        self.successful_studies.put_nowait(study_id)
        return f"{study_id} indexing is completed"

    async def reindex_study_models(
        self,
        new_study_ids: None | list[str] = None,
        updated_study_ids: None | list[str] = None,
        deleted_study_ids: None | list[str] = None,
    ):
        if not new_study_ids:
            new_study_ids = []
        if not updated_study_ids:
            updated_study_ids = []
        if not deleted_study_ids:
            deleted_study_ids = []
        start_time = time.time()
        if len(updated_study_ids) == 0:
            logger.info("There is no study to reindex")
        if len(deleted_study_ids) == 0:
            logger.info("There is no study index to delete")
        else:
            await self.delete_index_documents(deleted_study_ids)

        study_ids = updated_study_ids
        study_ids.extend(new_study_ids or [])
        if len(study_ids) == 0:
            return
        await self.update_indices()

        logger.info("Number of indexed studies: %s", len(study_ids))
        try:
            max_concurrent = min(len(study_ids), self.concurrent)

            sem = asyncio.Semaphore(max_concurrent)

            async def bounded(study_id):
                async with sem:
                    return await self.reindex_task(study_id)

            tasks = [asyncio.create_task(bounded(sid)) for sid in study_ids]

            for t in asyncio.as_completed(tasks):
                try:
                    result = await t
                    logger.info(result)
                except Exception:
                    logger.exception("reindex_task failed")

            total_process_time = f"{((time.time() - start_time) / 60.0):04f} mins"
            logger.info(
                "Total time for %s assays: %s", len(study_ids), total_process_time
            )
        finally:
            logger.info("Completion of reindex tasks...")

    async def delete_index_documents(self, deleted_study_ids: set[str]):
        for study_id in deleted_study_ids:
            response = await self.search_index_management_gateway.delete_by_query(
                self.index_config.assignment_index_name,
                body={"query": {"match": {"studyId": study_id}}},
            )
            logger.debug(
                "Deleted %s assignment documents: %s",
                study_id,
                response.raw.get("deleted", 0),
            )
            response = await self.search_index_management_gateway.delete_by_query(
                self.index_config.assay_index_name,
                body={"query": {"match": {"studyId": study_id}}},
            )
            logger.debug(
                "Deleted %s assay documents: %s",
                study_id,
                response.raw.get("deleted", 0),
            )
            response = await self.search_index_management_gateway.delete_by_query(
                self.index_config.sample_index_name,
                body={"query": {"match": {"studyId": study_id}}},
            )

            logger.debug(
                "Deleted %s sample documents: %s",
                study_id,
                response.raw.get("deleted", 0),
            )
            response = await self.search_index_management_gateway.delete_by_query(
                self.index_config.study_index_name,
                body={"query": {"match": {"reservedAccession": study_id}}},
            )
            if response.raw.get("deleted", 0) == 0:
                logger.warning(
                    "%s study document is not deleted. There is no index %s or document",
                    study_id,
                    self.index_config.study_index_name,
                )
            else:
                logger.debug("Deleted study document: %s ", study_id)


async def log_result(indexer: StudyModelEsIndexManager):
    successful_studies = []
    while not indexer.successful_studies.empty():
        successful_studies.append(await indexer.successful_studies.get())
    successful_studies.sort(key=sort_by_study_id)
    failed_studies = []
    while not indexer.failed_studies.empty():
        failed_studies.append(await indexer.failed_studies.get())

    failed_studies.sort(key=lambda x: sort_by_study_id(x[0]))
    if successful_studies:
        logger.info("Successfully indexed studies: %s", successful_studies)
    else:
        logger.info("There is no successfully indexed studies")
    if failed_studies:
        studies_str = "\n".join([f"'{x[0]} -> {x[1]}'" for x in failed_studies])
        logger.warning("Failed studies: %s", studies_str)


async def reindex_all(
    search_index_management_gateway: SearchIndexManagementGateway,
    study_metadata_service_factory: StudyMetadataServiceFactory,
    study_read_repository: StudyReadRepository,
    data_index_configuration: None | DataIndexConfiguration = None,
    min_last_update_date: None | datetime.datetime = None,
    max_last_update_date: None | datetime.datetime = None,
    exclude_studies: None | list[str] = None,
):
    indexer = StudyModelEsIndexManager(
        search_index_management_gateway=search_index_management_gateway,
        study_metadata_service_factory=study_metadata_service_factory,
        data_index_configuration=data_index_configuration,
        study_read_repository=study_read_repository,
        recreate_study_index=True,
        recreate_assay_index=True,
        recreate_sample_index=True,
        recreate_assignment_index=True,
        reindex_studies=True,
        reindex_samples=True,
        reindex_assays=True,
        reindex_assignments=True,
        concurrent=10,
        study_index_max_bulk_size=10,
        assay_index_max_bulk_size=1000,
        sample_index_max_bulk_size=1000,
        assignment_index_max_bulk_size=2000,
        delete_ignore_status=[],
        create_ignore_status=[],
    )
    study_ids = await get_study_ids(
        target_study_status_list=indexer.index_config.target_study_status_list,
        exclude_studies=exclude_studies,
        min_last_update_date=min_last_update_date,
        max_last_update_date=max_last_update_date,
    )

    await indexer.reindex_study_models(updated_study_ids=study_ids)
    await log_result(indexer)


async def maintain_indices(
    search_index_management_gateway: SearchIndexManagementGateway,
    study_metadata_service_factory: StudyMetadataServiceFactory,
    study_read_repository: StudyReadRepository,
    data_index_configuration: None | DataIndexConfiguration = None,
):
    indexer = StudyModelEsIndexManager(
        search_index_management_gateway=search_index_management_gateway,
        study_metadata_service_factory=study_metadata_service_factory,
        data_index_configuration=data_index_configuration,
        study_read_repository=study_read_repository,
        recreate_study_index=False,
        recreate_assay_index=False,
        recreate_sample_index=False,
        recreate_assignment_index=False,
        reindex_studies=True,
        reindex_samples=True,
        reindex_assays=True,
        reindex_assignments=True,
        concurrent=2,
        study_index_max_bulk_size=10,
        assay_index_max_bulk_size=1000,
        sample_index_max_bulk_size=1000,
        assignment_index_max_bulk_size=2000,
        delete_ignore_status=[],
        create_ignore_status=[],
    )
    (
        new_study_ids,
        updated_study_ids,
        deleted_study_ids,
    ) = await find_studies_will_be_processed(
        study_read_repository=study_read_repository,
        search_index_management_gateway=search_index_management_gateway,
        index_name=indexer.index_config.study_index_name,
        index_update_field="lastUpdateDatetime",
        db_update_field="update_date",
        target_study_status_list=indexer.index_config.target_study_status_list,
    )

    await indexer.reindex_study_models(
        new_study_ids=new_study_ids,
        updated_study_ids=updated_study_ids,
        deleted_study_ids=deleted_study_ids,
    )
    await log_result(indexer)


async def reindex_selected_studies(
    selected_studies: List[str],
    search_index_management_gateway: SearchIndexManagementGateway,
    study_metadata_service_factory: StudyMetadataServiceFactory,
    study_read_repository: StudyReadRepository,
    data_index_configuration: None | DataIndexConfiguration = None,
):
    indexer = StudyModelEsIndexManager(
        search_index_management_gateway=search_index_management_gateway,
        study_metadata_service_factory=study_metadata_service_factory,
        data_index_configuration=data_index_configuration,
        study_read_repository=study_read_repository,
        recreate_study_index=False,
        recreate_assay_index=False,
        recreate_sample_index=False,
        recreate_assignment_index=False,
        reindex_studies=True,
        reindex_samples=True,
        reindex_assays=True,
        reindex_assignments=True,
        concurrent=5,
        delete_ignore_status=[],
        create_ignore_status=[],
    )
    study_ids = await indexer.filter_selected_ids(
        selected_study_ids=selected_studies,
        target_study_status_list=indexer.index_config.target_study_status_list,
    )
    if study_ids:
        await indexer.reindex_study_models(updated_study_ids=study_ids)
    else:
        logger.error(
            "Selected studies do not exist or not in the requested study status."
        )
    await log_result(indexer)
