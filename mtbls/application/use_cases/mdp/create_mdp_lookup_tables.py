import csv
import logging
import re
from importlib import resources
from typing import List

import unidecode
from metabolights_utils.common import Path, sort_by_study_id
from metabolights_utils.models.isa.common import IsaTable, IsaTableFile
from metabolights_utils.models.metabolights.model import MetabolightsStudyModel
from pydantic import BaseModel

import mtbls
from mtbls.application.services.interfaces.repositories.study.study_read_repository import (
    StudyReadRepository,
)
from mtbls.application.services.interfaces.study_metadata_service_factory import (
    StudyMetadataServiceFactory,
)
from mtbls.application.use_cases.mdp.model import (
    ColumnType,
    Polarity,
    StudyLookup,
    StudyMsAssayFileLookup,
    StudyMsAssayLookup,
)
from mtbls.application.use_cases.validation.utils import evaulate_mtbls_model
from mtbls.domain.enums.study_status import StudyStatus
from mtbls.domain.shared.repository.entity_filter import EntityFilter
from mtbls.domain.shared.repository.query_options import QueryFieldOptions

logger = logging.getLogger(__name__)

PUBLIC_STUDY_FTP_BASE_URL = (
    "ftp://ftp.ebi.ac.uk/pub/databases/metabolights/studies/public"
)


class StudyCollection(BaseModel):
    submissions: List[MetabolightsStudyModel] = []


file_extensions = {
    r"^[ -~]+\.raw$": ".raw",
    r"^[ -~]+\.d$": ".d",
    r"^[ -~]+\.wiff$": ".wiff",
    r"^[ -~]+\.wiff\.scan$": ".wiff.scan",
    r"^[ -~]+\.raw\.zip$": ".raw.zip",
    r"^[ -~]+\.d\.zip$": ".d.zip",
    r"^[ -~]+\.wiff\.scan\.zip$": ".wiff.scan.zip",
    r"^[ -~]+\.mzml$": ".mzML",
    r"^[ -~]+\.mzxml$": ".mzXML",
}

derived_file_patterns = [
    r"^[ -~]+\.mzml$",
    r"^[ -~]+\.mzxml$",
    r"^[ -~]+\.raw$",
    r"^[ -~]+\.d$",
    r"^[ -~]+\.wiff$",
    r"^[ -~]+\.wiff\.scan$",
    r"^[ -~]+\.raw\.zip$",
    r"^[ -~]+\.d\.zip$",
    r"^[ -~]+\.wiff\.scan\.zip$",
]

raw_file_patterns = [
    r"^[ -~]+\.mzml$",
    r"^[ -~]+\.mzxml$",
    r"^[ -~]+\.raw$",
    r"^[ -~]+\.d$",
    r"^[ -~]+\.wiff$",
    r"^[ -~]+\.wiff.scan$",
    r"^[ -~]+\.raw\.zip$",
    r"^[ -~]+\.d\.zip$",
    r"^[ -~]+\.wiff\.scan.zip$",
]

filtered_file_extensions = [
    ".mzml",
    ".mzxml",
    ".raw",
    ".d",
    ".wiff",
    ".raw.zip",
    ".d.zip",
]

double_extension_set = {"scan", "zip"}
extensions_set = {file_extensions[x] for x in file_extensions}


def get_extension(data: str):
    if not data:
        return ""
    index = data.rfind(".")
    if index > 0:
        parts = data.lstrip(".").split(".")
        if len(parts) > 2:
            if parts[-1].lower() == "zip" and len(parts[-2]) <= 4:
                return f".{parts[-2]}.{parts[-1]}".lower()
        elif len(parts) > 1:
            return f".{parts[-1]}".lower()
    return ""


def get_column_values(
    study_model: MetabolightsStudyModel,
    assay_name: str,
    search_column,
    process_data_function,
):
    columns = study_model.assays[assay_name].table.columns
    data_list = values = study_model.assays[assay_name].table.data
    result = set()
    if search_column in columns:
        values = data_list[search_column]
        values_set = set(f.upper() for f in values) or set()
        for item in values_set:
            result.add(process_data_function(item))
    return result


def match_by_column_name_and_value_list(
    study_id,
    study_model: MetabolightsStudyModel,
    assay_name,
    search_column,
    search_column_values,
):
    columns = study_model.assays[assay_name].table.columns
    data_list = values = study_model.assays[assay_name].table.data
    if search_column in columns:
        values = data_list[search_column]
        values_set = set(f.upper().strip() for f in values if f.strip()) or set()
        values_set.discard("")
        values_set.discard(None)
        if len(values_set) == 1:
            for value in search_column_values:
                if values[0].upper() == value.upper():
                    return True, values_set, ""

            return (
                False,
                values_set,
                f"Not an expected value '{values[0]}' expected value: {str(search_column_values)}",
            )
        else:
            if values_set:
                return False, values_set, f"Contains multiple values<{str(values_set)}>"
            else:
                return False, values_set, "No values"
    else:
        return False, set(), f"Does not contain column: <{str(search_column)}>"


def match_by_column_name_and_value_pattern(
    study_model: MetabolightsStudyModel,
    assay_name,
    search_column,
    search_column_patterns,
    process_data_function,
):
    columns = study_model.assays[assay_name].table.columns
    data_list = values = study_model.assays[assay_name].table.data
    unmatched_extensions = set()
    matched_extensions = set()
    if search_column in columns:
        values = data_list[search_column]
        values_set = set(f.upper() for f in values) or set()

        for column_value in values_set:
            matched = False
            for pattern in search_column_patterns:
                if re.match(pattern, column_value.lower()):
                    matched = True
                    break
            processed_data = process_data_function(column_value)
            if not matched:
                unmatched_extensions.add(processed_data)
            else:
                matched_extensions.add(processed_data)

        matched_extensions.discard("")
        matched_extensions.discard(None)

        if len(matched_extensions) > 1:
            return False, unmatched_extensions, "Match with multiple file extensions"
        elif len(matched_extensions) == 1:
            return True, matched_extensions, ""
        else:
            return False, set(), "Not matched with any pattern"
    else:
        return False, set(), f"Does not contain column: <{str(search_column)}>"


def find_unique_values(study_model: MetabolightsStudyModel, assay_name, search_column):
    columns = study_model.assays[assay_name].table.columns
    data_list = values = study_model.assays[assay_name].table.data
    if search_column in columns:
        values = data_list[search_column]
        values_set = set(f.lower() for f in values if f.strip()) or set()
        if len(values_set) > 0:
            return True, values_set, ""
        else:
            return False, set(), "No value is found"
    else:
        return False, set(), f"Does not contain column: <{str(search_column)}>"


def check_assay(study_id: str, study_model: MetabolightsStudyModel, assay_file: str):
    column_type_found = False
    column_model_found = False
    scan_polarity_found = False
    raw_files_found = False
    derived_files_found = False
    column_models = set()
    column_types = set()
    scan_polarities = set()

    column_type_found, column_types, msg = match_by_column_name_and_value_list(
        study_id,
        study_model,
        assay_file,
        "Parameter Value[Column type]",
        [
            "HILIC",
            "REVERSE PHASE",
            "REVERSE-PHASE",
            "REVERSED PHASE",
            "REVERSE PHASE ",
            "RESERVE PHASE",
        ],
    )
    column_model_found, column_models, _ = find_unique_values(
        study_model, assay_file, "Parameter Value[Column model]"
    )

    column_models.discard("")
    column_models.discard(None)
    column_types.discard("")
    column_types.discard(None)
    if column_type_found and column_model_found:
        scan_polarity_found, scan_polarities, msg = match_by_column_name_and_value_list(
            study_id,
            study_model,
            assay_file,
            "Parameter Value[Scan polarity]",
            [
                "negative",
                "negitive",
                "negative ",
                "negarive",
                "negtive",
                "negative scan",
                "negative ionization",
                "positive",
                "positive ",
                "positive scan",
                "alternating",
                "alternating scan",
                "alternative",
            ],
        )
        if scan_polarity_found:
            column_names = study_model.assays[assay_file].table.columns
            raw_file_columns = [
                x for x in column_names if x.startswith("Raw Spectral Data File")
            ]
            all_raw_file_patterns_set = set()
            raw_files_found = False
            for column_name in raw_file_columns:
                found, raw_file_patterns_set, msg = (
                    match_by_column_name_and_value_pattern(
                        study_model,
                        assay_file,
                        column_name,
                        raw_file_patterns,
                        get_extension,
                    )
                )
                raw_file_patterns_set.discard("")
                raw_file_patterns_set.discard(None)
                if found:
                    all_raw_file_patterns_set.update(raw_file_patterns_set)
                    raw_files_found = found
            derived_file_columns = [
                x for x in column_names if x.startswith("Derived Spectral Data File")
            ]
            all_derived_file_patterns_set = set()
            derived_files_found = False
            for column_name in derived_file_columns:
                found, derived_file_patterns_set, msg = (
                    match_by_column_name_and_value_pattern(
                        study_model,
                        assay_file,
                        column_name,
                        derived_file_patterns,
                        get_extension,
                    )
                )
                derived_file_patterns_set.discard("")
                derived_file_patterns_set.discard(None)
                if found:
                    all_derived_file_patterns_set.update(derived_file_patterns_set)
                    derived_files_found = found

            if raw_files_found or derived_files_found:
                all_raw_file_patterns_set = (
                    all_raw_file_patterns_set
                    if raw_files_found
                    else all_raw_file_patterns_set.intersection(extensions_set)
                )
                all_derived_file_patterns_set = (
                    all_derived_file_patterns_set
                    if derived_files_found
                    else all_derived_file_patterns_set.intersection(extensions_set)
                )
                # assay_candidates["matched"].append(
                return (
                    study_id,
                    assay_file,
                    [
                        column_types,
                        column_models,
                        scan_polarities,
                        all_raw_file_patterns_set,
                        all_derived_file_patterns_set,
                    ],
                    "matched",
                    "",
                )

                # study_candidates.add(study_id)
            else:
                raw_file_extensions = get_column_values(
                    study_model, assay_file, "Raw Spectral Data File", get_extension
                )
                derived_file_extensions = get_column_values(
                    study_model, assay_file, "Derived Spectral Data File", get_extension
                )
                raw_file_extensions.discard("")
                raw_file_extensions.discard(None)
                derived_file_extensions.discard("")
                derived_file_extensions.discard(None)
                return (
                    study_id,
                    assay_file,
                    [
                        column_types,
                        column_models,
                        scan_polarities,
                        raw_file_extensions,
                        derived_file_extensions,
                    ],
                    "unmatched-file_extension",
                    str(
                        [
                            column_types,
                            column_models,
                            scan_polarities,
                            raw_file_extensions,
                            derived_file_extensions,
                        ]
                    ),
                )

        else:
            return (
                study_id,
                assay_file,
                [column_types, column_models, set(), set(), set()],
                "unmatched-scan-polarity",
                msg,
            )
    else:
        _, column_types, msg = find_unique_values(
            study_model, assay_file, "Parameter Value[Column type]"
        )
        _, column_models, msg = find_unique_values(
            study_model, assay_file, "Parameter Value[Column model]"
        )
        # if found1 and found2:
        #     return (
        #         study_id,
        #         assay_file,
        #         [column_types, column_models, set(), set(), set()],
        #         "unmatched-column-type",
        #         f"Unmatched column types '{', '.join(column_types)}, models: '{', '.join(column_models)}'",
        #     )
        # else:
        return (
            study_id,
            assay_file,
            [column_types, column_models, set(), set(), set()],
            "unmatched-column-type",
            f"Unmatched column types '{', '.join(column_types)}, models: '{', '.join(column_models)}'",
        )


# def get_study_ids(
#     connection,
#     exclude_studies: Set[str] = None,
#     min_last_update_date: datetime.datetime = None,
#     max_last_update_date: datetime.datetime = None,
# ) -> List[str]:
#     try:
#         if not exclude_studies:
#             exclude_studies = set()
#         db_metadata_collector = DbMetadataCollector()
#         studies = db_metadata_collector.get_updated_public_study_ids_from_db(
#             connection=connection,
#             min_last_update_date=min_last_update_date,
#             max_last_update_date=max_last_update_date,
#         )
#         study_ids = [study["acc"] for study in studies if study and study["acc"] not in exclude_studies]
#         study_ids.sort(key=sort_by_study_id)
#         return study_ids
#     except Exception as ex:
#         raise ex


class SampleTableProcessor:
    def __init__(
        self, sample_table: IsaTable, sample_index, organism_index, organism_part_index
    ) -> None:
        self.sample_index = sample_index
        self.organism_part_index = organism_part_index
        self.organism_index = organism_index
        self.sample_table = sample_table
        self.sample_table_map = None
        if self.sample_index >= 0:
            self.sample_table_map = self._get_sample_table_map(
                sample_table, sample_index
            )

    def get_organism(self, sample_name):
        if not self.sample_table_map or self.organism_index < 0:
            return None
        if sample_name in self.sample_table_map:
            row_index = self.sample_table_map[sample_name]
            column_data = self.sample_table.data[
                self.sample_table.columns[self.organism_index]
            ]

            return column_data[row_index]
        return None

    def get_organism_part(self, sample_name):
        if not self.sample_table_map or self.organism_part_index < 0:
            return None
        if sample_name in self.sample_table_map:
            row_index = self.sample_table_map[sample_name]
            column_data = self.sample_table.data[
                self.sample_table.columns[self.organism_part_index]
            ]

            return column_data[row_index]
        return None

    def _get_sample_table_map(self, sample_table, sample_name_index=0):
        sample_table_map = {}
        sample_column_data = sample_table.data[sample_table.columns[sample_name_index]]

        row_index = 0
        for data in sample_column_data:
            sample_table_map[data] = row_index
            row_index += 1
        return sample_table_map


class MtblsLookupTableCreator:
    def __init__(
        self,
        study_metadata_service_factory: StudyMetadataServiceFactory,
        study_read_repository: None | StudyReadRepository = None,
        selected_studies=None,
        output_folder_path: None | str = "./dist",
        column_model_mapping_file_path: None | str = None,
    ) -> None:
        self.selected_studies = selected_studies
        self.study_metadata_service_factory = study_metadata_service_factory
        self.study_read_repository = study_read_repository
        self.column_model_mapping_file_path = column_model_mapping_file_path
        self.output_folder_path = (
            Path(output_folder_path) if output_folder_path else Path("./dist")
        )
        self.column_type_control_list_file_path = column_model_mapping_file_path

    async def get_public_studies(self) -> list[str]:
        result = await self.study_read_repository.select_fields(
            query_field_options=QueryFieldOptions(
                filters=[EntityFilter(key="status", value=StudyStatus.PUBLIC)],
                selected_fields=["accession_number"],
            )
        )
        resources_map = {x[0]: x for x in result.data}
        resource_ids: list[str] = list(resources_map.keys())
        return resource_ids

    async def create_mtbls_lcms_lookup_tables(self):
        study_map = {}
        study_assay_map = {}
        study_assay_files_map = {}
        column_type_control_list = {}
        mapping_file_path = self.column_type_control_list_file_path
        if mapping_file_path and Path(mapping_file_path).exists():
            file = Path(mapping_file_path).open("r", encoding="utf-8")

        else:
            default_path = resources.files(mtbls.__name__).joinpath(
                "resources/mdp/column_model.tsv"
            )
            logger.warning(
                "Column model mapping file is not found at path: %s. "
                "The default mapping file will be used.",
                default_path,
            )
            file = Path(default_path).open("r", encoding="utf-8")
        try:
            rd = csv.reader(file, delimiter="\t", quotechar='"')
            row_index = 0
            for row in rd:
                if row_index == 0:
                    row_index += 1
                    continue

                row_index += 1
                if row[2] == "1":
                    continue
                key = unidecode.unidecode(row[1].lower()).replace(" ", "")
                min_peak_value = row[4]
                max_peak_value = row[5]
                center_value = row[6]
                column_type_control_list[key] = (
                    min_peak_value,
                    max_peak_value,
                    center_value,
                )
        finally:
            if file:
                file.close()

        study_ids = await self.get_public_studies()
        study_ids.sort(key=sort_by_study_id)
        for study_id in study_ids:
            process = False
            if not self.selected_studies:
                process = True
            else:
                if study_id in self.selected_studies:
                    process = True
            if process:
                with await self.study_metadata_service_factory.create_service(
                    study_id
                ) as service:
                    study_model = await service.load_study_model(
                        load_sample_file=True,
                        load_assay_files=True,
                        load_maf_files=False,
                        load_folder_metadata=True,
                        load_db_metadata=True,
                    )
                    errors = evaulate_mtbls_model(study_model)
                    if errors:
                        logger.error(
                            "Study %s is skipped due to errors in loading the study model. Errors: %s",
                            study_id,
                            errors,
                        )
                        continue
                self._update_lookup_maps(
                    study_map,
                    study_assay_map,
                    study_assay_files_map,
                    study_model,
                    column_type_control_list,
                )
        self.output_folder_path.mkdir(exist_ok=True, parents=True)
        metabolights_studies = self.output_folder_path / "metabolights_studies.loc"

        metabolights_ms_assays = self.output_folder_path / "metabolights_ms_assays.loc"

        metabolights_ms_assay_files = (
            self.output_folder_path / "metabolights_ms_assay_files.loc",
        )

        self._write_study_to_file(
            file_path=str(metabolights_studies), study_map=study_map
        )
        self._write_study_ms_assay_to_file(
            file_path=str(metabolights_ms_assays), study_assay_map=study_assay_map
        )
        self._write_study_ms_assay_files_to_file(
            file_path=str(metabolights_ms_assay_files),
            study_assay_files_map=study_assay_files_map,
        )
        logger.info("Task is ended")

    def _write_study_to_file(
        self,
        file_path: str,
        study_map: dict[str, StudyLookup],
    ):
        with Path(file_path).open("w", encoding="utf-8") as tsvfile:
            values = ["#value", "name", "title", "study_no", "technique", "url"]
            row = "\t".join(values)
            tsvfile.write(f"{row}")

            def sort(key: str):
                value = int(key.replace("MTBLS", "").replace("REQ", ""))
                return value

            models = list(study_map.keys())
            models.sort(key=sort)
            for study_id in models:
                model: StudyLookup = study_map[study_id]
                values = [
                    model.value,
                    model.name,
                    model.title,
                    str(model.study_no),
                    model.technique,
                    model.url,
                ]
                row = "\t".join(values)
                tsvfile.write(f"\n{row}")

    def _write_study_ms_assay_to_file(self, file_path, study_assay_map):
        with Path(file_path).open("w", encoding="utf-8") as tsvfile:
            values = [
                "#value",
                "name",
                "study_id",
                "polarity",
                "organism_part",
                "column_model",
                "min_max_peak_width",
                "center_sample",
                "text",
                "parameters",
                "url",
            ]
            row = "\t".join(values)
            tsvfile.write(f"{row}")
            for v in study_assay_map.values():
                model: StudyMsAssayLookup = v
                values = [
                    model.value,
                    model.name,
                    model.study_id,
                    model.polarity,
                    model.organism_part,
                    model.column_model,
                    model.param_min_max_peak_width,
                    str(model.param_center),
                    model.text,
                    model.parameters_merged,
                    model.url,
                ]
                row = "\t".join(values)
                tsvfile.write(f"\n{row}")

    def _write_study_ms_assay_files_to_file(
        self, file_path, study_assay_files_map: dict[str, StudyMsAssayFileLookup]
    ):
        with Path(file_path).open("w", encoding="utf-8") as tsvfile:
            values = [
                "#value",
                "name",
                "study_id",
                "assay_id",
                "file_format",
                "organism_part",
                "url",
            ]
            row = "\t".join(values)
            tsvfile.write(f"{row}")
            for model in study_assay_files_map.values():
                values = [
                    model.value,
                    model.name,
                    model.study_id,
                    model.assay_id,
                    model.file_format,
                    f"{model.organism}:{model.organism_part}",
                    model.url,
                ]
                row = "\t".join(values)
                tsvfile.write(f"\n{row}")

    def _update_lookup_maps(
        self,
        study_map,
        study_assay_map,
        study_assay_file_map,
        m_study: MetabolightsStudyModel,
        column_type_control_list: dict[str, tuple],
    ) -> None:
        if not m_study or not m_study.assays:
            return
        study_id = m_study.study_db_metadata.study_id
        tecnologies = set()
        ms_tecnologies = set()
        ms_assays = []
        matched = False
        for assay_file in m_study.assays:
            assay = m_study.assays[assay_file]
            tecnologies.add(assay.assay_technique.name)
            if not assay.assay_technique.name == "LC-MS":
                continue
            result = check_assay(study_id, m_study, assay_file)

            if not result or result[3] != "matched":
                continue

            in_control_list = False
            for column_type_item in result[2][1]:
                item = unidecode.unidecode(column_type_item).replace(" ", "")
                if item in column_type_control_list:
                    in_control_list = True
                    break
            if not in_control_list:
                continue

            matched = True

            if assay.assay_technique.name == "LC-MS":
                ms_tecnologies.add(assay.assay_technique.name)
                ms_assays.append(assay)

        if not ms_assays:
            if "LC-MS" in tecnologies:
                logger.info(
                    "%s has LC-MS assys(s). However they are not valid: '%s'",
                    study_id,
                    ", ".join(tecnologies),
                )
            else:
                logger.warning(
                    "%s has no LC-MS assay. Technology: %s",
                    study_id,
                    ", ".join(tecnologies),
                )
            return
        else:
            if len(ms_assays) != len(m_study.assays):
                logger.info(
                    "%s has LC-MS assay(s). Technology: %s",
                    study_id,
                    ", ".join(ms_tecnologies),
                )
            else:
                logger.info("%s technology: %s", study_id, ", ".join(ms_tecnologies))
        if not matched:
            return
        lookup_model: StudyLookup = StudyLookup()
        lookup_model.value = study_id

        study_number = m_study.study_db_metadata.numeric_study_id
        lookup_model.study_no = study_number
        lookup_model.technique = "mass spectrometry"
        base_url = PUBLIC_STUDY_FTP_BASE_URL
        lookup_model.url = f"{base_url.rstrip('/')}/{study_id}"
        study_map[study_id] = lookup_model

        if m_study.investigation.studies[0]:
            lookup_model.title = m_study.investigation.studies[0].title.replace(
                "\n", ""
            )
            lookup_model.name = f"{study_id}: {lookup_model.title}"

            sample_table = m_study.samples[
                m_study.investigation.studies[0].file_name
            ].table
            sample_index_list = self._find_data_index_with_field_name(
                sample_table, "sample name"
            )
            organism_index_list = self._find_data_index_with_field_name(
                sample_table, "characteristics[organism]"
            )
            organism_part_index_list = self._find_data_index_with_field_name(
                sample_table, "characteristics[organism part]"
            )
            sample_table_processor = None
            sample_index = sample_index_list[0] if sample_index_list else -1
            organism_index = organism_index_list[0] if organism_index_list else -1
            organism_part_index = (
                organism_part_index_list[0] if organism_part_index_list else -1
            )

            if (
                not sample_index_list
                or not organism_index_list
                or not organism_part_index_list
            ):
                logger.error(
                    "%s has invalid sample table indices: "
                    "sample_index: %d organism_index: %d organism_part_index: %d",
                    study_id,
                    sample_index,
                    organism_index,
                    organism_part_index,
                )

            sample_table_processor = SampleTableProcessor(
                sample_table, sample_index, organism_index, organism_part_index
            )
            self._process_assays(
                study_assay_map,
                study_assay_file_map,
                m_study,
                ms_assays,
                sample_table_processor,
                column_type_control_list,
            )

    def _process_assays(
        self,
        study_assay_map,
        study_assay_file_map,
        m_study: MetabolightsStudyModel,
        assays: List[IsaTableFile],
        sample_table_processor: SampleTableProcessor,
        column_type_control_list: dict[str, tuple],
    ) -> None:
        study_id = m_study.study_db_metadata.study_id
        index = 0
        for assay in assays:
            index += 1

            derived_file_index_list = self._find_data_index_with_field_name(
                assay.table, "derived spectral data file"
            )
            raw_file_index_list = self._find_data_index_with_field_name(
                assay.table, "raw spectral data file"
            )

            model: StudyMsAssayLookup = StudyMsAssayLookup()
            model.value = f"{study_id}-{index:02}"
            model.name = assay.file_path
            model.study_id = study_id

            # model.assay_name = f"Assay {index:02}"
            # model.assay_sheet_file_name = assay.filePath
            # model.technique = assay.assayTechnique.name
            # model.platform = assay.platform

            base_url = PUBLIC_STUDY_FTP_BASE_URL
            model.url = f"{base_url.rstrip('/')}/{study_id}/{assay.file_path}"

            sample_index_list = self._find_data_index_with_field_name(
                assay.table, "sample name"
            )
            sample_index = sample_index_list[0] if sample_index_list else -1
            column_model_index_list = self._find_data_index_with_field_name(
                assay.table, "parameter value[column model]"
            )
            column_type_index_list = self._find_data_index_with_field_name(
                assay.table, "parameter value[column type]"
            )

            scan_polarity_index_list = self._find_data_index_with_field_name(
                assay.table, "parameter value[scan polarity]"
            )

            scan_polarity_index = (
                scan_polarity_index_list[0] if scan_polarity_index_list else -1
            )
            column_model_index = (
                column_model_index_list[0] if column_model_index_list else -1
            )
            column_type_index = (
                column_type_index_list[0] if column_type_index_list else -1
            )
            organism_parts = set()
            for i in range(assay.table.row_count):
                sample_name = self._get_data_with_index(assay.table, i, sample_index)
                organism_name = (
                    sample_table_processor.get_organism(sample_name=sample_name)
                    if sample_name and sample_index >= 0
                    else ""
                )
                organism_part_name = (
                    sample_table_processor.get_organism_part(sample_name=sample_name)
                    if sample_name and sample_index >= 0
                    else ""
                )
                if organism_name and organism_part_name:
                    organism_parts.add(f"{organism_name}:{organism_part_name}")
                elif organism_name or organism_part_name:
                    organism_parts.add(f"{organism_name}:{organism_part_name}")

                if i == 0:
                    column_model = (
                        self._get_data_with_index(assay.table, i, column_model_index)
                        if column_model_index >= 0
                        else ""
                    )
                    column_type = (
                        self._get_data_with_index(assay.table, i, column_type_index)
                        if column_type_index >= 0
                        else ""
                    )
                    scan_polarity = (
                        self._get_data_with_index(assay.table, i, scan_polarity_index)
                        if scan_polarity_index >= 0
                        else ""
                    )
                    model.column_model = column_model.upper()
                    model.polarity = Polarity.match(scan_polarity).value
                    model.column_type = ColumnType.match(column_type).value
                    model.param_polarity = model.polarity.lower()[:3]
                    key = unidecode.unidecode(model.column_model).replace(" ", "")
                    if key in column_type_control_list:
                        values = column_type_control_list[key]
                        model.param_min_max_peak_width = f"{values[0]};{values[1]}"
                        model.param_center = values[2]
                    else:
                        model.param_min_max_peak_width = "5;20"
                        model.param_center = 2
            if len(organism_parts) > 3:
                new_list = list(organism_parts)[:3]
                new_list.append("...:...")
                model.organism_part = ",".join(new_list)
            else:
                model.organism_part = ",".join(list(organism_parts))

            model.text = f"{model.polarity} | {model.column_type} | {model.column_model} | {model.organism_part}"
            model.parameters_merged = f"{model.param_polarity},{model.param_min_max_peak_width},{model.param_center}"
            study_assay_map[model.value] = model

            self._process_assay_files(
                study_assay_file_map,
                m_study,
                assay,
                model,
                sample_table_processor,
                sample_index,
                column_model_index,
                scan_polarity_index,
                raw_file_index_list,
                derived_file_index_list,
            )

    def _process_assay_files(
        self,
        study_assay_file_map,
        m_study: MetabolightsStudyModel,
        assay: IsaTableFile,
        assay_model: StudyMsAssayLookup,
        sample_table_processor: SampleTableProcessor,
        sample_index,
        column_model_index,
        scan_polarity_index,
        raw_file_index_list,
        derived_file_index_list,
    ) -> None:
        study_id = m_study.study_db_metadata.study_id
        file_name_index_map = {}
        for raw_index in raw_file_index_list:
            file_name_index_map[raw_index] = "raw"
        for raw_index in derived_file_index_list:
            file_name_index_map[raw_index] = "derived"

        file_indices = list(file_name_index_map.keys())
        base_url = PUBLIC_STUDY_FTP_BASE_URL
        file_count = 0
        for i in range(assay.table.row_count):
            sample_name = self._get_data_with_index(assay.table, i, sample_index)
            organism_name = (
                sample_table_processor.get_organism(sample_name=sample_name)
                if sample_name and sample_index >= 0
                else ""
            )
            organism_part_name = (
                sample_table_processor.get_organism_part(sample_name=sample_name)
                if sample_name and sample_index >= 0
                else ""
            )
            column_model = (
                self._get_data_with_index(assay.table, i, column_model_index)
                if column_model_index >= 0
                else ""
            )
            scan_polarity = (
                self._get_data_with_index(assay.table, i, scan_polarity_index)
                if scan_polarity_index >= 0
                else ""
            )

            for file_index in file_indices:
                file_name = (
                    self._get_data_with_index(assay.table, i, file_index)
                    if file_index >= 0
                    else ""
                )
                if file_name:
                    lower = file_name.lower()
                    matched = False
                    for extension in filtered_file_extensions:
                        if lower.endswith(extension):
                            matched = True
                            break
                    if not matched:
                        continue
                    file_count += 1
                    model: StudyMsAssayFileLookup = StudyMsAssayFileLookup()
                    model.assay_id = assay_model.value
                    model.scan_polarity = scan_polarity
                    model.column_type = column_model
                    model.organism = organism_name
                    model.organism_part = organism_part_name
                    model.study_id = study_id
                    new_model = model
                    new_model.value = f"{assay_model.value}-{(i + 1):04}-{str(file_count)}-{file_name_index_map[file_index]}"
                    name, ext = self._get_file_name_and_ext(file_name)
                    new_model.file_format = ext
                    new_model.name = name

                    # file_location = f"{self.output_folder_path}/{study_id}/{file_name}"
                    if file_name in m_study.study_folder_metadata.folders:
                        new_model.is_dir = True
                    # if Path(file_location).exists() and Path(file_location).is_dir():

                    new_model.url = f"{base_url.rstrip('/')}/{study_id}/{file_name}"
                    study_assay_file_map[new_model.value] = new_model
                    new_model.ms_level = ""  # TODO
                    new_model.peak_peaking = ""  # TODO
                    description = []
                    description.append(
                        new_model.scan_polarity if new_model.scan_polarity else "-"
                    )
                    description.append(
                        new_model.column_type if new_model.column_type else "-"
                    )
                    description.append(
                        f"{new_model.organism}:{new_model.organism_part}"
                        if new_model.organism
                        else "-"
                    )

                    # description.append(new_model.file_format if new_model.file_format else "-")
                    # description.append(new_model.ms_level if new_model.ms_level else "-")
                    # description.append(new_model.peak_peaking if new_model.peak_peaking else "-")
                    model.description = " | ".join(description)

    def _get_file_name_and_ext(self, file_name: str):
        name: str = Path(file_name).name
        first_dot_index = name.find(".")
        if first_dot_index > 0:
            ext = name[(first_dot_index + 1) :]
            if ext:
                return name, ext

        return name, "UNKNOWN"

    def _find_data_index_with_field_name(
        self, table: IsaTable, field_name: str
    ) -> List[int]:
        data_index_list = []
        for column in table.headers:
            try:
                if column.column_header.lower() == field_name.lower():
                    data_index_list.append(column.column_index)

                # data_key = field.split("~")
                # if len(data_key) > 1:
                #     data_index = int(data_key[0])
                #     data_field_name = data_key[1]
                #     if data_field_name.lower().startswith(field_name.lower()):
                #         data_index_list.append(data_index)
            except Exception as e:
                logger.error("Parse error for field %s: %s", field_name, str(e))
        return data_index_list

    def _get_data_with_index(
        self, table: IsaTable, row_index: int, column_index: int
    ) -> str:
        column_data = table.data[table.columns[column_index]]
        if len(column_data) > row_index:
            return column_data[row_index]
        return ""
