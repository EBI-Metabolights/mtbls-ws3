import datetime
import logging
import re
import shutil
import uuid
from pathlib import Path
from typing import Union

from metabolights_utils.common import CamelCaseModel
from metabolights_utils.isatab import Writer
from metabolights_utils.models.isa.common import IsaTableFile
from metabolights_utils.models.metabolights.model import MetabolightsStudyModel
from metabolights_utils.provider.async_provider.study_provider import (
    AsyncMetabolightsStudyProvider,
)

from mtbls.application.services.interfaces.repositories.study.study_read_repository import (  # noqa: E501
    StudyReadRepository,
)
from mtbls.application.services.interfaces.study_metadata_service import (
    StudyMetadataService,
)
from mtbls.application.services.study_metadata_service.db_metadata_collector import (
    DefaultAsyncDbMetadataCollector,
)
from mtbls.application.services.study_metadata_service.models import (
    IsaTableDataUpdates,
)
from mtbls.application.services.study_metadata_service.repository_file_metadata_provider import (  # noqa: E501
    RepositoryStudyMetadataFileProvider,
)
from mtbls.application.services.study_metadata_service.repository_info_collector import (  # noqa: E501
    RepositoryInfoCollector,
)
from mtbls.domain.entities.investigation import (
    InvestigationFileObject,
    InvestigationItem,
)
from mtbls.domain.entities.isa_table import (
    IsaTableData,
    IsaTableFileObject,
    IsaTableRow,
)
from mtbls.domain.entities.study_file import ResourceCategory, StudyFileOutput
from mtbls.domain.exceptions.repository import StudyObjectNotFoundError
from mtbls.domain.shared.data_types import JsonPathOperation
from mtbls.domain.shared.repository.entity_filter import EntityFilter
from mtbls.domain.shared.repository.query_options import QueryOptions
from mtbls.domain.shared.repository.sort_option import SortOption
from mtbls.domain.shared.repository.study_bucket import StudyBucket
from mtbls.infrastructure.repositories.file_object.study_metadata.mongodb.investigation_file_repository import (  # noqa: E501
    MongoDbInvestigationObjectRepository,
)
from mtbls.infrastructure.repositories.file_object.study_metadata.mongodb.isa_table_file_repository import (  # noqa: E501
    MongoDbIsaTableObjectRepository,
)
from mtbls.infrastructure.repositories.file_object.study_metadata.mongodb.isa_table_file_row_repository import (  # noqa: E501
    MongoDbIsaTableRowObjectRepository,
)
from mtbls.infrastructure.repositories.study_file.mongodb.study_file_repository import (
    MongoDbStudyFileRepository,
)

logger = logging.getLogger(__name__)


class MongoDbStudyMetadataService(StudyMetadataService):
    def __init__(
        self,
        resource_id: str,
        study_read_repository: StudyReadRepository,
        study_file_repository: MongoDbStudyFileRepository,
        investigation_object_repository: MongoDbInvestigationObjectRepository,
        isa_table_object_repository: MongoDbIsaTableObjectRepository,
        isa_table_row_object_repository: MongoDbIsaTableRowObjectRepository,
        temp_path: Union[None, str] = None,
        study_bucket: Union[None, StudyBucket] = None,
    ) -> None:
        self.db_metadata_collector = DefaultAsyncDbMetadataCollector(
            study_read_repository=study_read_repository
        )
        self.study_file_repository = study_file_repository
        self.investigation_object_repository = investigation_object_repository
        self.isa_table_object_repository = isa_table_object_repository
        self.isa_table_row_object_repository = isa_table_row_object_repository
        self.investigation_file_collection_name = (
            self.investigation_object_repository.collection_name
        )
        self.isa_table_files_collection_name = (
            self.isa_table_object_repository.collection_name
        )
        self.isa_table_items_collection_name = (
            isa_table_row_object_repository.collection_name
        )
        self.study_bucket = (
            study_bucket if study_bucket else StudyBucket.PRIVATE_METADATA_FILES
        )
        self.isa_table_collection = self.isa_table_object_repository.collection
        self.isa_table_items_collection = (
            self.isa_table_row_object_repository.collection
        )
        self.investigation_file_collection = (
            self.investigation_object_repository.collection
        )
        self.resource_id = resource_id
        self.numeric_resource_id = int(
            resource_id.removeprefix("REQ").removeprefix("MTBLS")
        )
        self.temp_path = temp_path if temp_path else "/tmp/study-metadata-service"
        self.transaction_id = str(uuid.uuid4())
        self.staging_path = (
            Path(self.temp_path) / Path(self.resource_id) / Path(self.transaction_id)
        )

        self.staging_path_str = str(self.staging_path)

    def __enter__(self) -> StudyMetadataService:
        if self.staging_path.exists():
            shutil.rmtree(str(self.staging_path))
        self.staging_path.mkdir(parents=True, exist_ok=True)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        shutil.rmtree(self.staging_path, ignore_errors=True)

    def get_study_metadata_path(
        self, study_id: str, file_relative_path: Union[None, str] = None
    ) -> str:
        if file_relative_path:
            file_path = self.staging_path / Path(file_relative_path)
            return str(file_path)
        return self.staging_path_str

    def exists(
        self, study_id: str, file_relative_path: Union[None, str] = None
    ) -> bool:
        if file_relative_path:
            file_path = self.staging_path / Path(file_relative_path)
            return file_path
        return self.staging_path.exists()

    async def load_study_model(  # noqa: PLR0913
        self,
        load_sample_file: bool = False,
        load_assay_files: bool = False,
        load_maf_files: bool = False,
        load_folder_metadata: bool = False,
        load_db_metadata: bool = False,
        samples_sheet_offset: Union[int, None] = None,
        samples_sheet_limit: Union[int, None] = None,
        assay_sheet_offset: Union[int, None] = None,
        assay_sheet_limit: Union[int, None] = None,
        assignment_sheet_offset: Union[int, None] = None,
        assignment_sheet_limit: Union[int, None] = None,
        calculate_data_folder_size: bool = False,
        calculate_metadata_size: bool = False,
    ) -> MetabolightsStudyModel:
        investigation_object_key = "i_Investigation.txt"
        # investigations = await self.investigation_object_repository.find(
        #     query_options=QueryOptions(
        #         filters=[
        #             EntityFilter(key="resourceId", value=self.resource_id),
        #             EntityFilter(key="objectKey", value=investigation_object_key),
        #         ]
        #     )
        # )
        investigation_result = self.investigation_file_collection.find_one(
            {"resourceId": self.resource_id, "objectKey": investigation_object_key},
            {"_id": 0},
        )
        investigation_path = self.staging_path / Path(investigation_object_key)
        if investigation_result:
            investigation_object = InvestigationFileObject.model_validate(
                investigation_result
            )
            item = investigation_object.data
            investigation = item.to_investigation()
            Writer.get_investigation_file_writer().write(
                investigation,
                str(investigation_path),
                values_in_quotation_mark=False,
                investigation_module_name=None,
            )

            # isa_table_files = await self.isa_table_object_repository.find(
            #     query_options=QueryOptions(
            #         filters=[
            #             EntityFilter(key="resourceId", value=self.resource_id),
            #         ]
            #     )
            # )
            isa_table_files = self.isa_table_collection.find(
                {"resourceId": self.resource_id}
            ).sort({"objectKey": 1})

            for item in isa_table_files:
                isa_table = IsaTableFileObject.model_validate(item)
                isa_table_path = self.staging_path / Path(isa_table.object_key)
                isa_table_data = await self.load_isa_table_file(isa_table.object_key)
                await self.dump_isa_table_data(isa_table_data, str(isa_table_path))

        provider = AsyncMetabolightsStudyProvider(
            folder_metadata_collector=(
                RepositoryInfoCollector(
                    resource_id=self.resource_id,
                    study_file_repository=self.study_file_repository,
                )
                if load_folder_metadata
                else None
            ),
            db_metadata_collector=(
                self.db_metadata_collector if load_db_metadata else None
            ),
            metadata_file_provider=RepositoryStudyMetadataFileProvider(
                resource_id=self.resource_id,
                metadata_files_object_repository=self,
                download_path=self.staging_path,
            ),
        )

        return await provider.load_study(
            study_id=self.resource_id,
            study_path=str(self.staging_path),
            connection=self.db_metadata_collector if load_db_metadata else None,
            load_sample_file=load_sample_file,
            load_assay_files=load_assay_files,
            load_maf_files=load_maf_files,
            load_folder_metadata=load_folder_metadata,
            samples_sheet_offset=samples_sheet_offset,
            samples_sheet_limit=samples_sheet_limit,
            assay_sheet_offset=assay_sheet_offset,
            assay_sheet_limit=assay_sheet_limit,
            assignment_sheet_offset=assignment_sheet_offset,
            assignment_sheet_limit=assignment_sheet_limit,
            calculate_data_folder_size=calculate_data_folder_size,
            calculate_metadata_size=calculate_metadata_size,
        )

    async def process_investigation_file(
        self,
        operation: JsonPathOperation,
        target_jsonpath: str,
        output_model_class: type[CamelCaseModel],
        input_data: Union[None, str, CamelCaseModel] = None,
        field_name: Union[None, str] = None,
        object_key: Union[int, None] = None,
    ) -> tuple[Union[list[str], list[CamelCaseModel]], list[int]]:
        object_key = object_key if object_key else "i_Investigation.txt"

        if operation == "insert":
            jsonpath = self.get_mongodb_update_path(object_key, target_jsonpath)
            input_value = input_data.model_dump(by_alias=True)
            result = self.investigation_file_collection.update_one(
                {"resourceId": self.resource_id, "objectKey": object_key},
                {"$push": {"data." + jsonpath: input_value}},
            )
            return_values, return_indices = self.fetch_nested_item(
                target_jsonpath, output_model_class, object_key
            )
            if result.modified_count > 0:
                return [return_values[-1]], [return_indices[-1]]

            raise ValueError(self.resource_id, jsonpath, "Update failed", input_value)
        elif operation == "delete":
            return_values, return_indices = self.fetch_nested_item(
                target_jsonpath, output_model_class, object_key
            )

            jsonpath = self.get_mongodb_update_path(object_key, target_jsonpath)

            result = self.investigation_file_collection.update_one(
                {"resourceId": self.resource_id, "objectKey": object_key},
                {"$unset": {"data." + jsonpath: 1}},
            )
            parent = "data." + ".".join(jsonpath.split(".")[:-1])
            result = self.investigation_file_collection.update_one(
                {"resourceId": self.resource_id, "objectKey": object_key},
                {"$pull": {parent: None}},
            )
            if result.modified_count > 0:
                return return_values, return_indices
            raise ValueError(
                self.resource_id, jsonpath, "Delete failed", return_indices
            )
        elif operation == "update":
            target_jsonpath += (
                f"{field_name}.{field_name}" if field_name else target_jsonpath
            )
            jsonpath = self.get_mongodb_update_path(object_key, target_jsonpath)
            if issubclass(output_model_class, CamelCaseModel):
                input_value = input_data.model_dump(by_alias=True)
            elif issubclass(output_model_class, str):
                input_value = input_data
            else:
                raise ValueError("Unexpected input data class")
            result = self.investigation_file_collection.update_one(
                {"resourceId": self.resource_id, "objectKey": object_key},
                {"$set": {"data." + jsonpath: input_value}},
            )

            if result.modified_count < 1:
                raise ValueError("Unexpected input data class")

        elif operation == "patch":
            jsonpath = self.get_mongodb_update_path(object_key, target_jsonpath)
            if issubclass(output_model_class, CamelCaseModel):
                input_value = input_data.model_dump(
                    by_alias=True, exclude_defaults=True, exclude_unset=True
                )
                update_dict = {}
                for key, value in input_value.items():
                    update_dict[f"data.{jsonpath}.{key}"] = value

                result = self.investigation_file_collection.update_one(
                    {"resourceId": self.resource_id, "objectKey": object_key},
                    {"$set": update_dict},
                )

            elif issubclass(output_model_class, str):
                input_value = input_data
                result = self.investigation_file_collection.update_one(
                    {"resourceId": self.resource_id, "objectKey": object_key},
                    {"$set": {"data." + jsonpath: input_value}},
                )
            else:
                raise ValueError("Unexpected input data class")
            if result.modified_count < 1:
                raise ValueError("Unexpected input data class")
        else:
            raise ValueError("Unexpected operation")

        return_values, return_indices = self.fetch_nested_item(
            target_jsonpath, output_model_class, object_key
        )
        return return_values, return_indices

    def fetch_nested_item(
        self,
        target_jsonpath: str,
        output_model_class: type[CamelCaseModel],
        object_key: Union[int, None] = None,
    ):
        convertor = (
            self.string_convertor
            if issubclass(output_model_class, str)
            else self.object_convertor
        )
        pipeline = [self.get_pipeline_match(object_key=object_key)]
        aggregation = self.jsonpath_to_mongodb("data." + target_jsonpath)
        pipeline.extend(aggregation)

        result = self.investigation_file_collection.aggregate(pipeline)
        data = [x["data"] for x in result]
        # if target_jsonpath.endswith("]"):
        values = []
        for item in data:
            if isinstance(item, list):
                for value in item:
                    values.append(value)
            else:
                values.append(item)
        return_indices = [x for x in range(len(values))]
        if values and "index" in values[0]:
            values.sort(key=lambda x: x["index"])
            return_indices = [x["index"] for x in values]

        return_values = [convertor(output_model_class, x) for x in values]

        # else:
        #     return_values = [convertor(output_model_class, x) for x in data]
        #     return_indices = []
        return return_values, return_indices

    def object_convertor(
        self, output_model_class: type[CamelCaseModel], data: dict
    ) -> CamelCaseModel:
        return output_model_class.model_validate(data)

    def string_convertor(self, output_model_class: str, data: str) -> str:
        return data

    jsonpath_regex = re.compile(
        r"""
        (?P<root>\$)                       # 0 - Root element $
        |(?P<dot>\.)                       # 1 - Dot operator .
        |(?P<recursive>\.\.)               # 2 - Recursive descent ..
        |(?P<wildcard>\*)                  # 3- Wildcard *
        |(?P<bracket_open>\[)              # 4- Opening bracket [
        |(?P<bracket_close>\])             # 5- Closing bracket ]
        |(?P<filter>@\.[^\)]+)             # 6- Filters e.g., [?(@.price < 10)]
        |(?P<array_index>\d+)              # 7- Array index e.g., [0]
        |(?P<quoted_field>'[^']*'|"[^"]*") # 8- Quoted field e.g., ['field']
        |(?P<identifier>[a-zA-Z_][\w]*)    # 9- Identifier e.g., fieldName
    """,
        re.VERBOSE,
    )
    jsonpath_expression_map = {
        "==": "$eq",
        "!=": "$ne",
        ">": "$gt",
        ">=": "$gte",
        "<": "$lt",
        "<=": "$lte",
    }

    def jsonpath_to_mongodb(self, expression: str) -> list[dict]:
        expression.removeprefix("$.")
        tokens = self.jsonpath_regex.findall(expression)
        pipeline = []
        current_field = None
        for token in tokens:
            token = "".join(token).strip()
            if token == "$":
                continue
            elif token == ".":
                continue
            elif re.match(r"[a-zA-Z_][\w]*", token):  # Identifier (field name)
                if current_field:
                    current_field += f".{token}"
                else:
                    current_field = f"{token}"
            elif re.match(r"'[^']*'|\"[^\"]*\"", token):  # Quoted field name
                field_name = token.strip("'\"")
                if current_field:
                    current_field += f".{field_name}"
                else:
                    current_field = f"{field_name}"
            elif re.match(r"\d+", token):  # Array index
                selected_indices = [int(token)]
                pipeline.append(
                    self.add_index_field(
                        current_field, selected_indices=selected_indices
                    )
                )
                pipeline.append(
                    {"$unwind": "$" + current_field},
                )
            elif token == "*":  # Wildcard (e.g., `[*]`)
                pipeline.append(self.add_index_field(current_field))
            elif token.startswith("@"):  # Filter
                conditions = token.split(" & ")  # conditions
                matches = []
                pipeline.append(self.add_index_field(current_field))
                for condition in conditions:
                    if condition.startswith("@."):
                        expression = condition[2:]
                        expression_parts = [x.strip() for x in expression.split(" ")]

                        if len(expression_parts) == 3:
                            field, operator, value = expression_parts
                            value = value.strip("'\"")
                            if operator in self.jsonpath_expression_map:
                                op = self.jsonpath_expression_map[operator]
                                expr = {op: ["$$param." + field, value]}
                            else:
                                raise ValueError(
                                    f"Unsupported filter operator: {operator}"
                                )
                        else:
                            raise ValueError(
                                f"Unsupported filter expression: '{expression}'"
                            )
                    else:
                        raise ValueError(f"Unsupported filter condition: {condition}")
                    matches.append(expr)

                pipeline.append(
                    {
                        "$project": {
                            current_field: {
                                "$filter": {
                                    "input": "$" + current_field,
                                    "as": "param",
                                    "cond": {"$and": matches},
                                }
                            }
                        }
                    }
                )
            else:
                if token not in ["[", "]"]:
                    raise ValueError(f"Unsupported token: {token}")
        field = current_field
        result = re.match(r"^(.+)\.(\d)$", current_field)
        if result:
            field = result.groups()[0]
        pipeline.append({"$project": {"_id": 0, "data": "$" + field}})
        return pipeline

    def get_mongodb_update_path(
        self, object_key: Union[None, str], target_jsonpath: str
    ):
        object_key = object_key if object_key else "i_Investigation.txt"
        jsonpath = re.sub(r"\[([^\]]+)\]", r".\1", target_jsonpath)
        jsonpath = re.sub(r"\.\*(\.)?", "", jsonpath)
        return jsonpath

    def get_pipeline_match(self, object_key: Union[None, str] = None):
        object_key = object_key if object_key else "i_Investigation.txt"
        return {
            "$match": {
                "$and": [
                    {"resourceId": {"$eq": self.resource_id}},
                    {
                        "objectKey": {"$eq": object_key},
                    },
                ]
            }
        }

    def add_index_field(
        self, current_field: str, selected_indices: Union[None, list[int]] = None
    ) -> dict:
        indices = {
            "$range": [
                0,
                {"$size": "$" + current_field},
            ],
        }
        if selected_indices:
            indices = {
                "$filter": {
                    "input": {
                        "$range": [
                            0,
                            {"$size": "$" + current_field},
                        ],
                    },
                    "as": "index",
                    "cond": {"$in": ["$$index", selected_indices]},
                }
            }
        return {
            "$addFields": {
                current_field: {
                    "$map": {
                        "input": indices,
                        "as": "index",
                        "in": {
                            "$mergeObjects": [
                                {"index": "$$index"},
                                {
                                    "$arrayElemAt": [
                                        "$" + current_field,
                                        "$$index",
                                    ]
                                },
                            ]
                        },
                    }
                }
            }
        }

    async def save_study_model(
        self,
        model: MetabolightsStudyModel,
    ) -> bool:
        investigation_item = InvestigationItem.get_from_investigation(
            model.investigation
        )
        self.save_investigation_file(investigation_item)

    async def load_investigation_file(
        self,
        object_key: Union[str, None] = None,
    ) -> InvestigationItem:
        object_key = object_key if object_key else "i_Investigation.txt"
        result = self.investigation_file_collection.find_one(
            {"resourceId": self.resource_id, "objectKey": object_key}, {"_id": 0}
        )
        if result and "data" in result:
            return InvestigationItem.model_validate(result["data"])
        raise StudyObjectNotFoundError(
            self.resource_id,
            bucket_name=self.investigation_object_repository.study_bucket.value,
            object_key=object_key,
        )

    async def save_investigation_file(
        self,
        investigation: InvestigationItem,
        object_key: Union[str, None] = None,
    ):
        object_key = object_key if object_key else "i_Investigation.txt"
        now = datetime.datetime.now(datetime.UTC)
        current = self.investigation_file_collection.find_one(
            {"resourceId": self.resource_id, "objectKey": object_key},
        )
        if current:
            self.investigation_file_collection.update_one(
                {"resourceId": self.resource_id, "objectKey": object_key},
                [
                    {"$unset": {"data": ""}},
                    {"$set": {"updatedAt": now}},
                    {"$set": {"data": investigation.model_dump(by_alias=True)}},
                ],
            )
        else:
            await self.investigation_object_repository.create(
                InvestigationFileObject(
                    data=investigation,
                    object_key=object_key,
                    parent_object_key="",
                    bucket_name=self.study_bucket.value,
                    resource_id=self.resource_id,
                    numeric_resource_id=self.numeric_resource_id,
                    created_at=now,
                    basename=object_key,
                    category=ResourceCategory.METADATA_RESOURCE,
                    extension=".txt",
                )
            )

    async def restore_metadata_from_snapshot(
        self, snapshot_name: str
    ) -> tuple[str, str]: ...

    def create_audit_folder_name(
        self,
        folder_suffix: Union[None, str] = "BACKUP",
        folder_prefix: Union[None, str] = None,
        timestamp_format: str = "%Y-%m-%d_%H-%M-%S",
    ) -> Union[None, str]: ...

    async def create_metadata_snapshot(
        self,
        prefix: Union[None, str] = None,
        suffix: Union[None, str] = None,
        timestamp_format: str = "%Y-%m-%d_%H-%M-%S",
    ) -> tuple[str, str]: ...

    async def get_isa_table_data_columns(
        self,
        object_key: str,
    ) -> IsaTableFileObject:
        isa_table_json = self.isa_table_collection.find_one(
            {
                "resourceId": self.resource_id,
                "objectKey": object_key,
                "bucketName": "metadata_files",
            }
        )
        return IsaTableFileObject.model_validate(isa_table_json)

    async def list_isa_files(
        self,
    ) -> list[StudyFileOutput]:
        isa_table_json = self.isa_table_collection.find(
            {
                "resourceId": self.resource_id,
                "bucketName": self.study_bucket.value,
            },
            {"_id": 0, "data": 0},
        )
        isa_files = [StudyFileOutput.model_validate(x) for x in isa_table_json]
        investigation_files = self.investigation_file_collection.find(
            {
                "resourceId": self.resource_id,
                "bucketName": self.study_bucket.value,
            },
            {"_id": 0, "data": 0},
        )
        isa_files.extend(
            [StudyFileOutput.model_validate(x) for x in investigation_files]
        )
        return isa_files

    async def update_isa_table_rows(
        self,
        object_key: str,
        updates: IsaTableDataUpdates = None,
    ) -> list[IsaTableRow]:
        row_ids = []
        for row in updates.rows:
            escaped_names = {x: x.replace(".", "\uff0e") for x in row.data}
            updated_values = {f"data.{escaped_names[x]}": row.data[x] for x in row.data}
            set_stage = {"$set": updated_values}
            row_ids.append(row.row_index)
            result = self.isa_table_items_collection.update_one(
                {
                    "resourceId": self.resource_id,
                    "parentObjectKey": object_key,
                    "bucketName": "metadata_files",
                    "rowIndex": row.row_index,
                },
                set_stage,
            )

        updated_values = self.isa_table_items_collection.find(
            {
                "resourceId": self.resource_id,
                "parentObjectKey": object_key,
                "bucketName": "metadata_files",
                "rowIndex": {"$in": row_ids},
            },
            {"_id": 0, "rowIndex": 1, "data": 1},
        )
        result = [IsaTableRow.model_validate(x) for x in updated_values]
        return result

    async def get_isa_table_rows(
        self,
        object_key: str,
        offset: Union[int, None] = None,
        limit: Union[int, None] = None,
    ) -> list[IsaTableRow]:
        filtered_data = {
            "resourceId": self.resource_id,
            "parentObjectKey": object_key,
            "bucketName": "metadata_files",
        }
        query = self.isa_table_items_collection.find(
            filtered_data, {"_id": 0, "rowIndex": 1, "data": 1}
        ).sort({"rowIndex": 1})
        if offset:
            query = query.skip(offset)
        if limit:
            query = query.limit(limit)
        rows = [IsaTableRow.model_validate(x) for x in query]
        rows.sort(key=lambda x: x.row_index)
        return rows

    async def load_isa_table_file(
        self,
        object_key: str,
        offset: Union[int, None] = None,
        limit: Union[int, None] = None,
    ) -> IsaTableData:
        # isa_table_json = self.isa_table_collection.find_one(
        #     {
        #         "resourceId": self.resource_id,
        #         "objectKey": object_key,
        #         "bucketName": "metadata_files",
        #     }
        # )
        # isa_table_data = IsaTableData.model_validate(isa_table_json)
        # filtered_data = {
        #     "resourceId": self.resource_id,
        #     "parentObjectKey": object_key,
        #     "bucketName": "metadata_files",
        # }
        # query = self.isa_table_items_collection.find(
        #     filtered_data, {"_id": 0, "rowIndex": 1, "data": 1}
        # ).sort({"rowIndex": 1})
        # if offset:
        #     query = query.skip(offset)
        # if limit:
        #     query = query.limit(limit)
        # rows = [IsaTableRow.model_validate(x) for x in query]
        # rows.sort(key=lambda x: x.row_index)
        # isa_table_data.rows = rows
        # isa_table_data.limit = limit
        # isa_table_data.offset = offset
        # return isa_table_data
        table_find_result = await self.isa_table_object_repository.find(
            query_options=QueryOptions(
                filters=[
                    EntityFilter(key="resourceId", value=self.resource_id),
                    EntityFilter(key="objectKey", value=object_key),
                ]
            )
        )

        if table_find_result and table_find_result.data:
            isa_table = table_find_result.data[0]
            isa_table_data = IsaTableData(
                columns=isa_table.columns,
                data_type=isa_table.data_type,
                limit=limit,
                offset=offset,
            )
            rows_result = await self.isa_table_row_object_repository.find(
                query_options=QueryOptions(
                    filters=[
                        EntityFilter(key="resourceId", value=self.resource_id),
                        EntityFilter(key="objectKey", value=object_key),
                    ],
                    sort_options=[SortOption(key="rowIndex")],
                    limit=limit,
                    offset=offset,
                )
            )
            if rows_result and rows_result.data:
                isa_table_data.rows = [
                    IsaTableRow.model_validate(x) for x in rows_result.data
                ]
            return isa_table_data

        raise StudyObjectNotFoundError(self.resource_id, object_key)

    async def save_isa_table_file(self, isa_table_file: IsaTableFile, object_key: str):
        self.get_isa_table_data_columns()
