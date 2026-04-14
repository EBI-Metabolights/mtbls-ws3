import io
import json
import logging
from typing import Any

from metabolights_utils.isatab import (
    IsaTableFileReader,
    Reader,
)
from metabolights_utils.isatab.reader import (
    IsaTableFileReaderResult,
)

from mtbls.application.services.interfaces.repositories.file_object.file_object_write_repository import (  # noqa: E501
    FileObjectReadRepository,
)

logger = logging.getLogger(__name__)


class IndexedDataFileReport:
    def __init__(
        self,
        internal_files_object_repository: FileObjectReadRepository,
        metadata_files_object_repository: FileObjectReadRepository,
        data_file_index_file_key: None | str = None,
    ):
        self.data_file_index_file_key = data_file_index_file_key
        if not self.data_file_index_file_key:
            self.data_file_index_file_key = "DATA_FILES/data_file_index.json"
        self.internal_files_object_repository = internal_files_object_repository
        self.metadata_files_object_repository = metadata_files_object_repository

    async def create_report(self, resource_id: str) -> str:
        metadata_file_prefixes = ["a_", "i_", "s_", "m_"]
        if not await self.internal_files_object_repository.exists(
            resource_id, object_key=self.data_file_index_file_key
        ):
            return ""

        content = await self.internal_files_object_repository.get_content(
            resource_id,
            object_key=self.data_file_index_file_key,
        )
        data: dict[str, Any] = json.loads(content)
        private_data_files = []
        stop_folders = set()
        for k, v in data.get("private_data_files", {}).items():
            is_dir = v.get("is_dir", False)
            is_stop_folder = v.get("is_stop_folder", False)
            parent_path = v.get("parent_relative_path", "")
            if is_stop_folder:
                stop_folders.add(k)
            if (not is_dir or is_stop_folder) and parent_path not in stop_folders:
                prefix = k[:2] if len(k) > 1 else k
                if prefix not in metadata_file_prefixes:
                    private_data_files.append(k)

        private_data_files.sort()
        assay_file_names = [
            x
            for x in await self.metadata_files_object_repository.list(resource_id)
            if len(x.basename) and x.basename[:2] == "a_"
        ]
        assay_reader: IsaTableFileReader = Reader.get_assay_file_reader(
            results_per_page=100000
        )

        assay_files: dict[str, list[tuple[str, str]]] = {}
        for assay_file in assay_file_names:
            content = await self.metadata_files_object_repository.get_content(
                resource_id,
                object_key=assay_file.object_key,
            )
            bytes_io = io.BytesIO(content)
            result: IsaTableFileReaderResult = assay_reader.read(
                bytes_io, offset=0, limit=None
            )
            for header in result.isa_table_file.table.headers:
                column = header.column_name
                values = result.isa_table_file.table.data.get(column)
                if not values or " Data File" not in column:
                    continue
                unique_values = [x for x in set(values) if x]
                for file in unique_values:
                    if file not in assay_files:
                        assay_files[file] = []
                    if (assay_file.object_key, header) not in assay_files[file]:
                        assay_files[file].append((assay_file.object_key, header))
        report: list[list[str]] = []

        for file in private_data_files:
            notes = ""
            references = assay_files.get(file)
            if references:
                notes = "Referenced in " + ",".join(
                    [f"{x[0]}:{x[1]}" for x in references]
                )
            else:
                notes = "Not referenced in any assay file"
            report.append([file, notes])
        if report:
            return "FILE_PATH\tNOTES\n" + "\n".join("\t".join(x) for x in report)
        else:
            logger.error(
                "%s %s does not exist on %s bucket.",
                resource_id,
                self.data_file_index_file_key,
                self.internal_files_object_repository.get_bucket().name,
            )
        return ""
