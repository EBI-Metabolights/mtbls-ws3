import logging
from typing import Dict, Tuple

from metabolights_utils.models.isa.common import IsaTableColumn, IsaTableFile
from pydantic import BaseModel

from mtbls.domain.domain_services.modifier.base_modifier import BaseModifier
from mtbls.domain.shared.modifier import UpdateLog

logger = logging.getLogger(__name__)


class ColumnUpdateLog(BaseModel):
    header: str = ""
    index: int = -1
    cell_updates: Dict[str, Dict[str, list[int]]] = {}
    updates: Dict[int, Tuple[str, str]] = {}


class IsaTableColumnUpdateHandler(BaseModifier):
    def __init__(self, isa_table_file: IsaTableFile):
        self.isa_table_file = isa_table_file
        self.column_updates: Dict[str, ColumnUpdateLog] = {}

    def update_isa_table_cell(
        self, column: IsaTableColumn, old_value: str, new_value: str, index: int
    ):
        if column.column_name not in self.column_updates:
            self.column_updates[column.column_name] = ColumnUpdateLog(
                header=column.column_header, index=column.column_index
            )
        column_log: ColumnUpdateLog = self.column_updates[column.column_name]
        if old_value not in column_log.cell_updates:
            column_log.cell_updates[old_value] = {}
        if new_value not in column_log.cell_updates[old_value]:
            column_log.cell_updates[old_value][new_value] = []

        column_log.cell_updates[old_value][new_value].append(index + 1)
        self.isa_table_file.table.data[column.column_name][index] = new_value
        self.column_updates[column.column_name].updates[index] = (old_value, new_value)

    def get_isa_table_update_logs(self, limit: int = 5) -> list[UpdateLog]:
        update_logs: list[UpdateLog] = []
        if not self.column_updates:
            return update_logs
        for _, column_log in self.column_updates.items():
            column_index = column_log.index
            column_header = column_log.header
            file = self.isa_table_file.file_path
            values = [
                (x, "".join(column_log.cell_updates[x].keys()))
                for x in column_log.cell_updates
            ]
            old_values = [f"'{x[0]}'" for x in values]
            old_values_str = self.get_list_string(old_values, limit=limit)

            new_values = [f"'{x[1]}'" for x in values]
            new_values_str = self.get_list_string(new_values, limit=limit)
            update_logs.append(
                UpdateLog(
                    source=file,
                    action=f"Update [column {column_index + 1}] {column_header}",
                    old_value=old_values_str,
                    new_value=new_values_str,
                )
            )
        return update_logs
