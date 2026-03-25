from pydantic import BaseModel

from mtbls.domain.entities.isa_table import IsaTableRow


class IsaTableDataUpdates(BaseModel):
    rows: list[IsaTableRow] = []


class IsaTableDataRowDelete(BaseModel):
    deleted_row_ids: list[str] = []
