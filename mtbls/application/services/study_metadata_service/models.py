from pydantic import BaseModel

from mtbls.domain.entities.isa_table import IsaTableRow


class IsaTableDataUpdates(BaseModel):
    resource_id: str = ""
    object_key: str = ""
    rows: list[IsaTableRow] = []
