import enum
from typing import Annotated, List

from pydantic import BaseModel, Field

SECTION_DB_VALUE_MAP = {
    "info": "Info",
    "submitters": "Submitters",
    "data": "Data",
    "top-submitters": "Topsubmitters",
}


class StatisticCategory(enum.StrEnum):
    DATA = "data"
    SUBMITTERS = "submitters"
    TOP_SUBMITTERS = "top-submitters"
    INFO = "info"

    def get_db_value(self):
        return SECTION_DB_VALUE_MAP.get(self.value)


class MetricData(BaseModel):
    key: Annotated[str, Field(description="Name of metric")]
    value: Annotated[str, Field(description="Value of metric")]


class StatisticData(BaseModel):
    title: Annotated[str, Field(default="", description="Title of statistics")]
    key_values: Annotated[
        List[MetricData],
        Field(alias="keyvalues", default=[], description="Metric and their values"),
    ]
