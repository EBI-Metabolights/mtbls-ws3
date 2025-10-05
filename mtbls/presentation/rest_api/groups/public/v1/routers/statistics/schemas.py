import enum
from typing import Annotated, List

from pydantic import BaseModel, Field


class StatisticCategory(enum.StrEnum):
    SUBMITTERS = "Submitters"
    DATA = "Data"
    TOP_SUBMITTERS = "Topsubmitters"
    INFO = "Info"


class MetricData(BaseModel):
    key: Annotated[str, Field(description="Name of metric")]
    value: Annotated[str, Field(description="Value of metric")]


class StatisticData(BaseModel):
    title: Annotated[str, Field(default="", description="Title of statistics")]
    key_values: Annotated[
        List[MetricData],
        Field(alias="keyvalues", default=[], description="Metric and their values"),
    ]
