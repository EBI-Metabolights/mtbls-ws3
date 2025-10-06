import logging
from typing import Annotated, Literal, Union

from metabolights_utils.common import CamelCaseModel
from metabolights_utils.models.isa.enums import ColumnsStructure
from pydantic import (
    Field,
)

from mtbls.domain.entities.base_entity import BaseEntity

logger = logging.getLogger(__name__)


class ColumnDefinition(CamelCaseModel):
    column_index: Annotated[
        Union[int, None],
        Field(description="Index of column in ISA Table. First column index is 0."),
    ] = None

    column_name: Annotated[
        Union[str, None],
        Field(
            description="Index as string of a column in ISA Table. First column index is '0'."  # noqa: E501
        ),
    ] = None

    column_header: Annotated[str, Field(description="Header of ISA table column.")] = ""


class ColumnInfo(CamelCaseModel):
    column_index: Annotated[
        Union[int, None],
        Field(description="Index of column in ISA Table. First column index is 0."),
    ] = None

    column_name: Annotated[
        Union[str, None],
        Field(
            description="Index as string of a column in ISA Table. First column index is '0'."  # noqa: E501
        ),
    ] = None

    column_header: Annotated[str, Field(description="Header of ISA table column.")] = ""

    unique_column_name: Annotated[
        str,
        Field(
            description="Unique header name of column. It is same if column header is unique in ISA table."  # noqa: E501
        ),
    ] = ""
    additional_columns: Annotated[
        list[str],
        Field(
            description="Linked column names. If column is ontology or a column with unit ontology column, it lists the following column headers."  # noqa: E501
        ),
    ] = []

    column_category: Annotated[
        str,
        Field(
            description="Column category. e.g., Parameter Value, Factor Value, protocol"
        ),
    ] = ""

    column_structure: Annotated[
        ColumnsStructure, Field(description="Structure of column in ISA table.")
    ] = ColumnsStructure.SINGLE_COLUMN

    column_prefix: Annotated[
        str, Field(description="Column prefix if header has a value between [].")
    ] = ""

    default_value: Annotated[
        str,
        Field(description="Default value", exclude=True),
    ] = ""


class IndexedData(CamelCaseModel):
    id_: Annotated[
        str,
        Field(description="Investigation identifier.", alias="_id"),
    ] = ""
    bucket_name: Union[None, str] = None
    resource_id: Union[None, str] = None
    numeric_resource_id: Union[None, int] = None
    object_key: Union[None, str] = None


class IsaTableRow(CamelCaseModel):
    row_index: Union[None, int] = None
    data: Annotated[
        dict[str, str], Field(description="Row data with column names")
    ] = {}


class IsaTableFileObject(BaseEntity, IndexedData):
    data_type: Literal["sample", "assay", "maf"]
    columns: Annotated[
        list[ColumnInfo],
        Field(description="Column information. e.g. column order, name"),
    ] = []


class IsaTableRowObject(BaseEntity, IndexedData, IsaTableRow):
    data_type: Literal["sample", "assay", "maf"]
    parent_object_key: Union[None, str] = None


class IsaTableData(CamelCaseModel):
    data_type: Literal["sample", "assay", "maf"]
    columns: Annotated[
        Union[list[ColumnDefinition], list[ColumnInfo]],
        Field(description="Column information. e.g. column order, name"),
    ] = []
    rows: list[IsaTableRowObject] = []
    offset: int = 0
    limit: Union[None, int] = None


class StudyAssayData(IsaTableData): ...
