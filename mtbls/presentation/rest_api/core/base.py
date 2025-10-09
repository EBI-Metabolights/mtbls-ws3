from typing import TypeVar

from metabolights_utils.common import CamelCaseModel
from pydantic import ConfigDict
from pydantic.alias_generators import to_camel, to_pascal


class APIBaseModel(CamelCaseModel):
    """Base model class to convert python attributes to camel case"""

    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
        json_schema_serialization_defaults_required=True,
        field_title_generator=lambda field_name, field_info: to_pascal(
            field_name.replace("_", " ").strip()
        ),
    )


T = TypeVar("T", bound=CamelCaseModel)

L = TypeVar("L")
