from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel, to_pascal


class StudyBaseModel(BaseModel):
    """Base model class to convert python attributes to camel case"""

    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
        json_schema_serialization_defaults_required=True,
        field_title_generator=lambda field, field_info: to_pascal(
            field.replace("_", " ").strip()
        ),
    )


class OntologyTerm(StudyBaseModel):
    term: Annotated[str, Field(description="Ontology term")]

    term_accession_number: Annotated[
        str,
        Field(
            description="The accession number from "
            "the Term Source associated with the term.",
        ),
    ]
    term_source_ref: Annotated[
        str,
        Field(
            description="Source reference name of ontology term. "
            "e.g., EFO, OBO, NCBITAXON."
        ),
    ]
