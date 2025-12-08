from typing import Annotated, Any, Self

from metabolights_utils.common import CamelCaseModel
from pydantic import Field, field_validator, model_validator

from mtbls.domain.entities.ontology.ontology_search import OntologyTermHit


class BioportalSearchResultItem(CamelCaseModel):
    pref_label: str
    id_: Annotated[str, Field(alias="@id")]
    ontology: str = ""
    notation: str = ""
    description: str = ""
    synonym: list[str] = []

    @model_validator(mode="wrap")
    @classmethod
    def validate_model(cls, v: Any, handler) -> Self:
        if isinstance(v, dict):
            ontology = v.get("links", {}).get("ontology", "")
            if "/" in ontology:
                v["ontology"] = ontology.split("/")[-1]
            else:
                v["ontology"] = "<UNDEFINED>"
        return handler(v)

    @field_validator("description", mode="before")
    def description_validator(val):
        if isinstance(val, list):
            return ". ".join(val)
        return str(val) if val else ""

    @field_validator("synonym", mode="before")
    def synonym_validator(val):
        if isinstance(val, list):
            return val
        return [val]

    def convert_to_ontology_term_hit(self, origin: str = "", origin_url: str = ""):
        return OntologyTermHit(
            term=self.pref_label or "",
            term_accession_number=self.id_ or "",
            term_source_ref=self.ontology or "",
            description=self.description or "",
            curie=self.notation or "",
            synonym=self.synonym or [],
            origin=origin or "",
            origin_url=origin_url or "",
        )
