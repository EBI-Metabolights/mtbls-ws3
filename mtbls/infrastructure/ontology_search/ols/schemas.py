from pydantic import BaseModel, field_validator

from mtbls.domain.entities.ontology.ontology_search import OntologyTermHit


class OlsSearchResultItem(BaseModel):
    label: str
    iri: str
    ontology_prefix: str = ""
    obo_id: str
    description: str = ""
    synonym: list[str] = []

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
            term=self.label or "",
            term_accession_number=self.iri or "",
            term_source_ref=self.ontology_prefix or "",
            description=self.description or "",
            curie=self.obo_id or "",
            synonym=self.synonym or [],
            origin=origin or "",
            origin_url=origin_url or "",
        )
