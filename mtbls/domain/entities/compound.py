from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict

from mtbls.domain.entities.base_entity import BaseCompound
from mtbls.domain.enums.entity import Entity


class CompoundOutput(BaseCompound):
    model_config = ConfigDict(from_attributes=True, strict=True, entity_type=Entity.Compound)


class CompoundFlags(BaseModel):
    hasLiterature: Optional[bool] = None
    hasReactions: Optional[bool] = None
    hasSpecies: Optional[bool] = None
    hasPathways: Optional[bool] = None
    hasNMR: Optional[bool] = None
    hasMS: Optional[bool] = None


class MAFEntry(BaseModel):
    model_config = ConfigDict(extra="ignore")
    index: Optional[int] = None
    database_identifier: Optional[str] = None
    metabolite_identification: Optional[str] = None


class CompoundCounts(BaseModel):
    synonyms: Optional[int] = None
    iupac: Optional[int] = None
    citations: Optional[int] = None
    reactions: Optional[int] = None
    species_hits: Optional[int] = None
    species_total_assays: Optional[int] = None
    kegg: Optional[int] = None
    reactome: Optional[int] = None
    wikipathways: Optional[int] = None
    spectra: Optional[int] = None


class PathwaysKeggItem(BaseModel):
    id: Optional[str] = None
    name: Optional[str] = None
    ko: Optional[str] = None


class CompoundPathways(BaseModel):
    kegg: Optional[List[PathwaysKeggItem]] = None
    reactome: Optional[List[str]] = None
    wikipathways: Optional[List[str]] = None


class Citation(BaseModel):
    source: Optional[str] = None
    type: Optional[str] = None
    value: Optional[str] = None
    title: Optional[str] = None
    doi: Optional[str] = None
    abstract: Optional[str] = None
    author: Optional[str] = None


class Reaction(BaseModel):
    name: Optional[str] = None
    id: Optional[str] = None
    biopax2: Optional[str] = None
    cmlreact: Optional[str] = None


class SpeciesHit(BaseModel):
    species: Optional[str] = None
    species_accession: Optional[str] = None
    maf_entry: Optional[MAFEntry] = None
    assay_index: Optional[int] = None


# ----- RAW submodels -----


class RawPathways(BaseModel):
    WikiPathways: Optional[Dict[str, Any]] = None
    KEGGPathways: Optional[List[Dict[str, Any]]] = None
    ReactomePathways: Optional[Dict[str, Any]] = None


class RawSpectra(BaseModel):
    NMR: Optional[List[Dict[str, Any]]] = None
    MS: Optional[List[Dict[str, Any]]] = None


class RawCompound(BaseModel):
    flags: Optional[Dict[str, Any]] = None  # accept bools/strings from Mongo
    id: Optional[str] = None
    name: Optional[str] = None
    definition: Optional[str] = None
    iupacNames: Optional[List[str]] = None
    smiles: Optional[str] = None
    inchi: Optional[str] = None
    inchiKey: Optional[str] = None
    charge: Optional[int] = None
    averagemass: Optional[float] = None
    exactmass: Optional[float] = None
    formula: Optional[str] = None

    species: Optional[Dict[str, Any]] = None

    synonyms: Optional[List[str]] = None

    pathways: Optional[RawPathways] = None
    spectra: Optional[RawSpectra] = None

    citations: Optional[List[Citation]] = None
    reactions: Optional[List[Reaction]] = None

    structure: Optional[str] = None
    spectrum_ids: Optional[List[str]] = None
    spectra_count: Optional[int] = None


class MAFEntry(BaseModel):
    model_config = ConfigDict(extra="ignore")
    index: Optional[int] = None
    database_identifier: Optional[str] = None
    metabolite_identification: Optional[str] = None


class Compound(BaseCompound):
    model_config = ConfigDict(
        extra="ignore",  # ignore unexpected fields (e.g. _id, future keys)
        arbitrary_types_allowed=True,
        entity_type=Entity.Compound,
    )
    # required
    id: str
    name: str
    inchiKey: str

    # optional simple fields
    definition: Optional[str] = None
    iupacNames: Optional[List[str]] = None
    synonyms: Optional[List[str]] = None
    smiles: Optional[str] = None
    inchi: Optional[str] = None
    formula: Optional[str] = None
    charge: Optional[int] = None
    organisms: Optional[List[str]] = None
    averagemass: Optional[float] = None
    exactmass: Optional[float] = None

    # nested objects
    flags: Optional[CompoundFlags] = None
    counts: Optional[CompoundCounts] = None
    pathways: Optional[CompoundPathways] = None

    citations: Optional[List[Citation]] = None
    reactions: Optional[List[Reaction]] = None
    species_hits: Optional[List[SpeciesHit]] = None
    spectra: Optional[RawSpectra] = None

    spectrum_ids: Optional[List[str]] = None
    spectra_count: Optional[int] = None
    structure_molfile: Optional[str] = None

    raw: Optional[RawCompound] = None

    @classmethod
    def from_mongo(cls, doc: Dict[str, Any]) -> "Compound":
        """
        Build a Compound from a MongoDB document.

        - Drops the Mongo _id field.
        - Relies on Pydantic to validate/coerce types according to this model.
        """
        return cls.from_mongo_with_raw(doc, include_raw=False)

    @classmethod
    def from_mongo_with_raw(cls, doc: Dict[str, Any], include_raw: bool = True) -> "Compound":
        """
        Build a Compound from a MongoDB document, with optional
        inclusion of the raw payload.
        """
        data = dict(doc)  # shallow copy
        data.pop("_id", None)

        # Map structure -> structure_molfile for clarity
        structure = data.pop("structure", None)
        if structure:
            data["structure_molfile"] = structure

        # Flatten species map into species_hits list
        species_hits: List[SpeciesHit] = []
        species_map: Dict[str, Any] = data.pop("species", {}) or {}
        for species_name, entries in species_map.items():
            for entry in entries or []:
                species_hits.append(
                    SpeciesHit(
                        species=entry.get("Species") or species_name or None,
                        species_accession=entry.get("SpeciesAccession"),
                        maf_entry=entry.get("MAFEntry"),
                        assay_index=entry.get("Assay"),
                    )
                )
        if species_hits:
            data["species_hits"] = species_hits

        # Spectra passthrough (if present)
        if "spectra" in doc:
            data["spectra"] = doc.get("spectra")

        if include_raw:
            data["raw"] = RawCompound.model_validate(doc)

        return cls.model_validate(data)
