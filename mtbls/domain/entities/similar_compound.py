from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field


class SimilarCompound(BaseModel):
    """A compound returned from a similarity search with its Tanimoto score."""

    model_config = ConfigDict(extra="ignore")

    id: str = Field(description="Compound identifier (e.g., MTBLC100)")
    name: str = Field(description="Compound name")
    tanimoto_score: float = Field(ge=0.0, le=1.0, description="Tanimoto similarity score (0.0 to 1.0)")

    # Additional fields matching search result style
    formula: Optional[str] = Field(default=None, description="Molecular formula")
    smiles: Optional[str] = Field(default=None, description="SMILES notation")
    inchiKey: Optional[str] = Field(default=None, description="InChI key")
    exactmass: Optional[float] = Field(default=None, description="Exact mass")
    averagemass: Optional[float] = Field(default=None, description="Average mass")


class SimilarCompoundsResult(BaseModel):
    """Result container for similar compounds search."""

    model_config = ConfigDict(extra="ignore")

    query_compound_id: str = Field(description="ID of the compound used as query")
    similar_compounds: List[SimilarCompound] = Field(default_factory=list, description="List of similar compounds")
    threshold: float = Field(description="Minimum Tanimoto threshold used")
    total_found: int = Field(description="Total number of similar compounds found")
