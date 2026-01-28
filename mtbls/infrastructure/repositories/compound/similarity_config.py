from pydantic import BaseModel, Field


class CompoundSimilarityConfig(BaseModel):
    """Configuration for compound similarity search."""

    # Tanimoto similarity threshold (0.0 to 1.0)
    threshold: float = Field(
        default=0.5,
        ge=0.0,
        le=1.0,
        description="Minimum Tanimoto similarity score for results",
    )

    # Maximum number of similar compounds to return
    limit: int = Field(
        default=10,
        gt=0,
        le=100,
        description="Maximum number of similar compounds to return",
    )

    # Number of fingerprint bits to use for screening stage
    max_screening_bits: int = Field(
        default=20,
        gt=0,
        le=100,
        description="Maximum bits to use in Stage A screening for performance",
    )

    # Fingerprint parameters (must match indexed data)
    fingerprint_radius: int = Field(
        default=2,
        description="Morgan fingerprint radius (2 = ECFP4)",
    )

    fingerprint_nbits: int = Field(
        default=2048,
        description="Number of bits in Morgan fingerprint",
    )

    # MongoDB collection name
    collection_name: str = Field(
        default="compounds",
        description="MongoDB collection containing compound documents",
    )
