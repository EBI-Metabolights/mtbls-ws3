import logging
from typing import Any, Dict, List, Optional, Tuple

from mtbls.application.services.interfaces.repositories.compound.compound_similarity_repository import (
    CompoundSimilarityRepository,
)
from mtbls.domain.entities.similar_compound import SimilarCompound
from mtbls.infrastructure.persistence.db.mongodb.db_client import DocumentDatabaseClient
from mtbls.infrastructure.repositories.compound.similarity_config import (
    CompoundSimilarityConfig,
)

logger = logging.getLogger(__name__)

# Lazy import RDKit to allow graceful handling if not installed
_rdkit_available: Optional[bool] = None


def _check_rdkit() -> bool:
    """Check if RDKit is available."""
    global _rdkit_available
    if _rdkit_available is None:
        try:
            from rdkit import Chem as _Chem  # noqa: F401
            from rdkit.Chem import AllChem as _AllChem  # noqa: F401

            # Verify they're usable
            _ = _Chem, _AllChem
            _rdkit_available = True
        except ImportError:
            _rdkit_available = False
            logger.warning(
                "RDKit not installed. Similarity search by SMILES/InChI will not be available."
            )
    return _rdkit_available


def _compute_fingerprint(
    molfile: Optional[str] = None,
    smiles: Optional[str] = None,
    inchi: Optional[str] = None,
    radius: int = 2,
    nbits: int = 2048,
) -> Tuple[Optional[List[int]], Optional[int]]:
    """
    Compute Morgan fingerprint for a molecule.

    Args:
        molfile: MOL block string
        smiles: SMILES string
        inchi: InChI string
        radius: Morgan fingerprint radius
        nbits: Number of bits in fingerprint

    Returns:
        (fingerprint_bits, bit_count) or (None, None) if unparseable
    """
    if not _check_rdkit():
        raise ImportError("RDKit is required for fingerprint computation")

    from rdkit import Chem
    from rdkit.Chem import AllChem, inchi as rdkit_inchi

    mol = None

    # Try molfile first
    if molfile and mol is None:
        try:
            mol = Chem.MolFromMolBlock(molfile, sanitize=True)
        except Exception:
            mol = None

    # Try SMILES
    if smiles and mol is None:
        try:
            mol = Chem.MolFromSmiles(smiles)
        except Exception:
            mol = None

    # Try InChI
    if inchi and mol is None:
        try:
            mol = rdkit_inchi.MolFromInchi(inchi, sanitize=True)
        except Exception:
            mol = None

    if mol is None:
        return (None, None)

    try:
        bv = AllChem.GetMorganFingerprintAsBitVect(mol, radius=radius, nBits=nbits)
        on_bits = list(bv.GetOnBits())
        return (on_bits, len(on_bits))
    except Exception:
        return (None, None)


def _select_screening_bits(
    fingerprint_bits: List[int], max_bits: int = 20
) -> List[int]:
    """
    Select a subset of bits for the screening stage.

    Strategy: Use bits spread across the fingerprint space to maximize
    selectivity. Pick bits at regular intervals for good coverage.
    """
    if len(fingerprint_bits) <= max_bits:
        return fingerprint_bits

    step = len(fingerprint_bits) / max_bits
    return [fingerprint_bits[int(i * step)] for i in range(max_bits)]


def _build_similarity_pipeline(
    query_bits: List[int],
    query_bit_count: int,
    threshold: float,
    screening_bits: List[int],
    limit: int,
    fingerprint_nbits: int,
    exclude_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Build MongoDB aggregation pipeline for Tanimoto similarity search.

    Two-stage approach:
      Stage A (Screening): Use $in on fingerprint_bits + bit count range pruning
      Stage B (Scoring): Calculate exact Tanimoto using $setIntersection
    """
    # Bit count range for pruning rule:
    # T * query_count <= candidate_count <= query_count / T
    min_bit_count = int(threshold * query_bit_count)
    max_bit_count = (
        int(query_bit_count / threshold) if threshold > 0 else fingerprint_nbits
    )

    pipeline: List[Dict[str, Any]] = []

    # Stage A: Coarse Screening
    match_conditions: Dict[str, Any] = {
        # Must have fingerprint data
        "fingerprint_bits": {"$exists": True},
        # Bit count pruning rule
        "fingerprint_bit_count": {
            "$gte": min_bit_count,
            "$lte": max_bit_count,
        },
    }

    # Exclude the query compound itself if searching by ID
    if exclude_id:
        match_conditions["id"] = {"$ne": exclude_id}

    pipeline.append({"$match": match_conditions})

    # Must share at least one of the screening bits (separate $match for index usage)
    pipeline.append({"$match": {"fingerprint_bits": {"$in": screening_bits}}})

    # Stage B: Exact Tanimoto Scoring
    # Calculate intersection size
    pipeline.append(
        {
            "$addFields": {
                "_intersection": {
                    "$size": {"$setIntersection": ["$fingerprint_bits", query_bits]}
                },
                "_query_bit_count": query_bit_count,
            }
        }
    )

    # Calculate Tanimoto: intersection / (A + B - intersection)
    pipeline.append(
        {
            "$addFields": {
                "tanimoto_score": {
                    "$divide": [
                        "$_intersection",
                        {
                            "$subtract": [
                                {
                                    "$add": [
                                        "$fingerprint_bit_count",
                                        "$_query_bit_count",
                                    ]
                                },
                                "$_intersection",
                            ]
                        },
                    ]
                }
            }
        }
    )

    # Filter by threshold
    pipeline.append({"$match": {"tanimoto_score": {"$gte": threshold}}})

    # Sort by similarity (descending)
    pipeline.append({"$sort": {"tanimoto_score": -1}})

    # Limit results
    pipeline.append({"$limit": limit})

    # Project final fields
    pipeline.append(
        {
            "$project": {
                "_id": 0,
                "id": 1,
                "name": 1,
                "formula": 1,
                "smiles": 1,
                "inchiKey": 1,
                "exactmass": 1,
                "averagemass": 1,
                "tanimoto_score": 1,
            }
        }
    )

    return pipeline


class MongoCompoundSimilarityRepository(CompoundSimilarityRepository):
    """MongoDB implementation of compound similarity search using Tanimoto scoring."""

    def __init__(
        self,
        database_client: DocumentDatabaseClient,
        config: Optional[CompoundSimilarityConfig] = None,
    ) -> None:
        self.database_client = database_client
        self.config = config or CompoundSimilarityConfig()

    async def find_similar_by_id(
        self,
        compound_id: str,
        threshold: Optional[float] = None,
        limit: Optional[int] = None,
    ) -> List[SimilarCompound]:
        """Find compounds similar to an existing compound by its ID."""
        threshold = threshold if threshold is not None else self.config.threshold
        limit = limit if limit is not None else self.config.limit

        async with self.database_client.database() as database:
            collection = database[self.config.collection_name]

            # Fetch the reference compound's structure/smiles
            ref = collection.find_one(
                {"id": compound_id},
                {
                    "structure": 1,
                    "smiles": 1,
                    "fingerprint_bits": 1,
                    "fingerprint_bit_count": 1,
                },
            )

            if not ref:
                raise ValueError(f"Compound '{compound_id}' not found")

            # Check if compound has pre-computed fingerprint
            if ref.get("fingerprint_bits") and ref.get("fingerprint_bit_count"):
                query_bits = ref["fingerprint_bits"]
                query_bit_count = ref["fingerprint_bit_count"]
            else:
                # Compute fingerprint from structure/smiles
                molfile = ref.get("structure")
                smiles = ref.get("smiles")
                query_bits, query_bit_count = _compute_fingerprint(
                    molfile=molfile,
                    smiles=smiles,
                    radius=self.config.fingerprint_radius,
                    nbits=self.config.fingerprint_nbits,
                )

            if query_bits is None:
                raise ValueError(
                    f"Compound '{compound_id}' has no parseable structure or fingerprint data"
                )

            return await self._execute_similarity_search(
                collection=collection,
                query_bits=query_bits,
                query_bit_count=query_bit_count,
                threshold=threshold,
                limit=limit,
                exclude_id=compound_id,
            )

    async def find_similar_by_smiles(
        self,
        smiles: str,
        threshold: Optional[float] = None,
        limit: Optional[int] = None,
    ) -> List[SimilarCompound]:
        """Find compounds similar to a molecule specified by SMILES."""
        threshold = threshold if threshold is not None else self.config.threshold
        limit = limit if limit is not None else self.config.limit

        query_bits, query_bit_count = _compute_fingerprint(
            smiles=smiles,
            radius=self.config.fingerprint_radius,
            nbits=self.config.fingerprint_nbits,
        )

        if query_bits is None:
            raise ValueError(f"Could not parse SMILES: {smiles}")

        async with self.database_client.database() as database:
            collection = database[self.config.collection_name]
            return await self._execute_similarity_search(
                collection=collection,
                query_bits=query_bits,
                query_bit_count=query_bit_count,
                threshold=threshold,
                limit=limit,
            )

    async def find_similar_by_inchi(
        self,
        inchi: str,
        threshold: Optional[float] = None,
        limit: Optional[int] = None,
    ) -> List[SimilarCompound]:
        """Find compounds similar to a molecule specified by InChI."""
        threshold = threshold if threshold is not None else self.config.threshold
        limit = limit if limit is not None else self.config.limit

        query_bits, query_bit_count = _compute_fingerprint(
            inchi=inchi,
            radius=self.config.fingerprint_radius,
            nbits=self.config.fingerprint_nbits,
        )

        if query_bits is None:
            raise ValueError(f"Could not parse InChI: {inchi}")

        async with self.database_client.database() as database:
            collection = database[self.config.collection_name]
            return await self._execute_similarity_search(
                collection=collection,
                query_bits=query_bits,
                query_bit_count=query_bit_count,
                threshold=threshold,
                limit=limit,
            )

    async def _execute_similarity_search(
        self,
        collection,
        query_bits: List[int],
        query_bit_count: int,
        threshold: float,
        limit: int,
        exclude_id: Optional[str] = None,
    ) -> List[SimilarCompound]:
        """Execute the similarity search aggregation pipeline."""
        screening_bits = _select_screening_bits(
            query_bits, self.config.max_screening_bits
        )

        pipeline = _build_similarity_pipeline(
            query_bits=query_bits,
            query_bit_count=query_bit_count,
            threshold=threshold,
            screening_bits=screening_bits,
            limit=limit,
            fingerprint_nbits=self.config.fingerprint_nbits,
            exclude_id=exclude_id,
        )

        logger.debug(f"Executing similarity pipeline with {len(pipeline)} stages")

        results = list(collection.aggregate(pipeline))

        logger.debug(
            f"Found {len(results)} similar compounds with Tanimoto >= {threshold}"
        )

        return [
            SimilarCompound(
                id=r.get("id", ""),
                name=r.get("name", ""),
                tanimoto_score=r.get("tanimoto_score", 0.0),
                formula=r.get("formula"),
                smiles=r.get("smiles"),
                inchiKey=r.get("inchiKey"),
                exactmass=r.get("exactmass"),
                averagemass=r.get("averagemass"),
            )
            for r in results
        ]
