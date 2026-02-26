import abc
from typing import List, Optional

from mtbls.domain.entities.similar_compound import SimilarCompound


class CompoundSimilarityRepository(abc.ABC):
    """Interface for compound similarity search operations."""

    @abc.abstractmethod
    async def find_similar_by_id(
        self,
        compound_id: str,
        threshold: Optional[float] = None,
        limit: Optional[int] = None,
    ) -> List[SimilarCompound]:
        """
        Find compounds similar to an existing compound by its ID.

        Args:
            compound_id: ID of the reference compound (e.g., "MTBLC100")
            threshold: Minimum Tanimoto similarity (uses config default if None)
            limit: Maximum number of results (uses config default if None)

        Returns:
            List of similar compounds sorted by similarity score (descending)

        Raises:
            ValueError: If the compound is not found or has no fingerprint data
        """
        ...

    @abc.abstractmethod
    async def find_similar_by_smiles(
        self,
        smiles: str,
        threshold: Optional[float] = None,
        limit: Optional[int] = None,
    ) -> List[SimilarCompound]:
        """
        Find compounds similar to a molecule specified by SMILES.

        Args:
            smiles: SMILES string of the query molecule
            threshold: Minimum Tanimoto similarity (uses config default if None)
            limit: Maximum number of results (uses config default if None)

        Returns:
            List of similar compounds sorted by similarity score (descending)

        Raises:
            ValueError: If the SMILES cannot be parsed
        """
        ...

    @abc.abstractmethod
    async def find_similar_by_inchi(
        self,
        inchi: str,
        threshold: Optional[float] = None,
        limit: Optional[int] = None,
    ) -> List[SimilarCompound]:
        """
        Find compounds similar to a molecule specified by InChI.

        Args:
            inchi: InChI string of the query molecule
            threshold: Minimum Tanimoto similarity (uses config default if None)
            limit: Maximum number of results (uses config default if None)

        Returns:
            List of similar compounds sorted by similarity score (descending)

        Raises:
            ValueError: If the InChI cannot be parsed
        """
        ...
