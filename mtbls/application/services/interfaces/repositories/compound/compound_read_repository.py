import abc
from typing import List, Tuple, Union

from mtbls.domain.entities.compound import Compound


class CompoundReadRepository:
    @abc.abstractmethod
    async def get_compound_by_id(self, id_: str) -> Union[None, Compound]: ...

    @abc.abstractmethod
    async def get_compounds_by_ids(
        self, ids: List[str]
    ) -> Tuple[List[Compound], List[str]]:
        """
        Retrieve multiple compounds by their IDs in a single batch query.

        Args:
            ids: List of compound IDs (e.g., ['MTBLC1', 'MTBLC2', ...])

        Returns:
            Tuple of (found_compounds, missing_ids) where:
            - found_compounds: List of Compound objects that were found
            - missing_ids: List of IDs that were not found in the database
        """
        ...
