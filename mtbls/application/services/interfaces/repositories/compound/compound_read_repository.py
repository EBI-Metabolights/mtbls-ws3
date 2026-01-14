

import abc
from typing import Union

from mtbls.domain.entities.compound import Compound


class CompoundReadRepository:

    @abc.abstractmethod
    async def get_compound_by_id(
        self, id_: str
    ) -> Union[None, Compound]: ...
