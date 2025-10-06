import abc
from typing import Generic

from mtbls.application.services.interfaces.repositories.default.abstract_read_repository import (  # noqa: E501
    AbstractReadRepository,
)
from mtbls.domain.shared.data_types import ID_TYPE, INPUT_TYPE, OUTPUT_TYPE


class AbstractWriteRepository(
    Generic[INPUT_TYPE, OUTPUT_TYPE, ID_TYPE],
    AbstractReadRepository[OUTPUT_TYPE, ID_TYPE],
    abc.ABC,
):
    @abc.abstractmethod
    async def create(self, entity: INPUT_TYPE) -> OUTPUT_TYPE: ...

    @abc.abstractmethod
    async def update(self, entity: OUTPUT_TYPE) -> OUTPUT_TYPE: ...

    @abc.abstractmethod
    async def delete(self, id_: ID_TYPE) -> bool: ...
