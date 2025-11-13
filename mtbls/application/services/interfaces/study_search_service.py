import abc

from mtbls.domain.entities.search.study.index_search import BaseSearchInput, BaseSearchResult



class StudySearchService(abc.ABC):
    @abc.abstractmethod
    async def search(
        self, 
        query: BaseSearchInput 
        ) -> BaseSearchResult: ... 