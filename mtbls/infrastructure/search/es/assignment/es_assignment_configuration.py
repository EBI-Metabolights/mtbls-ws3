from mtbls.infrastructure.search.es.es_configuration import ElasticsearchConfiguration


class AssignmentElasticSearchConfiguration(ElasticsearchConfiguration):
    api_key_name: str = "assignment"
    index_name: str = "assignment"
    max_study_ids: int = 10000  # Max unique study IDs to return
