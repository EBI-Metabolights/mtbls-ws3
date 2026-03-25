from mtbls.infrastructure.search.es.es_configuration import ElasticsearchConfiguration


class SampleElasticSearchConfiguration(ElasticsearchConfiguration):
    api_key_name: str = "sample"
    index_name: str = "sample"
    max_study_ids: int = 10000
