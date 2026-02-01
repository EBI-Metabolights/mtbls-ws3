from mtbls.infrastructure.search.es.es_configuration import ElasticsearchConfiguration


class AssayElasticSearchConfiguration(ElasticsearchConfiguration):
    api_key_name: str = "assay"
    index_name: str = "assay"
    max_study_ids: int = 10000
