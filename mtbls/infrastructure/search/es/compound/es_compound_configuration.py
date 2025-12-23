
from typing import Optional, Tuple
from mtbls.infrastructure.search.es.es_configuration import ElasticsearchConfiguration


class CompoundElasticSearchConfiguration(ElasticsearchConfiguration):
    api_key_name: str = "compound"
    index_name: str = "compounds_search_v1"

    # Fields searched by the main query, with rough boosting
    search_fields: Tuple[str, ...] = (
        "id^10",             # exact IDs should win hard
        "name^5",            # primary compound name
        "synonyms^3",        # alternative names, still important
        "iupacNames^3",      # scientific names
        "definition^2",      # descriptive text
        "formula^2",         # useful for text searches on formula strings
        "inchiKey^2",
        "smiles",
        "inchi",
        "species_hits.species",   # allow species name search
    )

    facet_size: int = 25

    # Fields actually returned in the _source for result cards / details
    source_includes: Optional[Tuple[str, ...]] = (
        "id",
        "name",
        "definition",
        "synonyms",
        "iupacNames",
        "smiles",
        "inchi",
        "inchiKey",
        "formula",
        "charge",
        "averagemass",
        "exactmass",
        "flags",
        "counts",
        "species_hits",
        "spectra_count",
    )
