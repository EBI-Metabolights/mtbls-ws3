from typing import Optional, Tuple

from pydantic import BaseModel


class ElasticsearchConfiguration(BaseModel):
    origin_url: str = ""  # do we want this to be a secret?
    api_key_name: Optional[str] = None


class StudyElasticSearchConfiguration(ElasticsearchConfiguration):
    api_key_name: str = "study"
    index_name: str = "completed-study-search-index"
    search_fields: Tuple[str, ...] = (
        "studyId^8",
        "title^3",
        "description",
        "annotations",
        "curatorAnnotations",
        "publications.title",
        "publications.authorList",
        "publications.doi^5",
        "publications.pubMedId^2",
    )
    # Nested search fields grouped by path; the gateway will add nested queries for these
    nested_search_fields: Tuple[Tuple[str, Tuple[str, ...]], ...] = ()
    facet_size: int = 25

    source_includes: Optional[Tuple[str, ...]] = (
        "studyId",
        "title",
        "description",
        "status",
        "curationRequest",
        "publicReleaseDate",
        "modifiedTime",
        "sizeInBytes",
        "sizeInText",
        "publications",
        "protocols",
        "organisms",
        "organismParts",
        "variants",
        "sampleTypes",
        "assayTechniques",
        "technologyTypes",
        "designDescriptors",
        "factors",
        "submitters",
        "contacts",
        "assays",
        "fundings",
        "curatorAnnotations",
        "annotations",
        "submissionDate",
        "investigationFilePath",
        "sampleFilePath",
    )
