from typing import Optional, Tuple
from pydantic import BaseModel

class ElasticsearchConfiguration(BaseModel):
    origin_url: str = "" # do we want this to be a secret?


class StudyElasticSearchConfiguration(ElasticsearchConfiguration):
    index_name: str = "public-study-search-index"
    search_fields: Tuple[str, ...] = (
        "title^3",
        "description",
        "organisms",
        "organismParts",
        "studyId^10",
        "submitters.fullname",
        "submitters.email",
        "submitters.orcid",
        "protocolDescriptions",
        "country",
        "annotations",
        "publications.title",
        "publications.authorList",
        "publications.doi^5",
        "publications.pubmedId^2",
        "contactFullnames^2",
        "contactEmails^2",
        
    )
    facet_size: int = 25
    
    source_includes: Optional[Tuple[str, ...]] = (
        "studyId",
        "title",
        "description",
        "publicReleaseDate",
        "modifiedTime",
        "sizeInBytes",
        "organisms",
        "organismParts",
        "assayTechniques",
        "technologyTypes",
        "country",
    )
    