from typing import Optional, Tuple

from pydantic import BaseModel


class ElasticsearchConfiguration(BaseModel):
    origin_url: str = ""  # do we want this to be a secret?


class StudyElasticSearchConfiguration(ElasticsearchConfiguration):
    #index_name: str = "public-study-search-index"
    index_name: str = "completed-study-search-index"
    # Fields that can be searched without nested queries (top-level text/keyword fields)
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
    nested_search_fields: Tuple[Tuple[str, Tuple[str, ...]], ...] = (
        (
            "submitters",
            (
                "submitters.fullname",
                "submitters.email",
                "submitters.orcid",
                "submitters.affiliation",
                "submitters.address",
                "submitters.roles",
                "submitters.country",
            ),
        ),
        (
            "contacts",
            (
                "contacts.firstName",
                "contacts.lastName",
                "contacts.midInitials",
                "contacts.email",
                "contacts.orcid",
                "contacts.affiliation",
                "contacts.address",
                "contacts.roles.term",
            ),
        ),
        (
            "organisms",
            (
                "organisms.term",
                "organisms.termAccessionNumber",
            ),
        ),
        (
            "organismParts",
            (
                "organismParts.term",
                "organismParts.termAccessionNumber",
            ),
        ),
        (
            "assayTechniques",
            (
                "assayTechniques.name",
                "assayTechniques.main",
                "assayTechniques.technique",
                "assayTechniques.sub",
            ),
        ),
        (
            "technologyTypes",
            (
                "technologyTypes.term",
                "technologyTypes.termAccessionNumber",
            ),
        ),
        (
            "factors",
            (
                "factors.term",
                "factors.termAccessionNumber",
            ),
        ),
        (
            "designDescriptors",
            (
                "designDescriptors.term",
                "designDescriptors.termAccessionNumber",
            ),
        ),
        (
            "variants",
            (
                "variants.term",
                "variants.termAccessionNumber",
            ),
        ),
        (
            "sampleTypes",
            (
                "sampleTypes.term",
                "sampleTypes.termAccessionNumber",
            ),
        ),
        (
            "protocols",
            (
                "protocols.name",
                "protocols.description",
                "protocols.parameters.term",
                "protocols.components.name",
                "protocols.components.type",
            ),
        ),
        (
            "fundings",
            (
                "fundings.funder",
                "fundings.grantIdentifier",
                "fundings.fundRefId",
            ),
        ),
        (
            "assays",
            (
                "assays.technologyPlatform",
                "assays.measurementType.term",
                "assays.technologyType.term",
                "assays.technique.main",
                "assays.technique.sub",
                "assays.technique.technique",
            ),
        ),
    )
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
