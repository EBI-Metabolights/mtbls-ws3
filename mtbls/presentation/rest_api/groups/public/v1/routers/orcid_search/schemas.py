from typing import Annotated

from metabolights_utils.common import CamelCaseModel
from pydantic import Field


class CitedDataset(CamelCaseModel):
    study_accession: Annotated[str, Field(description="Cited MetaboLights dataset")] = (
        ""
    )
    is_submitter: Annotated[
        bool,
        Field(
            description="Is summitter a member of the MetaboLights dataset submitters"
        ),
    ] = False
    is_public: Annotated[
        bool,
        Field(description="Is dataset status public"),
    ] = False


class Citation(CamelCaseModel):
    title: Annotated[str, Field(description="Title of the publication")] = ""
    doi: Annotated[str, Field(description="DOI of the publication")] = ""
    pubmed_id: Annotated[str, Field(description="PubMedId of the publication")] = ""
    authors: Annotated[str, Field(description="Authors of the publication")] = ""
    journal: Annotated[str, Field(description="Journal of the publication")] = ""
    publication_date: Annotated[
        str, Field(description="Publication year of the publication")
    ] = ""
    cited_datasets: Annotated[
        None | list[CitedDataset],
        Field(description="Cited MetaboLights datasets."),
    ] = None


class MetaboLightsStudyCitation(CamelCaseModel):
    study_accession: Annotated[
        str, Field(description="MetaboLights Study accession number")
    ] = ""
    study_title: Annotated[
        str, Field(description="Title of the MetaboLights study")
    ] = ""

    publications: Annotated[
        list[Citation],
        Field(
            description="Publications that cite the MetaboLights study",
        ),
    ] = []
    is_public: Annotated[
        bool,
        Field(description="Is the MetaboLights study status public"),
    ] = False
    is_submitter: Annotated[
        bool,
        Field(
            description="Is ORCID owner a member of the MetaboLights study submitters"
        ),
    ] = False


class EuroPmcSearchResult(CamelCaseModel):
    study_list: Annotated[
        list[MetaboLightsStudyCitation],
        Field(
            description="Is ORCID owner a member of the MetaboLights study submitters"
        ),
    ] = []
