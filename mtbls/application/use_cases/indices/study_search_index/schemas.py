from __future__ import annotations

import datetime
from typing import Any, List, Set, Union

from metabolights_utils.common import CamelCaseModel
from metabolights_utils.models.metabolights.model import CurationRequest
from pydantic import ConfigDict, Field, computed_field, field_validator
from typing_extensions import Annotated


def coerce_ontology_item(v: Any) -> OntologyModel:
    if isinstance(v, OntologyModel):
        return v
    if isinstance(v, str):
        # best-effort: keep the label, leave refs empty
        return OntologyModel(term=v, term_source_ref="", term_accession_number="")
    if isinstance(v, dict):
        # allow both snake_case and camelCase keys if your cache varies
        return OntologyModel(
            term=v.get("term", ""),
            term_source_ref=v.get("termSourceRef")
            or v.get("term_source_ref", "")
            or "",
            term_accession_number=v.get("termAccessionNumber")
            or v.get("term_accession_number", "")
            or "",
        )
    raise TypeError(f"Unsupported ontology item type: {type(v)}")


class OntologySourceReferenceModel(CamelCaseModel):
    """
    Ontology Source used elsewhere in the ISA-Tab files
    within the context of an Investigation.
    """

    source_name: Annotated[
        str,
        Field(description="The name of the source of a term."),
    ] = ""

    source_file: Annotated[
        str,
        Field(description="A file name or a URI of an official resource."),
    ] = ""
    source_version: Annotated[
        str,
        Field(
            exclude=True,
            description="The version number of the Term Source to support terms tracking.",
        ),
    ] = ""
    source_description: Annotated[
        str,
        Field(description="Source description."),
    ] = ""
    model_config = ConfigDict(from_attributes=True)

    def __hash__(self) -> int:
        return hash(f"{self.source_name}:{self.source_file}:{self.source_description}")

    def __eq__(self, __value: object) -> bool:
        if not isinstance(__value, OntologySourceReferenceModel):
            return False
        other: OntologySourceReferenceModel = __value
        if (
            self.source_name == other.source_name
            and self.source_file == other.source_file
            and self.source_description == other.source_description
        ):
            return True
        return False


class OntologyModel(CamelCaseModel):
    term: Annotated[
        str,
        Field(description="Term of the ontology."),
    ] = ""
    term_source_ref: Annotated[
        str,
        Field(
            description="Term source reference. "
            "The Source REF has to match one the Term Source Name "
            "declared in the in the Ontology Source Reference section."
        ),
    ] = ""
    term_accession_number: Annotated[
        str,
        Field(
            description="The accession number from the Term Source associated with the selected term."
        ),
    ] = ""
    model_config = ConfigDict(from_attributes=True)

    def __hash__(self) -> int:
        return hash(str(self))

    def __str__(self) -> str:
        return f"{self.term},{self.term_source_ref},{self.term_accession_number}"

    def __eq__(self, __value: object) -> bool:
        if not isinstance(__value, OntologyModel):
            return False
        return str(self) == str(__value)

    def __lt__(self, __value: object) -> bool:
        if not isinstance(__value, OntologyModel):
            return False
        return str(self) < str(__value)

    def __gt__(self, __value: object) -> bool:
        if not isinstance(__value, OntologyModel):
            return False
        return str(self) > str(__value)


class ValueAndUnitModel(CamelCaseModel):
    value: Annotated[
        str,
        Field(description="Value"),
    ] = ""
    unit_term: Annotated[
        str,
        Field(description="Unit of the value"),
    ] = ""
    term_accession_number: Annotated[
        str,
        Field(description="The accession number for unit."),
    ] = ""

    term_source_ref: Annotated[
        str,
        Field(description="The ontology source name that this unit term comes from."),
    ] = ""

    def __hash__(self) -> int:
        return hash(str(self))

    def __str__(self) -> str:
        return ",".join(
            [
                self.value,
                self.unit_term,
                self.term_source_ref,
                self.term_accession_number,
            ]
        )

    def __eq__(self, __value: object) -> bool:
        if not isinstance(__value, ValueAndUnitModel):
            return False
        return str(self) == str(__value)

    def __lt__(self, __value: object) -> bool:
        if not isinstance(__value, ValueAndUnitModel):
            return False
        return str(self) < str(__value)

    def __gt__(self, __value: object) -> bool:
        if not isinstance(__value, ValueAndUnitModel):
            return False
        return str(self) > str(__value)


class ContactModel(CamelCaseModel):
    last_name: Annotated[
        str,
        Field(
            description="The last name of a person "
            "associated with the study or investigation."
        ),
    ] = ""
    first_name: Annotated[
        str,
        Field(description="Study or Investigation Person Name"),
    ] = ""
    mid_initials: Annotated[
        str,
        Field(
            description="The middle initials of a person "
            "associated with the study or investigation."
        ),
    ] = ""
    email: Annotated[
        str,
        Field(
            description="The email address of a person "
            "associated with the study or investigation."
        ),
    ] = ""
    address: Annotated[
        str,
        Field(
            description="The address of a person associated "
            "with the study or investigation."
        ),
    ] = ""
    affiliation: Annotated[
        str,
        Field(
            description="The organization affiliation "
            "for a person associated with the study or investigation."
        ),
    ] = ""
    roles: Annotated[
        List[OntologyModel],
        Field(
            description="Term to classify the role(s) "
            "performed by a person in the context of "
            "the study or investigation."
        ),
    ] = []

    orcid: Annotated[
        str,
        Field(
            description="A unique Open Researcher and "
            "Contributor IDentifier (ORCID) of person."
        ),
    ] = ""

    model_config = ConfigDict(from_attributes=True)

    def __hash__(self) -> int:
        return hash(f"{self.email}:{self.last_name}:{self.first_name}")


class AssayTechniqueModel(CamelCaseModel):
    name: Annotated[
        str,
        Field(description="Short name of assay sub-technique."),
    ] = ""
    main: Annotated[
        str,
        Field(description="Name of assay main technique. MS or NMR."),
    ] = ""
    technique: Annotated[
        str,
        Field(description="Name of assay technique."),
    ] = ""
    sub: Annotated[
        str,
        Field(description="Name of assay sub-technique."),
    ] = ""
    model_config = ConfigDict(from_attributes=True)

    def __hash__(self) -> int:
        return hash(self.name)


class Contact(CamelCaseModel):
    fullname: str = ""
    email: str = ""
    orcid: str = ""
    affiliation: str = ""
    address: str = ""
    roles: list[str] = []


class Submitter(Contact):
    country: str = ""


class AssayModel(CamelCaseModel):
    file_path: Annotated[
        str,
        Field(description="Relative path of an assay file."),
    ] = ""
    measurement_type: Annotated[
        OntologyModel,
        Field(
            description="A term to qualify the endpoint, or what is being measured. "
            "It is `metabolite profiling` for MetaboLights repository."
        ),
    ] = OntologyModel()
    technology_type: Annotated[
        OntologyModel,
        Field(
            description="A term to identify the technology used "
            "to perform the measurement"
            "It is `mass spectrometry` or `NMR spectrometry` "
            "for MetaboLights repository."
        ),
    ] = OntologyModel()
    technology_platform: Annotated[
        str,
        Field(description="Manufacturer and platform name."),
    ] = ""
    technique: Annotated[
        AssayTechniqueModel,
        Field(
            description="A term to identify the technology used to perform the measurement."
        ),
    ] = AssayTechniqueModel()
    assignment_files: Annotated[
        List[str],
        Field(
            description="Relative paths of metabolite assignment files referenced in the assay file."
        ),
    ] = []

    model_config = ConfigDict(from_attributes=True)

    def __hash__(self) -> int:
        return hash(self.file_path)


class StudyPublicationModel(CamelCaseModel):
    pub_med_id: Annotated[
        str,
        Field(description="PubMed ID of publication."),
    ] = ""
    doi: Annotated[
        str,
        Field(description="DOI of publication."),
    ] = ""
    author_list: Annotated[
        str,
        Field(description="Author list of publication."),
    ] = ""
    title: Annotated[
        str,
        Field(description="Title of publication."),
    ] = ""
    status: Annotated[
        OntologyModel,
        Field(description="Status ontology of publication."),
    ] = OntologyModel()
    model_config = ConfigDict(from_attributes=True)

    def __hash__(self) -> int:
        return hash(self.title)


class StudyProtocolModel(CamelCaseModel):
    name: Annotated[
        str,
        Field(description="Protocol name."),
    ] = ""
    protocol_type: Annotated[
        OntologyModel,
        Field(description="Protocol type ontology."),
    ] = OntologyModel()

    description: Annotated[
        str,
        Field(description="Protocol description."),
    ] = ""

    uri: Annotated[
        str,
        Field(description="Protocol URI."),
    ] = ""
    version: Annotated[
        str,
        Field(description="Protocol version."),
    ] = ""
    parameters: Annotated[
        List[OntologyModel],
        Field(description="Protocol parameters."),
    ] = []

    components: Annotated[
        List[ProtocolComponentModel],
        Field(description="Protocol components."),
    ] = []

    model_config = ConfigDict(from_attributes=True)

    def __hash__(self) -> int:
        return hash(self.protocol_type)


class PublicStudyLiteIndexBaseModel(CamelCaseModel):
    public_release_date: Annotated[
        Union[None, datetime.datetime],
        Field(description="The date on which the study was released publicly."),
    ] = None

    curation_request: Annotated[
        CurationRequest, Field(description="Curation Request.")
    ] = ""

    study_id: Annotated[str, Field(description="Study identifier.")] = ""

    title: Annotated[
        str,
        Field(
            description="A concise phrase used to encapsulate the purpose "
            "and goal of the study."
        ),
    ] = ""

    size_in_bytes: Annotated[
        int,
        Field(description="Size of the study data and metadata file in bytes."),
    ] = 0

    size_in_text: Annotated[
        str,
        Field(
            description="Human readible format of the study data and metadata size.",
            examples=["13.01MB"],
        ),
    ] = ""

    publications: Annotated[
        List[StudyPublicationModel],
        Field(
            description="Studies.",
            json_schema_extra={"ontology_category": "organism"},
        ),
    ] = []

    protocols: Annotated[
        List[StudyProtocolModel],
        Field(
            description="protocols and their definitions.",
            json_schema_extra={"nested": True},
        ),
    ] = []

    organisms: Annotated[
        List[OntologyModel],
        Field(
            description="Organism terms defined in sample table.",
            json_schema_extra={"ontology_category": "organism"},
        ),
    ] = []

    organism_parts: Annotated[
        List[OntologyModel],
        Field(
            description="Organism part terms defined in sample table.",
            json_schema_extra={"ontology_category": "organism_part"},
        ),
    ] = []

    variants: Annotated[
        List[OntologyModel],
        Field(
            description="Variant terms defined in sample table.",
            json_schema_extra={"ontology_category": "variant"},
        ),
    ] = []

    sample_types: Annotated[
        List[OntologyModel],
        Field(
            description="Sample type terms defined in sample table.",
            json_schema_extra={"ontology_category": "sample_type"},
        ),
    ] = []

    factors: Annotated[
        List[OntologyModel],
        Field(
            description="Factors of the study.",
            json_schema_extra={"ontology_category": "factor"},
        ),
    ] = []

    design_descriptors: Annotated[
        List[OntologyModel],
        Field(
            description="Design descriptor terms of the study.",
            json_schema_extra={"ontology_category": "design_descriptor"},
        ),
    ] = []

    assay_techniques: Annotated[
        List[AssayTechniqueModel],
        Field(description="Assay techniques of the study."),
    ] = []

    technology_types: Annotated[
        List[OntologyModel],
        Field(
            description="Technology types of the study."
            "Technology types may be `mass spectrometry assay`, "
            "`NMR spectrometry assay` "
            "or both for MetaboLights repository."
        ),
    ] = []

    @field_validator(
        "organisms",
        "organism_parts",
        "variants",
        "sample_types",
        "factors",
        "design_descriptors",
        "technology_types",
        mode="before",
    )
    @classmethod
    def _coerce_ontology_lists(cls, v):
        if v is None:
            return []
        if not isinstance(v, list):
            v = list(v)  # in case a set sneaks in
        return [coerce_ontology_item(x) for x in v]

    model_config = ConfigDict(from_attributes=True)

    def __hash__(self) -> int:
        return hash(self.study_id)


class FundingModel(CamelCaseModel):
    funder: Annotated[
        str,
        Field(
            description="The name of the funding agency(s) that have sponsored "
            "the author and the study."
        ),
    ] = ""

    fund_ref_id: Annotated[
        str,
        Field(
            description="The unique fund reference identifier "
            "for the funding agency(s)."
        ),
    ] = ""

    grant_identifier: Annotated[
        str,
        Field(description="Unique grant identifier provided by the funding agency(s)."),
    ] = ""

    def __hash__(self) -> int:
        return hash(f"{self.funder}:{self.fund_ref_id}:{self.grant_identifier}")


class ProtocolComponentModel(CamelCaseModel):
    name: Annotated[
        str,
        Field(
            description="A component of protocol; e.g. instrument name, software name, "
            "and reagents name, etc."
        ),
    ] = ""
    type: Annotated[
        str,
        Field(description="Term to classify the protocol component."),
    ] = ""
    term_accession_number: Annotated[
        str,
        Field(
            description="The accession number for protocol component type "
            "from the term source reference associated."
        ),
    ] = ""

    term_source_ref: Annotated[
        str,
        Field(
            description="The controlled vocabulary or ontology source name "
            "that this protocol term comes from."
        ),
    ] = ""


class PublicStudyLiteIndexModel(PublicStudyLiteIndexBaseModel):
    modified_time: Annotated[
        Union[None, datetime.datetime],
        Field(description="Last modified time of the index item."),
    ] = None

    annotations: Annotated[
        List[str],
        Field(description="Annotatoions of the index item."),
    ] = []

    submission_date: Annotated[
        Union[None, datetime.datetime],
        Field(
            description="The date on which the investigation was reported "
            "to the MetaboLights repository."
        ),
    ] = None

    investigation_file_path: Annotated[
        str,
        Field(description="Investigation file name."),
    ] = "i_Investigation.txt"

    sample_file_path: Annotated[
        str,
        Field(description="Relative path of the study sample table file."),
    ] = ""

    description: Annotated[
        str,
        Field(
            description="A textual description or abstract of the study, "
            "with components such as objective or goals."
        ),
    ] = ""

    status: Annotated[
        str,
        Field(description="Public study status. Public or Public - Unreviewed"),
    ] = ""

    fundings: Annotated[
        List[FundingModel],
        Field(description="Founders and grant Ids of study."),
    ] = []

    contacts: Annotated[
        List[ContactModel],
        Field(description="Contacts of the study."),
    ] = []

    assays: Annotated[
        List[AssayModel],
        Field(description="Assay descriptions of the study."),
    ] = []

    submitters: Annotated[
        List[Submitter],
        Field(description="Submitter fullname and e-mail."),
    ] = []

    curator_annotations: Annotated[
        List[str],
        Field(description="Curator annotations."),
    ] = []

    def __hash__(self) -> int:
        return hash(self.study_id)


# class BaseAggregationBucket(CamelCaseModel):
#     key: str = ""
#     item_count: int = 0


# class AggregationBucket(BaseAggregationBucket):
#     buckets: Annotated[List[AggregationResponse], Field()] = None


# class AggregationResponse(CamelCaseModel):
#     name: str = ""
#     other_items_count: Annotated[int, Field()] = 0
#     buckets: Annotated[
#         Union[List[BaseAggregationBucket], List[AggregationBucket]],
#         Field(),
#     ] = []


# class PaginatedSearchResult(PaginatedResult, Generic[T]):
#     aggregations: Annotated[
#         List[AggregationResponse],
#         Field(description="Aggregation results in the response."),
#     ] = {}


# AggregationBucket.model_rebuild()


# class PaginatedSearchResultResponse(APIPaginatedResponse, Generic[T]):
#     """
#     API response model for paginated results.
#     """

#     content: Annotated[
#         Union[None, PaginatedSearchResult],
#         Field(
#             description="Paginated data and metadata of the response with aggregations."
#         ),
#     ] = None


# class PublicStudyLiteSearchResult(CamelCaseModel):
#     studies: Annotated[List[PublicStudyLiteIndexModel], Field(description="")] = []
#     aggregations: Annotated[
#         Dict[str, List[AggregationResponse]], Field(description="")
#     ] = []


class PublicStudyLiteIndexReferences(CamelCaseModel):
    model_config = ConfigDict(from_attributes=True)

    source_references: Annotated[Set[OntologySourceReferenceModel], Field()] = set()
    assay_techniques: Annotated[Set[AssayTechniqueModel], Field()] = set()

    organisms: Annotated[
        Set[OntologyModel], Field(json_schema_extra={"ontology_category": "organism"})
    ] = set()
    organism_parts: Annotated[
        Set[OntologyModel],
        Field(json_schema_extra={"ontology_category": "organism_part"}),
    ] = set()

    variants: Annotated[
        Set[OntologyModel], Field(json_schema_extra={"ontology_category": "variant"})
    ] = set()
    sample_types: Annotated[
        Set[OntologyModel],
        Field(json_schema_extra={"ontology_category": "sample_type"}),
    ] = set()
    factors: Annotated[
        Set[OntologyModel], Field(json_schema_extra={"ontology_category": "factor"})
    ] = set()
    design_descriptors: Annotated[
        Set[OntologyModel],
        Field(json_schema_extra={"ontology_category": "design_descriptor"}),
    ] = set()
    technology_types: Annotated[
        Set[OntologyModel],
        Field(json_schema_extra={"ontology_category": "technology_type"}),
    ] = set()
    protocol_names: Annotated[
        Set[OntologyModel],
        Field(json_schema_extra={"ontology_category": "protocol_name"}),
    ] = set()


class Annotation(CamelCaseModel):
    key: str = ""
    value: Union[str, OntologyModel, ValueAndUnitModel] = ""

    @computed_field
    @property
    def annotation(self) -> str:
        val = self.value if self.value else ""
        key = self.key if self.key else ""
        if isinstance(val, str):
            return f"{key}::{val}" if key else f"{val}"
        elif isinstance(self.value, OntologyModel):
            return f"{key}::{str(self.value)}"
        elif isinstance(self.value, ValueAndUnitModel):
            return f"{key}::{str(self.value)}"
        return str(val)

    def __str__(self) -> str:
        return self.annotation

    def __lt__(self, other: Annotation) -> bool:
        if not other:
            return False
        return self.annotation == other.annotation


class KeyValueModel(CamelCaseModel):
    key: str = ""
    value: str = ""


class MetabolightsIndexItem(CamelCaseModel):
    item_id: str = ""
    item_type: str = ""
    annotations: List[str] = []
    data: List[KeyValueModel] = []


class MetabolightsSearchIndexModel(CamelCaseModel):
    study_id: str = ""
    modified_time: Union[None, datetime.datetime] = None
    title: str = ""
    status: str = ""
    description: str = ""
    size: str = ""
    annotations: List[str] = []
    factors: List[str] = []
    funders: List[str] = []
    design_descriptors: List[str] = []
    organisms: List[str] = []
    organism_parts: List[str] = []
    variants: List[str] = []
    sample_types: List[str] = []
    protocol_descriptions: List[str] = []
    assay_techniques: List[str] = []
    assay_main_techniques: List[str] = []
    assay_sub_techniques: List[str] = []
    technology_types: List[str] = []
    public_release_date: Union[None, datetime.datetime] = None
    submission_date: Union[None, datetime.datetime] = None
    contact_fullnames: List[str] = []
    contact_emails: List[str] = []
    size_in_bytes: int = 0
    organism_superkingdom: str = ""
    organism_kingdom: str = ""
    organism_phylum: str = ""
    organism_class: str = ""
    organism_order: str = ""
    organism_family: str = ""
    organism_genus: str = ""
    organism_species: str = ""
    url: str = ""
    grant_ids: List[str] = []
    study_types: List[str] = []
    submitters: List[Submitter] = []
    contacts: List[Contact] = []
    publications: List[StudyPublicationModel] = []
    country: str = ""


annotation_reserved_keys = [
    "studyId",
    "sampleId",
    "investigationFile",
    "assayFile",
]
