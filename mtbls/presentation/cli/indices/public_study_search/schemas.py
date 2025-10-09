from __future__ import annotations

import datetime
from typing import Dict, Generic, List, Set, Union

from metabolights_utils.models.metabolights.model import CurationRequest
from pydantic import ConfigDict, Field, computed_field
from typing_extensions import Annotated

from mtbls.presentation.rest_api.core.base import APIBaseModel, T
from mtbls.presentation.rest_api.core.responses import (
    APIPaginatedResponse,
    PaginatedResult,
)


class OntologySourceReferenceModel(APIBaseModel):
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
            description="The version number of the Term Source "
            "to support terms tracking.",
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


class OntologyModel(APIBaseModel):
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
            description="The accession number from the Term Source "
            "associated with the selected term."
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


class ValueAndUnitModel(APIBaseModel):
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


class ContactModel(APIBaseModel):
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


class AssayTechniqueModel(APIBaseModel):
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


class Contact(APIBaseModel):
    fullname: str = ""
    email: str = ""
    orcid: str = ""
    affiliation: str = ""
    address: str = ""
    roles: list[str] = []


class Submitter(Contact):
    country: str = ""


class AssayModel(APIBaseModel):
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
            description="A term to identify the technology used "
            "to perform the measurement."
        ),
    ] = AssayTechniqueModel()
    assignment_files: Annotated[
        Set[str],
        Field(
            description="Relative paths of metabolite assignment files "
            "referenced in the assay file."
        ),
    ] = set()

    model_config = ConfigDict(from_attributes=True)

    def __hash__(self) -> int:
        return hash(self.file_path)


class StudyPublicationModel(APIBaseModel):
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


class StudyProtocolModel(APIBaseModel):
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
        set[OntologyModel],
        Field(description="Protocol parameters."),
    ] = ""

    components: Annotated[
        set[ProtocolComponentModel],
        Field(description="Protocol components."),
    ] = ""
    model_config = ConfigDict(from_attributes=True)

    def __hash__(self) -> int:
        return hash(self.protocol_type)


class PublicStudyLiteIndexBaseModel(APIBaseModel):
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
        set[StudyPublicationModel],
        Field(
            description="Studies.",
            json_schema_extra={"ontology_category": "organism"},
        ),
    ] = set()

    protocols: Annotated[
        set[StudyProtocolModel],
        Field(
            description="protocols and their definitions.",
            json_schema_extra={"nested": True},
        ),
    ] = set()

    organisms: Annotated[
        set[OntologyModel],
        Field(
            description="Organism terms defined in sample table.",
            json_schema_extra={"ontology_category": "organism"},
        ),
    ] = set()

    organism_parts: Annotated[
        set[OntologyModel],
        Field(
            description="Organism part terms defined in sample table.",
            json_schema_extra={"ontology_category": "organism_part"},
        ),
    ] = set()

    variants: Annotated[
        set[OntologyModel],
        Field(
            description="Variant terms defined in sample table.",
            json_schema_extra={"ontology_category": "variant"},
        ),
    ] = set()

    sample_types: Annotated[
        set[OntologyModel],
        Field(
            description="Sample type terms defined in sample table.",
            json_schema_extra={"ontology_category": "sample_type"},
        ),
    ] = set()

    factors: Annotated[
        set[OntologyModel],
        Field(
            description="Factors of the study.",
            json_schema_extra={"ontology_category": "factor"},
        ),
    ] = set()

    design_descriptors: Annotated[
        set[OntologyModel],
        Field(
            description="Design descriptor terms of the study.",
            json_schema_extra={"ontology_category": "design_descriptor"},
        ),
    ] = set()

    assay_techniques: Annotated[
        set[AssayTechniqueModel],
        Field(description="Assay techniques of the study."),
    ] = set()

    technology_types: Annotated[
        set[OntologyModel],
        Field(
            description="Technology types of the study."
            "Technology types may be `mass spectrometry assay`, "
            "`NMR spectrometry assay` "
            "or both for MetaboLights repository."
        ),
    ] = set()

    model_config = ConfigDict(from_attributes=True)

    def __hash__(self) -> int:
        return hash(self.study_id)


class FundingModel(APIBaseModel):
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


class ProtocolComponentModel(APIBaseModel):
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
        set[FundingModel],
        Field(description="Founders and grant Ids of study."),
    ] = set()

    contacts: Annotated[
        set[ContactModel],
        Field(description="Contacts of the study."),
    ] = set()

    assays: Annotated[
        set[AssayModel],
        Field(description="Assay descriptions of the study."),
    ] = set()

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


class BaseAggregationBucket(APIBaseModel):
    key: str = ""
    item_count: int = 0


class AggregationBucket(BaseAggregationBucket):
    buckets: Annotated[List[AggregationResponse], Field()] = None


class AggregationResponse(APIBaseModel):
    name: str = ""
    other_items_count: Annotated[int, Field()] = 0
    buckets: Annotated[
        Union[List[BaseAggregationBucket], List[AggregationBucket]],
        Field(),
    ] = []


class PaginatedSearchResult(PaginatedResult, Generic[T]):
    aggregations: Annotated[
        List[AggregationResponse],
        Field(description="Aggregation results in the response."),
    ] = {}


AggregationBucket.model_rebuild()


class PaginatedSearchResultResponse(APIPaginatedResponse, Generic[T]):
    """
    API response model for paginated results.
    """

    content: Annotated[
        Union[None, PaginatedSearchResult],
        Field(
            description="Paginated data and metadata of the response with aggregations."
        ),
    ] = None


class PublicStudyLiteSearchResult(APIBaseModel):
    studies: Annotated[List[PublicStudyLiteIndexModel], Field(description="")] = []
    aggregations: Annotated[
        Dict[str, List[AggregationResponse]], Field(description="")
    ] = []


class PublicStudyLiteIndexReferences(APIBaseModel):
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


class Annotation(APIBaseModel):
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


class KeyValueModel(APIBaseModel):
    key: str = ""
    value: str = ""


class MetabolightsIndexItem(APIBaseModel):
    item_id: str = ""
    item_type: str = ""
    annotations: List[str] = []
    data: List[KeyValueModel] = []


class MetabolightsSearchIndexModel(APIBaseModel):
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


COUNTRIES = {
    "AF": "Afghanistan",
    "AL": "Albania",
    "DZ": "Algeria",
    "AS": "American Samoa",
    "AD": "Andorra",
    "AO": "Angola",
    "AI": "Anguilla",
    "AQ": "Antarctica",
    "AG": "Antigua and Barbuda",
    "AR": "Argentina",
    "AM": "Armenia",
    "AW": "Aruba",
    "AU": "Australia",
    "AT": "Austria",
    "AZ": "Azerbaijan",
    "BS": "Bahamas",
    "BH": "Bahrain",
    "BD": "Bangladesh",
    "BB": "Barbados",
    "BY": "Belarus",
    "BE": "Belgium",
    "BZ": "Belize",
    "BJ": "Benin",
    "BM": "Bermuda",
    "BT": "Bhutan",
    "BO": "Bolivia",
    "BA": "Bosnia and Herzegovina",
    "BW": "Botswana",
    "BV": "Bouvet Island",
    "BR": "Brazil",
    "BN": "Brunei Darussalam",
    "BG": "Bulgaria",
    "BF": "Burkina Faso",
    "BI": "Burundi",
    "KH": "Cambodia",
    "CM": "Cameroon",
    "CA": "Canada",
    "CV": "Cape Verde",
    "KY": "Cayman Islands",
    "CF": "Central African Republic",
    "TD": "Chad",
    "CL": "Chile",
    "CN": "China",
    "CX": "Christmas Island",
    "CC": "Cocos (Keeling) Islands",
    "CO": "Colombia",
    "KM": "Comoros",
    "CG": "Congo, Republic of The",
    "CD": "Congo, The Democratic Republic of The",
    "CK": "Cook Islands",
    "CR": "Costa Rica",
    "CI": "Cote D'ivoire",
    "HR": "Croatia",
    "CU": "Cuba",
    "CY": "Cyprus",
    "CZ": "Czech Republic",
    "DK": "Denmark",
    "DJ": "Djibouti",
    "DM": "Dominica",
    "DO": "Dominican Republic",
    "EC": "Ecuador",
    "EG": "Egypt",
    "SV": "El Salvador",
    "GQ": "Equatorial Guinea",
    "ER": "Eritrea",
    "EE": "Estonia",
    "ET": "Ethiopia",
    "FK": "Falkland Islands (Malvinas)",
    "FO": "Faroe Islands",
    "FJ": "Fiji",
    "FI": "Finland",
    "FR": "France",
    "GF": "French Guiana",
    "PF": "French Polynesia",
    "TF": "French Southern Territories",
    "GA": "Gabon",
    "GM": "Gambia",
    "GE": "Georgia",
    "DE": "Germany",
    "GH": "Ghana",
    "GI": "Gibraltar",
    "GR": "Greece",
    "GL": "Greenland",
    "GD": "Grenada",
    "GP": "Guadeloupe",
    "GU": "Guam",
    "GT": "Guatemala",
    "GG": "Guernsey",
    "GN": "Guinea",
    "GW": "Guinea-bissau",
    "GY": "Guyana",
    "HT": "Haiti",
    "HN": "Honduras",
    "HK": "Hong Kong",
    "HU": "Hungary",
    "IS": "Iceland",
    "IN": "India",
    "ID": "Indonesia",
    "IR": "Iran, Islamic Republic of",
    "IQ": "Iraq",
    "IE": "Ireland",
    "IM": "Isle of Man",
    "IL": "Israel",
    "IT": "Italy",
    "JM": "Jamaica",
    "JP": "Japan",
    "JE": "Jersey",
    "JO": "Jordan",
    "KZ": "Kazakhstan",
    "KE": "Kenya",
    "KI": "Kiribati",
    "KP": "Korea,  Democratic People's Republic of",
    "KR": "Korea,  Republic of",
    "KW": "Kuwait",
    "KG": "Kyrgyzstan",
    "LA": "Lao People's Democratic Republic",
    "LV": "Latvia",
    "LB": "Lebanon",
    "LS": "Lesotho",
    "LR": "Liberia",
    "LY": "Libyan Arab Jamahiriya",
    "LI": "Liechtenstein",
    "LT": "Lithuania",
    "LU": "Luxembourg",
    "MO": "Macao",
    "MK": "Macedonia,  The Former Yugoslav Republic of",
    "MG": "Madagascar",
    "MW": "Malawi",
    "MY": "Malaysia",
    "MV": "Maldives",
    "ML": "Mali",
    "MT": "Malta",
    "MH": "Marshall Islands",
    "MQ": "Martinique",
    "MR": "Mauritania",
    "MU": "Mauritius",
    "YT": "Mayotte",
    "MX": "Mexico",
    "FM": "Micronesia,  Federated States of",
    "MD": "Moldova,  Republic of",
    "MC": "Monaco",
    "MN": "Mongolia",
    "ME": "Montenegro",
    "MS": "Montserrat",
    "MA": "Morocco",
    "MZ": "Mozambique",
    "MM": "Myanmar",
    "NA": "Namibia",
    "NR": "Nauru",
    "NP": "Nepal",
    "NL": "Netherlands",
    "AN": "Netherlands Antilles",
    "NC": "New Caledonia",
    "NZ": "New Zealand",
    "NI": "Nicaragua",
    "NE": "Niger",
    "NG": "Nigeria",
    "NU": "Niue",
    "NF": "Norfolk Island",
    "NO": "Norway",
    "OM": "Oman",
    "PK": "Pakistan",
    "PW": "Palau",
    "PS": "State of Palestine",
    "PA": "Panama",
    "PG": "Papua New Guinea",
    "PY": "Paraguay",
    "PE": "Peru",
    "PH": "Philippines",
    "PN": "Pitcairn",
    "PL": "Poland",
    "PT": "Portugal",
    "PR": "Puerto Rico",
    "QA": "Qatar",
    "RE": "Reunion",
    "RO": "Romania",
    "RU": "Russian Federation",
    "RW": "Rwanda",
    "SH": "Saint Helena",
    "KN": "Saint Kitts and Nevis",
    "LC": "Saint Lucia",
    "PM": "Saint Pierre and Miquelon",
    "VC": "Saint Vincent and The Grenadines",
    "WS": "Samoa",
    "SM": "San Marino",
    "ST": "Sao Tome and Principe",
    "SA": "Saudi Arabia",
    "SN": "Senegal",
    "RS": "Serbia",
    "SC": "Seychelles",
    "SL": "Sierra Leone",
    "SG": "Singapore",
    "SK": "Slovakia",
    "SI": "Slovenia",
    "SB": "Solomon Islands",
    "SO": "Somalia",
    "ZA": "South Africa",
    "GS": "South Georgia and The South Sandwich Islands",
    "ES": "Spain",
    "LK": "Sri Lanka",
    "SD": "Sudan",
    "SR": "Suriname",
    "SJ": "Svalbard and Jan Mayen",
    "SZ": "Swaziland",
    "SE": "Sweden",
    "CH": "Switzerland",
    "SY": "Syrian Arab Republic",
    "TW": "Taiwan",
    "TJ": "Tajikistan",
    "TZ": "Tanzania,  United Republic of",
    "TH": "Thailand",
    "TL": "Timor-leste",
    "TG": "Togo",
    "TK": "Tokelau",
    "TO": "Tonga",
    "TT": "Trinidad and Tobago",
    "TN": "Tunisia",
    "TR": "Turkey",
    "TM": "Turkmenistan",
    "TC": "Turks and Caicos Islands",
    "TV": "Tuvalu",
    "UG": "Uganda",
    "UA": "Ukraine",
    "AE": "United Arab Emirates",
    "GB": "United Kingdom",
    "US": "United States",
    "UY": "Uruguay",
    "UZ": "Uzbekistan",
    "VU": "Vanuatu",
    "VE": "Venezuela",
    "VN": "Viet Nam",
    "VG": "Virgin Islands,  British",
    "VI": "Virgin Islands,  U.S.",
    "WF": "Wallis and Futuna",
    "EH": "Western Sahara",
    "YE": "Yemen",
    "ZM": "Zambia",
    "ZW": "Zimbabwe",
}
