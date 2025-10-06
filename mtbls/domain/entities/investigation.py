import logging
from typing import Annotated, Any, OrderedDict, Self, Union

from metabolights_utils.common import CamelCaseModel
from metabolights_utils.models.isa.investigation_file import (
    Assay,
    BaseSection,
    Comment,
    Factor,
    Investigation,
    InvestigationContacts,
    InvestigationPublications,
    IsaAbstractModel,
    OntologyAnnotation,
    OntologySourceReference,
    OntologySourceReferences,
    Person,
    Protocol,
    Publication,
    Study,
    StudyAssays,
    StudyContacts,
    StudyDesignDescriptors,
    StudyFactors,
    StudyProtocols,
    StudyPublications,
)
from pydantic import Field, field_validator, model_validator

from mtbls.domain.entities.base_file_object import BaseFileObject

logger = logging.getLogger(__name__)


class CommentItem(CamelCaseModel):
    name: Annotated[str, Field(description="Comment name")] = ""
    value: Annotated[str, Field(description="Comment value")] = ""


class CommentedItem(CamelCaseModel):
    comments: Annotated[list[CommentItem], Field(description="Comments")] = []

    @field_validator("comments", mode="before")
    @classmethod
    def validate_comment(cls, item: Union[None, list[Comment]]) -> list[CommentItem]:
        comments = []
        if not item:
            return comments
        if isinstance(item, list):
            for comment in item:
                value = ""
                name = ""
                if isinstance(comment, dict):
                    value = comment["value"]
                    name = comment["name"]
                else:
                    name = comment.name
                    value = comment.value

                if isinstance(value, list):
                    comments.extend(
                        [CommentItem(name=name, value=str(x)) for x in value]
                    )
                elif isinstance(value, str):
                    comments.append(CommentItem(name=name, value=value))
        return comments


class OntologySourceReferenceItem(CamelCaseModel):
    name: Annotated[
        str,
        Field(
            description="The name of the source of a term; i.e. the source controlled vocabulary or ontology."  # noqa: E501
            " These names will be used in all corresponding Term Source REF fields that occur elsewhere."  # noqa: E501
        ),
    ] = ""

    file: Annotated[
        str,
        Field(description="A file name or a URI of an official resource."),
    ] = ""
    version: Annotated[
        Union[str, int],
        Field(
            description="The version number of the Term Source to support terms tracking."  # noqa: E501
        ),
    ] = ""
    description: Annotated[
        str,
        Field(
            description="Description of source. "
            "Use for disambiguating resources when homologous prefixes have been used.",
        ),
    ] = ""


class CommentedOntologySourceReferenceItem(
    CommentedItem, OntologySourceReferenceItem
): ...


class OntologyItem(CamelCaseModel):
    term: Annotated[
        str,
        Field(description="Ontology term"),
    ] = ""

    term_accession_number: Annotated[
        str,
        Field(
            description="The accession number from the Term Source associated with the term.",  # noqa: E501
        ),
    ] = ""
    term_source_ref: Annotated[
        str,
        Field(
            description="Source reference name of ontology term. e.g., EFO, OBO, NCIT. "
            "Ontology source reference names should be defined in ontology source references section in i_Investigation.txt file",  # noqa: E501
        ),
    ] = ""


class DesignDescriptor(OntologyItem): ...


class CommentedDesignDescriptor(CommentedItem, DesignDescriptor): ...


class ValueTypeItem(CamelCaseModel):
    name: Annotated[
        str,
        Field(description=""),
    ] = ""
    type: Annotated[
        str,
        Field(description="Ontology term"),
    ] = ""
    term_accession_number: Annotated[
        str,
        Field(
            description="The accession number from the Term Source associated with the term.",  # noqa: E501
        ),
    ] = ""
    term_source_ref: Annotated[
        str,
        Field(
            description="Source reference name of ontology term. e.g., EFO, OBO, NCIT. "
            "Ontology source reference names should be defined in ontology source references section in i_Investigation.txt file",  # noqa: E501
        ),
    ] = ""


class FactorItem(CamelCaseModel):
    name: Annotated[
        str,
        Field(
            description="The name of one factor used in the study files. "
            "A factor corresponds to an independent variable manipulated by the experimentalist "  # noqa: E501
            "with the intention to affect biological systems in a way that can be measured by an assay.",  # noqa: E501
        ),
    ] = ""
    type: Annotated[
        OntologyItem,
        Field(
            description="A term allowing the classification of the factor into categories. "  # noqa: E501
            "The term is a controlled vocabulary or an ontology",
        ),
    ] = OntologyItem()


class CommentedFactorItem(CommentedItem, FactorItem): ...


class AssayItem(CamelCaseModel):
    file_name: Annotated[
        str,
        Field(
            description="Assay file name. Expected filename pattern is a_*.txt",
        ),
    ] = ""
    measurement_type: Annotated[
        OntologyItem,
        Field(
            description="A term to qualify what is being measured (e.g. metabolite identification).",  # noqa: E501
        ),
    ] = OntologyItem()
    technology_type: Annotated[
        OntologyItem,
        Field(
            description="Term to identify the technology used to perform the measurement, "  # noqa: E501
            "e.g. NMR spectrometry assay, mass spectrometry assay",
        ),
    ] = OntologyItem()
    technology_platform: Annotated[
        str,
        Field(
            description="Platform information "
            "such as assay technique name, polarity, column model, manufacturer, platform name.",  # noqa: E501
        ),
    ] = ""


class CommentedAssayItem(CommentedItem, AssayItem): ...


class PersonItem(CamelCaseModel):
    last_name: Annotated[
        str,
        Field(
            description="Last name of a person associated with the investigation or study."  # noqa: E501
        ),
    ] = ""
    first_name: Annotated[
        str,
        Field(
            description="First name of person associated with the investigation or study.",  # noqa: E501
        ),
    ] = ""
    mid_initials: Annotated[
        str,
        Field(
            description="Middle name initials (if exists) of person associated with the investigation or study",  # noqa: E501
        ),
    ] = ""
    email: Annotated[
        str,
        Field(
            description="Email address of person associated with the investigation or study",  # noqa: E501
        ),
    ] = ""
    phone: Annotated[
        str,
        Field(
            description="Phone number of person associated with the investigation or study",  # noqa: E501
        ),
    ] = ""
    fax: Annotated[
        str,
        Field(
            description="Fax number of person associated with the investigation or study",  # noqa: E501
        ),
    ] = ""
    address: Annotated[
        str,
        Field(
            description="Address of person associated with the investigation or study",
        ),
    ] = ""
    affiliation: Annotated[
        str,
        Field(
            description="Affiliation of person associated with the investigation or study",  # noqa: E501
        ),
    ] = ""
    roles: Annotated[
        list[OntologyItem],
        Field(
            description="Roles of person associated with the investigation or study. "
            "Multiple role can be defined for each person. Role is defined as an ontology term. "  # noqa: E501
            "e.g., NCIT:Investigator, NCIT:Author",
        ),
    ] = []


class CommentedPersonItem(CommentedItem, PersonItem): ...


class ProtocolItem(CamelCaseModel):
    name: Annotated[
        str,
        Field(description="Protocol name."),
    ] = ""
    protocol_type: Annotated[
        OntologyItem,
        Field(description="Term to classify the protocol."),
    ] = OntologyItem()
    description: Annotated[
        str,
        Field(description="Protocol description."),
    ] = ""
    uri: Annotated[
        str,
        Field(
            description="Pointer to external protocol resources "
            "that can be accessed by their Uniform Resource Identifier (URI).",
        ),
    ] = ""
    version: Annotated[
        str,
        Field(
            description="An identifier for the version to ensure protocol tracking.."
        ),
    ] = ""
    parameters: Annotated[
        list[OntologyItem],
        Field(description="Protocol's parameters."),
    ] = []
    components: Annotated[
        list[ValueTypeItem],
        Field(
            description="Protocol’s components; "
            "e.g. instrument names, software names, and reagents names.",
        ),
    ] = []


class CommentedProtocolItem(CommentedItem, ProtocolItem): ...


class PublicationItem(CamelCaseModel):
    pub_med_id: Annotated[
        str,
        Field(description="The PubMed IDs of the publication."),
    ] = ""
    doi: Annotated[
        str,
        Field(description="A Digital Object Identifier (DOI) for the publication."),
    ] = ""
    author_list: Annotated[
        str,
        Field(
            description="The list of authors associated with the publication. "
            "Comma (,) is recommended to define multiple authors."
        ),
    ] = ""
    title: Annotated[
        str,
        Field(
            description="The title of publication associated with the investigation."
        ),
    ] = ""
    status: Annotated[
        OntologyItem,
        Field(
            description="A term describing the status of that publication "
            "(i.e. EFO:submitted, EFO:in preparation, EFO:published).",
        ),
    ] = OntologyItem()


class CommentedPublicationItem(CommentedItem, PublicationItem): ...


class StudyItem(CamelCaseModel):
    identifier: Annotated[
        str,
        Field(
            description="A unique identifier, "
            "either a temporary identifier generated by MetaboLights repository.",
        ),
    ] = ""
    file_name: Annotated[
        str,
        Field(
            description="Name of the Sample Table file corresponding the definition of that Study.",  # noqa: E501
        ),
    ] = ""

    title: Annotated[
        str,
        Field(
            description="A concise phrase used to encapsulate the purpose and goal of the study.",  # noqa: E501
        ),
    ] = ""
    description: Annotated[
        str,
        Field(
            description="A textual description of the study, with components such as objective or goals.",  # noqa: E501
        ),
    ] = ""
    submission_date: Annotated[
        str,
        Field(
            description="The date on which the study is submitted to an archive. "
            "String formatted as ISO8601 date YYYY-MM-DD",
        ),
    ] = ""
    public_release_date: Annotated[
        str,
        Field(
            description="The date on which the study SHOULD be released publicly. "
            "String formatted as ISO8601 date YYYY-MM-DD",
        ),
    ] = ""


class CommentedStudyItem(CommentedItem, StudyItem): ...


class ExtendedStudyItem(CommentedStudyItem):
    design_descriptors: Annotated[
        list[CommentedDesignDescriptor],
        Field(description="Content of study design descriptors section."),
    ] = []
    publications: Annotated[
        list[CommentedPublicationItem],
        Field(description="Content of study publications section."),
    ] = []
    factors: Annotated[
        list[CommentedFactorItem],
        Field(description="Content of study factors section."),
    ] = []
    assays: Annotated[
        list[CommentedAssayItem],
        Field(
            description="Study assay section of i_Investigation.txt file. "
            "This section contains study assays and comments.",
        ),
    ] = []
    protocols: Annotated[
        list[CommentedProtocolItem],
        Field(description="Content of study protocols section."),
    ] = []
    contacts: Annotated[
        list[CommentedPersonItem],
        Field(description="Content of study contacts section."),
    ] = []


class InvestigationItem(CommentedItem):
    identifier: Annotated[
        str,
        Field(description="Investigation identifier."),
    ] = ""
    title: Annotated[
        str,
        Field(description="A concise name given to the investigation."),
    ] = ""
    description: Annotated[
        str,
        Field(description="A textual description of the investigation."),
    ] = ""
    submission_date: Annotated[
        str,
        Field(
            description="The date on which the investigation was reported to the MetaboLights repository. "  # noqa: E501
            "String formatted as ISO8601 date YYYY-MM-DD"
        ),
    ] = ""
    public_release_date: Annotated[
        str,
        Field(
            description="The date on which the investigation was released publicly. "
            "String formatted as ISO8601 date YYYY-MM-DD"
        ),
    ] = ""

    ontology_source_references: Annotated[
        list[CommentedOntologySourceReferenceItem],
        Field(description="Ontology sources used in the i_Investigation.txt file"),
    ] = []

    publications: Annotated[
        list[PublicationItem],
        Field(
            description="All publications prepared to report results of the investigation."  # noqa: E501
        ),
    ] = []

    contacts: Annotated[
        list[PersonItem],
        Field(description="People details of the investigation."),
    ] = []

    studies: Annotated[
        list[ExtendedStudyItem],
        Field(description="Studies carried out in the investigation."),
    ] = []

    @field_validator("ontology_source_references", mode="before")
    @classmethod
    def validate_ontology_source_references(
        cls, value
    ) -> list[CommentedOntologySourceReferenceItem]:
        if not value:
            return []
        if isinstance(value, OntologySourceReferences):
            values = [
                CommentedOntologySourceReferenceItem(
                    name=x.source_name,
                    file=x.source_file,
                    version=x.source_version,
                    description=x.source_description,
                )
                for x in value.references
            ]
            update_comments(value, values)
            return values
        return value

    @staticmethod
    def get_from_investigation(inv: Investigation) -> Self:
        investigation_item: InvestigationItem = InvestigationItem.model_validate(
            inv, from_attributes=True, strict=False
        )
        return investigation_item

    def to_investigation(self) -> Investigation:
        investigation: ExtendedInvestigation = ExtendedInvestigation.model_validate(
            self, from_attributes=True, strict=False
        )

        return investigation

    @model_validator(mode="wrap")
    @classmethod
    def validate_model(cls, v: Any, handler) -> Self:
        item: InvestigationItem = handler(v)
        if isinstance(v, Investigation):
            source: Investigation = v
            item.contacts = get_subitems(
                CommentedPersonItem,
                source.investigation_contacts,
                source.investigation_contacts.people,
            )
            item.publications = get_subitems(
                CommentedPublicationItem,
                source.investigation_publications,
                source.investigation_publications.publications,
            )
            for idx, source_study in enumerate(v.studies):
                if isinstance(source_study, Study):
                    study: StudyItem = item.studies[idx]
                    study.contacts = get_subitems(
                        CommentedPersonItem,
                        source_study.study_contacts,
                        source_study.study_contacts.people,
                    )

                    study.design_descriptors = get_subitems(
                        CommentedDesignDescriptor,
                        source_study.study_design_descriptors,
                        source_study.study_design_descriptors.design_types,
                    )
                    study.assays = get_subitems(
                        CommentedAssayItem,
                        source_study.study_assays,
                        source_study.study_assays.assays,
                    )

                    study.factors = get_subitems(
                        CommentedFactorItem,
                        source_study.study_factors,
                        source_study.study_factors.factors,
                    )
                    study.protocols = get_subitems(
                        CommentedProtocolItem,
                        source_study.study_protocols,
                        source_study.study_protocols.protocols,
                    )

                    study.publications = get_subitems(
                        CommentedPublicationItem,
                        source_study.study_publications,
                        source_study.study_publications.publications,
                    )

        return item


class ExtendedInvestigation(Investigation):
    @field_validator("ontology_source_references", mode="before")
    @classmethod
    def validate_ontology_source_references(cls, value) -> OntologySourceReferences:
        if not value:
            return OntologySourceReferences()
        if isinstance(value, list):
            return OntologySourceReferences(
                references=[
                    OntologySourceReference(
                        source_name=x.name,
                        source_file=x.file,
                        source_version=x.version,
                        source_description=x.description,
                    )
                    for x in value
                ],
                comments=build_comments(value),
            )

        return value

    @model_validator(mode="wrap")
    @classmethod
    def validate_model(cls, v: Any, handler) -> Investigation:
        item: Investigation = handler(v)
        if isinstance(v, InvestigationItem):
            source: InvestigationItem = v
            item.investigation_contacts = InvestigationContacts(
                people=[
                    Person.model_validate(x, from_attributes=True)
                    for x in source.contacts
                ],
                comments=build_comments(source.contacts),
            )

            item.investigation_publications = InvestigationPublications(
                publications=[
                    Publication.model_validate(x, from_attributes=True)
                    for x in source.publications
                ],
                comments=build_comments(source.publications),
            )

            for idx, study_item in enumerate(source.studies):
                study = item.studies[idx]
                study.study_assays = StudyAssays(
                    assays=[
                        Assay.model_validate(x, from_attributes=True)
                        for x in study_item.assays
                    ],
                    comments=build_comments(study_item.assays),
                )
                study.study_factors = StudyFactors(
                    factors=[
                        Factor.model_validate(x, from_attributes=True)
                        for x in study_item.factors
                    ],
                    comments=build_comments(study_item.factors),
                )
                study.study_protocols = StudyProtocols(
                    protocols=[
                        Protocol.model_validate(x, from_attributes=True)
                        for x in study_item.protocols
                    ],
                    comments=build_comments(study_item.protocols),
                )
                study.study_design_descriptors = StudyDesignDescriptors(
                    design_types=[
                        OntologyAnnotation.model_validate(x, from_attributes=True)
                        for x in study_item.design_descriptors
                    ],
                    comments=build_comments(study_item.design_descriptors),
                )

                study.study_contacts = StudyContacts(
                    people=[
                        Person.model_validate(x, from_attributes=True)
                        for x in study_item.contacts
                    ],
                    comments=build_comments(study_item.contacts),
                )
                study.study_publications = StudyPublications(
                    publications=[
                        Publication.model_validate(x, from_attributes=True)
                        for x in study_item.publications
                    ],
                    comments=build_comments(study_item.publications),
                )
        return item


def build_comments(items: list[CommentedItem]):
    comment_list: OrderedDict[str, Comment] = OrderedDict()
    count = len(items)
    for item in items:
        for comment in item.comments:
            if comment.name not in comment_list:
                comment_obj = Comment(name=comment.name, value=[""] * count)
                comment_list[comment.name] = comment_obj
    deleted_comments = []
    for idx, item in enumerate(items):
        for comment in item.comments:
            if comment.value:
                comment_list[comment.name].value[idx] = comment.value
    for comment in comment_list.values():
        values = set(comment.value)
        values.discard("")
        values.discard(None)
        if not values:
            deleted_comments.append(comment.name)
    if deleted_comments:
        for name in deleted_comments:
            logger.info("Comment %s is empty. It will be deleted.", name)
            del comment_list[name]

    return list(comment_list.values())


class InvestigationFileObject(BaseFileObject[InvestigationItem]): ...


def get_subitems(
    model_class: type[CommentedItem],
    source_section: BaseSection,
    source_list: list[IsaAbstractModel],
):
    values: list[CommentedItem] = [
        model_class.model_validate(x, from_attributes=True) for x in source_list
    ]
    return update_comments(source_section, values)


def update_comments(source_section: BaseSection, values: list[CommentedItem]):
    for idx, x in enumerate(values):
        for comment in source_section.comments:
            if isinstance(comment.value, str):
                if idx == 0:
                    x.comments.append(
                        CommentItem(name=comment.name, value=comment.value)
                    )
            elif isinstance(comment.value, list):
                val = ""
                if len(comment.value) > idx:
                    val = comment.value[idx]
                x.comments.append(CommentItem(name=comment.name, value=val))
    return values
