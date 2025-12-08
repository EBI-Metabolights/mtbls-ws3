from fastapi.openapi.models import Example

from mtbls.domain.entities.validation.validation_configuration import (
    BaseOntologyValidation,
    OntologyTerm,
    OntologyValidationType,
    ParentOntologyTerms,
)

ONTOLOGY_SEARCH_BODY_EXAMPLES = {
    "Search terms in any ontology": Example(
        summary="Search terms in any ontology. "
        "Results will be sorted the provided ontology order",
        value=BaseOntologyValidation(
            field_name="Characteristics[Organism part]",
            rule_name="organism-part-rule-01",
            validation_type=OntologyValidationType.ANY_ONTOLOGY_TERM,
            ontologies=["EFO", "NCBITAXON", "UBERON", "BTO", "NCIT", "MSIO"],
            allowed_parent_ontology_terms=None,
        ).model_dump(by_alias=True),
    ),
    "Search organism ontology terms": Example(
        summary="Search organism ontology terms from the selected ontologies",
        value=BaseOntologyValidation(
            field_name="Characteristics[Organism]",
            rule_name="organism-rule-01",
            validation_type=OntologyValidationType.SELECTED_ONTOLOGY,
            ontologies=["NCBITAXON", "ENVO"],
            allowed_parent_ontology_terms=None,
        ).model_dump(by_alias=True),
    ),
    "Search organism part ontology terms": Example(
        summary="Search organism part ontology terms from the selected ontologies",
        value=BaseOntologyValidation(
            field_name="Characteristics[Organism part]",
            rule_name="organism-part-rule-02",
            validation_type=OntologyValidationType.SELECTED_ONTOLOGY,
            ontologies=["UBERON", "BTO", "NCIT", "MSIO"],
            allowed_parent_ontology_terms=None,
        ).model_dump(by_alias=True),
    ),
    "Search disease terms": Example(
        summary="Search disease terms from the selected ontologies",
        value=BaseOntologyValidation(
            field_name="Characteristics[Disease]",
            rule_name="disease-rule-02",
            validation_type=OntologyValidationType.SELECTED_ONTOLOGY,
            ontologies=["UBERON", "BTO", "NCIT", "MSIO"],
            allowed_parent_ontology_terms=None,
        ).model_dump(by_alias=True),
    ),
    "Search unit terms in the selected ontologies": Example(
        summary="Search unit ontology terms in parent ontology terms",
        value=BaseOntologyValidation(
            field_name="Unit",
            rule_name="unit-rule-02",
            validation_type=OntologyValidationType.CHILD_ONTOLOGY_TERM,
            ontologies=["UO", "EFO", "NCIT"],
            allowed_parent_ontology_terms=ParentOntologyTerms(
                exclude_by_label_pattern=[r"^.*unit$", r"^.*Unit of.*$"],
                exclude_by_accession=[],
                parents=[
                    OntologyTerm(
                        term="unit",
                        term_source_ref="UO",
                        term_accession_number="http://purl.obolibrary.org/obo/UO_0000000",
                    ),
                    OntologyTerm(
                        term="unit",
                        term_source_ref="EFO",
                        term_accession_number="http://purl.obolibrary.org/obo/UO_0000000",
                    ),
                    OntologyTerm(
                        term="Qualifier",
                        term_source_ref="NCIT",
                        term_accession_number="http://purl.obolibrary.org/obo/NCIT_C41009",
                    ),
                    OntologyTerm(
                        term="Unit of Measure",
                        term_source_ref="NCIT",
                        term_accession_number="http://purl.obolibrary.org/obo/NCIT_C25709",
                    ),
                ],
            ),
        ).model_dump(by_alias=True),
    ),
    "Search mass spectrometry instruments": Example(
        summary="Search mass spectrometry instrument "
        "ontology terms in parent ontology terms",
        value=BaseOntologyValidation(
            field_name="Parameter Value[Instrument]",
            rule_name="ms-intrument-rule-02",
            validation_type=OntologyValidationType.CHILD_ONTOLOGY_TERM,
            ontologies=["MS"],
            allowed_parent_ontology_terms=ParentOntologyTerms(
                exclude_by_label_pattern=["^.*instrument model"],
                exclude_by_accession=[],
                parents=[
                    OntologyTerm(
                        term="instrument model",
                        term_source_ref="MS",
                        term_accession_number="http://purl.obolibrary.org/obo/MS_1000031",
                    )
                ],
            ),
        ).model_dump(by_alias=True),
    ),
    "Search study factor ontology terms": Example(
        summary="Search study factor ontology terms in selected ontologies",
        value=BaseOntologyValidation(
            field_name="Study Factor Type",
            rule_name="study-factor-type-02",
            validation_type=OntologyValidationType.ANY_ONTOLOGY_TERM,
            ontologies=["BTO", "EFO", "NCIT"],
            allowed_parent_ontology_terms=None,
        ).model_dump(by_alias=True),
    ),
    "Search study contact role ontology terms": Example(
        summary="Search study contact role ontology terms in parent ontology term",
        value=BaseOntologyValidation(
            field_name="Study Contact Roles",
            rule_name="study-contact-roles-02",
            validation_type=OntologyValidationType.CHILD_ONTOLOGY_TERM,
            ontologies=["NCIT"],
            allowed_parent_ontology_terms=ParentOntologyTerms(
                exclude_by_label_pattern=[],
                exclude_by_accession=[],
                parents=[
                    OntologyTerm(
                        term="Personnel",
                        term_source_ref="NCIT",
                        term_accession_number="http://purl.obolibrary.org/obo/NCIT_C60758",
                    )
                ],
            ),
        ).model_dump(by_alias=True),
    ),
}
