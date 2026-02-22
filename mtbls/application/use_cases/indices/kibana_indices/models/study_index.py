import datetime
from typing import Dict, List, Union

from metabolights_utils.models.isa.common import (
    AssayTechnique,
    OntologyItem,
    OrganismAndOrganismPartPair,
)
from metabolights_utils.models.isa.investigation_file import (
    ExtendedOntologyAnnotation,
    Factor,
    OntologyAnnotation,
    OntologySourceReference,
    Person,
    Protocol,
    Publication,
)
from metabolights_utils.models.metabolights.model import Submitter
from pydantic import Field
from typing_extensions import Annotated

from mtbls.application.use_cases.indices.kibana_indices.models.common import (
    BaseEsIndex,
    BaseIndexItem,
    Country,
)

# from app.models.isa.study_db_metadata import Submitter


class StudySampleFileItem(BaseEsIndex):
    file_path: Annotated[str, Field(description="Content of investigation file.")] = ""
    organisms: Annotated[List[OntologyItem], Field(description="organisms.")] = []
    organism_parts: Annotated[
        List[OntologyItem], Field(description="organism parts.")
    ] = []
    organism_and_organism_part_pairs: Annotated[
        List[OrganismAndOrganismPartPair],
        Field(description="."),
    ] = []
    variants: List[OntologyItem] = []
    sample_types: Annotated[List[OntologyItem], Field(description=".")] = []


class StudyAssayFileItem(BaseEsIndex):
    measurement_type: OntologyAnnotation = Field(
        OntologyAnnotation(),
    )
    technology_type: OntologyAnnotation = Field(OntologyAnnotation())
    technology_platform: str = Field("")

    file_path: str = Field("")
    referenced_assignment_files: List[str] = Field([])

    referenced_raw_file_extensions: List[str] = Field([])
    referenced_derived_file_extensions: List[str] = Field([])
    assay_technique: AssayTechnique = Field(AssayTechnique())
    number_of_assay_rows: int = Field(0)


class StudyAssignmentFileItem(BaseEsIndex):
    filePath: str = Field("", alias="file_path")
    # identifiedMetaboliteNames: List[str] = []
    # identifiedMetaboliteChebiIds: List[str] = []
    # metaboliteAssignments: Dict[str, str] = {}
    assay_technique: AssayTechnique = Field(AssayTechnique())
    number_of_rows: int = Field(0)
    number_of_assigned_rows: int = Field(0)
    number_of_unassigned_rows: int = Field(0)


class StudyIndexItem(BaseIndexItem):
    ontology_source_references: List[OntologySourceReference] = Field([])
    title: str = ""
    description: str = ""
    submission_date: Union[str, datetime.datetime] = ""
    public_release_date: Union[str, datetime.datetime] = ""
    sample_file_name: str = ""
    reserved_accession: str = ""
    reserved_request_id: str = ""
    study_design_descriptors: List[ExtendedOntologyAnnotation] = []
    publications: List[Publication] = []
    factors: List[Factor] = []
    protocols: List[Protocol] = []
    contacts: List[Person] = []
    sample_file: StudySampleFileItem = StudySampleFileItem()
    assay_files: List[StudyAssayFileItem] = []
    metabolite_assignment_files: List[StudyAssignmentFileItem] = []
    first_submitter: Person = Person()
    numeric_study_id: int = 0
    db_id: Union[str, int] = ""
    tags: List[str] = []
    labels: Dict[str, str] = {}

    last_update_datetime: Union[str, datetime.datetime] = ""
    update_date: Union[str, datetime.datetime] = ""
    status_date: Union[str, datetime.datetime] = ""
    study_size_in_bytes: int = -1
    study_size_in_str: str = ""

    referenced_raw_file_extensions: List[str] = []
    referenced_derived_file_extensions: List[str] = []
    assay_techniques: List[AssayTechnique] = []

    identified_metabolite_names: List[str] = []
    identified_metabolite_chebi_ids: List[str] = []

    factor_header_names: List[str] = []
    factor_terms: List[str] = []

    submitters: List[Submitter] = []
    country: Country = Country()
    all_countries: List[Country] = []
    referenced_file_extensions: List[str] = []
    referenced_raw_file_extensions: List[str] = []
    referenced_derived_file_extensions: List[str] = []

    number_of_samples: int = 0
    number_of_assay_rows: int = 0
    number_of_assay_files: int = 0
    number_of_metabolite_assignment_files: int = 0

    number_of_total_metabolite_assignment_rows: int = 0
    number_of_metabolite_assigned_rows: int = 0
    number_of_metabolite_unassigned_rows: int = 0
    number_of_metabolites: int = 0
    number_of_raw_files: int = 0
    number_of_derived_files: int = 0
    number_of_all_referenced_files: int = 0
    # new fields
    first_private_date: Union[str, datetime.datetime] = ""
    first_public_date: Union[str, datetime.datetime] = ""
    created_at: Union[str, datetime.datetime] = ""
    revision_date: Union[str, datetime.datetime] = ""
    revision_number: int = 0
    dateset_licence: str = ""
    study_category: int = 0
    mhd_accession: str = ""
    mhd_model_version: str = ""
    template_name: str = ""
    template_version: str = ""
    sample_template: str = ""
