from typing import Any

import pytest
from metabolights_utils.models.isa.investigation_file import (
    Factor,
    OntologyAnnotation,
    OntologySourceReference,
    Study,
)
from metabolights_utils.models.metabolights.model import (
    MetabolightsStudyModel,
    Submitter,
)

from mtbls.domain.domain_services.modifier.metabolights_study_model_modifier import (
    InvestigationFileModifier,
)


@pytest.fixture(scope="function")
def modifier(
    templates: dict[str, Any],
    control_lists: dict[str, Any],
    metabolights_model: MetabolightsStudyModel,
) -> InvestigationFileModifier:
    modifier = InvestigationFileModifier(
        model=metabolights_model,
        templates=templates,
        control_lists=control_lists,
    )
    return modifier


@pytest.fixture
def ms_modifier(
    templates: dict[str, Any],
    control_lists: dict[str, Any],
    ms_metabolights_model: MetabolightsStudyModel,
) -> InvestigationFileModifier:
    modifier = InvestigationFileModifier(
        model=ms_metabolights_model,
        templates=templates,
        control_lists=control_lists,
    )
    return modifier


class TestNoModification:
    @pytest.mark.asyncio
    async def test_modify_no_modification_01(
        self,
        modifier: InvestigationFileModifier,
    ):
        modifier.modify()
        assert len(modifier.update_logs) == 0

    @pytest.mark.asyncio
    async def test_modify_no_modification_02(
        self,
        ms_modifier: InvestigationFileModifier,
    ):
        ms_modifier.modify()
        assert len(ms_modifier.update_logs) == 0


class TestUpdateTrailingSpaces:
    @pytest.mark.asyncio
    async def test_remove_trailing_01(
        self,
        modifier: InvestigationFileModifier,
    ):
        metabolights_model = modifier.model
        study = metabolights_model.investigation.studies[0]
        expected_title = study.title
        study.title = f" {study.title} \"' "
        modifier.update_trailing_spaces(
            [metabolights_model.investigation], "Investigation", list_items=True
        )
        assert expected_title == study.title
        assert len(modifier.update_logs) == 1

    @pytest.mark.asyncio
    async def test_remove_trailing_02(
        self,
        modifier: InvestigationFileModifier,
    ):
        metabolights_model = modifier.model
        study = metabolights_model.investigation.studies[0]
        role = study.study_contacts.people[0].roles[0]
        role_term = role.term
        role_accession = role.term_accession_number
        role.term = f" {role_term}"
        role.term_accession_number = f" {role_accession}"
        modifier.update_trailing_spaces(
            [metabolights_model.investigation], "Investigation", list_items=True
        )
        assert role.term == role_term
        assert role.term_accession_number == role_accession
        expected_logs_count = 2

        assert len(modifier.update_logs) == expected_logs_count

    @pytest.mark.asyncio
    async def test_remove_trialing_03(
        self,
        modifier: InvestigationFileModifier,
    ):
        metabolights_model = modifier.model
        studies = metabolights_model.investigation.studies
        study = studies[0]

        expected_title = study.title
        study.title = f" {study.title} \"' "
        modifier.update_trailing_spaces(
            [studies],
            "Investigation File Studies",
            list_items=True,
        )
        assert expected_title == study.title
        assert len(modifier.update_logs) == 1


class TestUpdateOntologies:
    @pytest.mark.asyncio
    async def test_update_ontologies_01(
        self,
        modifier: InvestigationFileModifier,
    ):
        modifier.update_ontologies()

        assert len(modifier.update_logs) == 0

    @pytest.mark.asyncio
    async def test_update_ontologies_02(
        self,
        modifier: InvestigationFileModifier,
    ):
        contact = modifier.model.investigation.studies[0].study_contacts.people[0]
        contact.roles[0].term = ""
        contact.roles[0].term_source_ref = "MTBLS"

        contact.roles[0].term_accession_number = ""

        modifier.update_ontologies()
        assert contact.roles[0].term == ""
        assert contact.roles[0].term_source_ref == ""
        assert contact.roles[0].term_accession_number == ""
        assert len(modifier.update_logs) == 1

    @pytest.mark.asyncio
    async def test_update_ontologies_03(
        self,
        modifier: InvestigationFileModifier,
    ):
        contact = modifier.model.investigation.studies[0].study_contacts.people[0]
        contact.roles[0].term = ""
        contact.roles[0].term_source_ref = ""

        contact.roles[
            0
        ].term_accession_number = "http://purl.obolibrary.org/obo/NCIT_C25936"

        modifier.update_ontologies()
        assert contact.roles[0].term == "Investigator"
        assert contact.roles[0].term_source_ref == "NCIT"
        assert (
            contact.roles[0].term_accession_number
            == "http://purl.obolibrary.org/obo/NCIT_C25936"
        )
        assert len(modifier.update_logs) == 1

    @pytest.mark.asyncio
    async def test_update_ontologies_04(
        self,
        modifier: InvestigationFileModifier,
    ):
        studies = modifier.model.investigation.studies
        studies.clear()
        modifier.update_ontologies()
        assert len(modifier.update_logs) == 0

    @pytest.mark.asyncio
    async def test_update_ontologies_05(
        self,
        modifier: InvestigationFileModifier,
    ):
        metabolights_model = modifier.model
        study = metabolights_model.investigation.studies[0]
        publication = study.study_publications.publications[0]
        expected_term_accession_number = publication.status.term_accession_number
        expected_term_source_ref = publication.status.term_source_ref

        publication.status.term_accession_number = ""
        publication.status.term_source_ref = ""
        modifier.update_ontologies()
        assert (
            expected_term_accession_number == publication.status.term_accession_number
        )
        assert expected_term_source_ref == publication.status.term_source_ref
        assert len(modifier.update_logs) == 1

    @pytest.mark.asyncio
    async def test_update_ontologies_06(
        self,
        modifier: InvestigationFileModifier,
    ):
        metabolights_model = modifier.model
        study = metabolights_model.investigation.studies[0]
        design_type = study.study_design_descriptors.design_types[5]
        expected_term_accession_number = design_type.term_accession_number
        expected_term_source_ref = design_type.term_source_ref

        design_type.term_accession_number = (
            "http://purl.obolibrary.org/obo/CHMO_0000591"
        )
        design_type.term_source_ref = "NCIT"
        modifier.update_ontologies()
        assert expected_term_accession_number == design_type.term_accession_number
        assert expected_term_source_ref == design_type.term_source_ref
        assert len(modifier.update_logs) == 1


class Test_rule_i_100_200_003_01:
    @pytest.mark.asyncio
    async def test_rule_i_100_200_003_01_01(
        self,
        modifier: InvestigationFileModifier,
    ):
        metabolights_model = modifier.model
        expected_value = metabolights_model.investigation.submission_date
        metabolights_model.study_db_metadata.submission_date = expected_value
        metabolights_model.investigation.submission_date = "13/12/2023"
        modifier.rule_i_100_200_003_01()
        assert expected_value == metabolights_model.investigation.submission_date
        assert len(modifier.update_logs) == 1


class Test_rule_i_100_200_004_01:
    @pytest.mark.asyncio
    async def test_rule_i_100_200_004_01_01(
        self,
        modifier: InvestigationFileModifier,
    ):
        metabolights_model = modifier.model
        expected_value = metabolights_model.investigation.public_release_date
        metabolights_model.study_db_metadata.release_date = expected_value
        metabolights_model.investigation.public_release_date = "13/12/2023"
        modifier.rule_i_100_200_004_01()
        assert expected_value == metabolights_model.investigation.public_release_date
        assert len(modifier.update_logs) == 1


class Test_rule_i_100_300_001_01:
    @pytest.mark.asyncio
    async def test_rule_i_100_300_001_01_01(
        self,
        modifier: InvestigationFileModifier,
    ):
        metabolights_model = modifier.model
        metabolights_model.investigation.studies = []
        modifier.rule_i_100_300_001_01()
        assert len(metabolights_model.investigation.studies) == 1
        assert len(modifier.update_logs) > 0


class Test_rule_i_100_300_001_02:
    @pytest.mark.asyncio
    async def test_rule_i_100_300_001_02_01(
        self,
        modifier: InvestigationFileModifier,
    ):
        metabolights_model = modifier.model
        metabolights_model.investigation.studies.append(Study())
        modifier.rule_i_100_300_001_02()
        assert len(metabolights_model.investigation.studies) == 1
        assert len(modifier.update_logs) == 1


class Test_rule_i_100_300_002_01:
    @pytest.mark.asyncio
    async def test_rule_i_100_300_002_01_01(
        self,
        modifier: InvestigationFileModifier,
    ):
        metabolights_model = modifier.model
        study = metabolights_model.investigation.studies[0]

        expected = study.identifier
        metabolights_model.study_db_metadata.study_id = expected
        study.identifier = "InvalidIdentifier"
        modifier.rule_i_100_300_002_01()
        assert study.identifier == expected
        assert len(modifier.update_logs) == 1

    @pytest.mark.asyncio
    async def test_rule_i_100_300_002_01_02(
        self,
        modifier: InvestigationFileModifier,
    ):
        metabolights_model = modifier.model
        investigation = metabolights_model.investigation
        expected = investigation.identifier
        metabolights_model.study_db_metadata.study_id = expected
        investigation.identifier = "InvalidIdentifier"
        modifier.rule_i_100_300_002_01()
        assert investigation.identifier == expected
        assert len(modifier.update_logs) == 1


class Test_rule_i_100_300_003_01:
    @pytest.mark.asyncio
    async def test_rule_i_100_300_003_01_01(
        self,
        modifier: InvestigationFileModifier,
    ):
        metabolights_model = modifier.model
        study = metabolights_model.investigation.studies[0]
        expected = study.title
        study.title = "  ".join(expected.split())
        modifier.rule_i_100_300_003_01()
        assert study.title == expected
        assert len(modifier.update_logs) == 1


class Test_rule_i_100_300_003_02:
    @pytest.mark.asyncio
    async def test_rule_i_100_300_003_02_01(
        self,
        modifier: InvestigationFileModifier,
    ):
        metabolights_model = modifier.model
        investigation = metabolights_model.investigation
        expected = investigation.title
        investigation.title = "  ".join(expected.split())
        modifier.rule_i_100_300_003_02()
        assert investigation.title == expected
        assert len(modifier.update_logs) == 1


class Test_rule_i_100_300_005_01:
    @pytest.mark.asyncio
    async def test_rule_i_100_300_005_01_01(
        self,
        modifier: InvestigationFileModifier,
    ):
        metabolights_model = modifier.model
        study = metabolights_model.investigation.studies[0]
        expected = study.submission_date
        metabolights_model.study_db_metadata.submission_date = expected
        study.submission_date = ""
        modifier.rule_i_100_300_005_01()
        assert study.submission_date == expected
        assert len(modifier.update_logs) == 1


class Test_rule_i_100_300_006_01:
    @pytest.mark.asyncio
    async def test_rule_i_100_300_006_01_01(
        self,
        modifier: InvestigationFileModifier,
    ):
        metabolights_model = modifier.model
        study = metabolights_model.investigation.studies[0]
        expected = study.public_release_date
        metabolights_model.study_db_metadata.release_date = expected

        study.public_release_date = ""
        modifier.rule_i_100_300_006_01()
        assert study.public_release_date == expected
        assert len(modifier.update_logs) == 1


class Test_rule_i_100_320_002_01:
    @pytest.mark.asyncio
    async def test_rule_i_100_320_002_01_01(
        self,
        modifier: InvestigationFileModifier,
    ):
        metabolights_model = modifier.model
        study = metabolights_model.investigation.studies[0]
        publication = study.study_publications.publications[0]
        expected = publication.doi
        publication.doi = "https://doi.org/10.1152/physiolgenomics.00194.2006"
        modifier.rule_i_100_320_002_01()
        assert publication.doi == expected
        assert len(modifier.update_logs) == 1


class Test_rule_i_100_320_007_01:
    @pytest.mark.asyncio
    async def test_rule_i_100_320_007_01_01(
        self,
        modifier: InvestigationFileModifier,
    ):
        metabolights_model = modifier.model
        study = metabolights_model.investigation.studies[0]
        publication = study.study_publications.publications[0]
        expected_source = publication.status.term_source_ref
        expected_accession = publication.status.term_accession_number
        publication.status.term_source_ref = ""
        publication.status.term_accession_number = ""
        modifier.rule_i_100_320_007_01()
        assert publication.status.term_accession_number == expected_accession
        assert publication.status.term_source_ref == expected_source
        assert len(modifier.update_logs) == 1

    @pytest.mark.asyncio
    async def test_rule_i_100_320_007_01_02(
        self,
        modifier: InvestigationFileModifier,
    ):
        metabolights_model = modifier.model
        study = metabolights_model.investigation.studies[0]
        publication = study.study_publications.publications[0]
        expected_term = "in preparation"
        publication.status.term = ""
        publication.status.term_source_ref = ""
        publication.status.term_accession_number = ""
        modifier.rule_i_100_320_007_01()
        assert publication.status.term == expected_term
        assert len(modifier.update_logs) == 1


class Test_rule_i_100_350_007_01:
    @pytest.mark.asyncio
    async def test_rule_i_100_350_007_01_02(
        self,
        modifier: InvestigationFileModifier,
    ):
        metabolights_model = modifier.model
        study = metabolights_model.investigation.studies[0]
        nmr_spectrometry_protocol = study.study_protocols.protocols[3]
        parameter_names = {x.term for x in nmr_spectrometry_protocol.parameters}
        params = {x.term: x for x in nmr_spectrometry_protocol.parameters}

        nmr_spectrometry_protocol.parameters.remove(params["Magnetic field strength"])

        modifier.rule_i_100_350_007_01()
        study = metabolights_model.investigation.studies[0]
        nmr_spectrometry_protocol = study.study_protocols.protocols[3]
        actual_values = {x.term for x in nmr_spectrometry_protocol.parameters}
        assert len(parameter_names.difference(actual_values)) == 0
        assert len(modifier.update_logs) == 1


class Test_rule_i_100_360_004_01:
    @pytest.mark.asyncio
    async def test_rule_i_100_360_004_01_01(
        self,
        modifier: InvestigationFileModifier,
    ):
        metabolights_model = modifier.model
        study = metabolights_model.investigation.studies[0]
        people = study.study_contacts.people
        people.clear()
        modifier.rule_i_100_360_004_01()

        assert len(people) == 1
        contact = people[0]
        email = "test@ebi.ac.uk"
        submitters = metabolights_model.study_db_metadata.submitters

        submitters.append(
            Submitter(
                first_name="Test",
                last_name="Last name",
                email=email,
                user_name=email,
                affiliation="EBI",
            )
        )
        modifier.rule_i_100_360_004_01()
        submitter = metabolights_model.study_db_metadata.submitters[0]
        assert contact.first_name == submitter.first_name
        assert contact.last_name == "Last Name"
        assert contact.email == submitter.user_name
        assert contact.affiliation == submitter.affiliation
        assert contact.roles[0].term == "Author"
        expected_logs_count = 5

        assert len(modifier.update_logs) == expected_logs_count

    @pytest.mark.asyncio
    async def test_rule_i_100_360_004_01_02(
        self,
        modifier: InvestigationFileModifier,
    ):
        metabolights_model = modifier.model
        study = metabolights_model.investigation.studies[0]
        people = study.study_contacts.people
        expected_contacts_count = len(people) - 1
        people[1].first_name = ""
        people[1].last_name = ""
        modifier.rule_i_100_360_004_01()

        assert len(people) == expected_contacts_count
        assert len(modifier.update_logs) == 1

    @pytest.mark.asyncio
    async def test_rule_i_100_360_004_01_03(
        self,
        modifier: InvestigationFileModifier,
    ):
        metabolights_model = modifier.model
        study = metabolights_model.investigation.studies[0]
        people = study.study_contacts.people
        email = people[0].email
        people[0].email = ""
        submitters = metabolights_model.study_db_metadata.submitters
        submitters.append(
            Submitter(
                first_name=people[0].first_name.upper(),
                last_name=people[0].last_name.upper(),
                email=email,
                user_name=email,
            )
        )
        modifier.rule_i_100_360_004_01()

        assert people[0].email == submitters[0].user_name
        assert len(modifier.update_logs) == 1

    @pytest.mark.asyncio
    async def test_rule_i_100_360_004_01_04(
        self,
        modifier: InvestigationFileModifier,
    ):
        metabolights_model = modifier.model
        study = metabolights_model.investigation.studies[0]
        people = study.study_contacts.people
        people[0].first_name = "R"
        people[0].last_name = "S"
        modifier.rule_i_100_360_004_01()

        assert people[0].first_name == "R."
        assert people[0].last_name == "S."
        expected_logs_count = 2
        assert len(modifier.update_logs) == expected_logs_count

    @pytest.mark.asyncio
    async def test_rule_i_100_360_004_01_05(
        self,
        modifier: InvestigationFileModifier,
    ):
        metabolights_model = modifier.model
        study = metabolights_model.investigation.studies[0]
        people = study.study_contacts.people
        people[0].roles.clear()
        modifier.rule_i_100_360_004_01()

        assert people[0].roles[0].term == "Author"
        assert people[0].roles[0].term_source_ref == "NCIT"
        assert (
            people[0].roles[0].term_accession_number
            == "http://purl.obolibrary.org/obo/NCIT_C42781"
        )
        assert len(modifier.update_logs) == 1


class TestUpperCaseName:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "in_out",
        [
            ("", ""),
            (" ", ""),
            (None, ""),
            ("test", "Test"),
            ("l", "L"),
            ("L.", "L."),
            ("test result", "Test Result"),
            ("test ", "Test"),
            ("Test Result", "Test Result"),
        ],
    )
    async def test_upper_case_name_01(
        self, modifier: InvestigationFileModifier, in_out: tuple[str, str]
    ):
        name = in_out[0]
        expected = in_out[1]
        actual = modifier.upper_case_name(name)

        assert actual == expected
        assert len(modifier.update_logs) == 0


class TestUpdateAssayDefaults:
    @pytest.mark.asyncio
    async def test_update_assay_defaults_01(
        self,
        modifier: InvestigationFileModifier,
    ):
        metabolights_model = modifier.model
        study = metabolights_model.investigation.studies[0]
        assay = study.study_assays.assays[0]
        measurement_type = assay.measurement_type.term
        technology_type = assay.technology_type.term
        technology_platform = assay.technology_platform
        assay.technology_platform = "Bruker"
        assay.technology_type.term = ""
        assay.measurement_type.term = ""
        modifier.update_assay_defaults()
        assert technology_platform == assay.technology_platform
        assert technology_type == assay.technology_type.term
        assert measurement_type == assay.measurement_type.term
        expected_logs_count = 3

        assert len(modifier.update_logs) == expected_logs_count

    @pytest.mark.asyncio
    async def test_update_assay_defaults_02(
        self,
        ms_modifier: InvestigationFileModifier,
    ):
        metabolights_model = ms_modifier.model
        study = metabolights_model.investigation.studies[0]
        assay = study.study_assays.assays[0]
        assay.technology_platform = ""
        ms_modifier.update_assay_defaults()
        assert (
            assay.technology_platform
            == "Liquid Chromatography MS - positive - reverse phase"
        )
        assert len(ms_modifier.update_logs) == 1

    @pytest.mark.asyncio
    async def test_update_assay_defaults_03(
        self,
        ms_modifier: InvestigationFileModifier,
    ):
        metabolights_model = ms_modifier.model
        study = metabolights_model.investigation.studies[0]
        assay = study.study_assays.assays[0]
        assay.technology_platform = "test"
        ms_modifier.update_assay_defaults()
        assert (
            assay.technology_platform
            == "Liquid Chromatography MS - positive - reverse phase - test"
        )
        assert len(ms_modifier.update_logs) == 1

    @pytest.mark.asyncio
    async def test_update_assay_defaults_04(
        self,
        ms_modifier: InvestigationFileModifier,
    ):
        metabolights_model = ms_modifier.model
        study = metabolights_model.investigation.studies[0]
        assay = study.study_assays.assays[0]
        old_name = assay.file_name
        assay.file_name = "a_MTBLS1_GC-MS_a.txt"
        assay.technology_platform = (
            "Liquid Chromatography MS - positive - reverse phase - test"
        )

        metabolights_model.assays[assay.file_name] = metabolights_model.assays[old_name]
        del metabolights_model.assays[old_name]
        metabolights_model.assays[assay.file_name].file_path = assay.file_name
        ms_modifier.rule_i_100_340_009_01()
        assert len(ms_modifier.update_logs) == 1
        ms_modifier.update_assay_defaults()
        assert len(ms_modifier.update_logs) == 2


class TestUpdateStudyFactorNames:
    @pytest.mark.asyncio
    async def test_update_study_factor_names_01(
        self,
        modifier: InvestigationFileModifier,
    ):
        metabolights_model = modifier.model
        study = metabolights_model.investigation.studies[0]
        factors = study.study_factors.factors
        expected_number_of_factors = len(factors)
        factors.append(Factor())
        modifier.update_study_factor_names()
        assert len(factors) == expected_number_of_factors
        assert len(modifier.update_logs) == 1

    @pytest.mark.asyncio
    async def test_update_study_factor_names_02(
        self,
        modifier: InvestigationFileModifier,
    ):
        metabolights_model = modifier.model
        study = metabolights_model.investigation.studies[0]
        factors = study.study_factors.factors
        expected_number_of_factors = len(factors)
        factors.append(Factor(name="gender", type=OntologyAnnotation(term="gender")))
        modifier.update_study_factor_names()
        assert len(factors) == expected_number_of_factors
        assert len(modifier.update_logs) == 1

    @pytest.mark.asyncio
    async def test_update_study_factor_names_03(
        self,
        modifier: InvestigationFileModifier,
    ):
        metabolights_model = modifier.model
        study = metabolights_model.investigation.studies[0]
        factors = study.study_factors.factors
        expected_number_of_factors = len(factors)
        factors.remove(factors[1])
        modifier.update_study_factor_names()
        assert factors[1].name == "Metabolic syndrome"
        assert factors[1].type.term == "Metabolic syndrome"
        assert len(factors) == expected_number_of_factors
        assert len(modifier.update_logs) == 1

    @pytest.mark.asyncio
    async def test_update_study_factor_names_04(
        self,
        modifier: InvestigationFileModifier,
    ):
        metabolights_model = modifier.model
        study = metabolights_model.investigation.studies[0]
        factors = study.study_factors.factors
        expected_number_of_factors = len(factors)
        factors[1].name = "metabolic Syndrome"
        modifier.update_study_factor_names()
        assert factors[1].name == "Metabolic syndrome"
        assert len(factors) == expected_number_of_factors
        assert len(modifier.update_logs) == 1

    @pytest.mark.asyncio
    async def test_update_study_factor_names_05(
        self,
        modifier: InvestigationFileModifier,
    ):
        metabolights_model = modifier.model
        study = metabolights_model.investigation.studies[0]
        factors = study.study_factors.factors
        expected_number_of_factors = len(factors) + 2
        factors.append(Factor(name="test-URL"))
        factors.append(Factor(name="l Test-Data"))
        modifier.update_study_factor_names()

        assert len(factors) == expected_number_of_factors
        expected_logs_count = 2
        assert len(modifier.update_logs) == expected_logs_count


class TestUpdateOntologySources:
    @pytest.mark.asyncio
    async def test_update_ontology_sources_01(
        self,
        ms_modifier: InvestigationFileModifier,
    ):
        ontology_sources = (
            ms_modifier.model.investigation.ontology_source_references.references
        )

        source_name = ontology_sources[0].source_name
        source_file = ontology_sources[0].source_file
        source_description = ontology_sources[0].source_description
        source_version = ontology_sources[0].source_version
        ontology_sources[0].source_version = ""
        ontology_sources[0].source_description = ""
        ontology_sources[0].source_file = ""
        ontology_sources[0].source_name = source_name

        ms_modifier.update_ontology_sources()

        assert ontology_sources[0].source_name == source_name
        assert ontology_sources[0].source_file == source_file
        assert ontology_sources[0].source_description == source_description
        assert ontology_sources[0].source_version == source_version
        assert len(ms_modifier.update_logs) == 1

    @pytest.mark.asyncio
    async def test_update_ontology_sources_02(
        self,
        ms_modifier: InvestigationFileModifier,
    ):
        ontology_sources = (
            ms_modifier.model.investigation.ontology_source_references.references
        )

        source_name = ontology_sources[0].source_name
        source_file = ontology_sources[0].source_file
        source_description = ontology_sources[0].source_description
        source_version = ontology_sources[0].source_version
        ontology_sources.remove(ontology_sources[0])

        ms_modifier.update_ontology_sources()

        assert ontology_sources[0].source_name == source_name
        assert ontology_sources[0].source_file == source_file
        assert ontology_sources[0].source_description == source_description
        assert ontology_sources[0].source_version == source_version
        expected_logs_count = 2
        assert len(ms_modifier.update_logs) == expected_logs_count

    @pytest.mark.asyncio
    async def test_update_ontology_sources_03(
        self,
        ms_modifier: InvestigationFileModifier,
    ):
        ontology_sources = (
            ms_modifier.model.investigation.ontology_source_references.references
        )

        source_name = ontology_sources[-1].source_name
        source_file = ontology_sources[-1].source_file
        source_description = ontology_sources[-1].source_description
        source_version = ontology_sources[-1].source_version
        ontology_sources.remove(ontology_sources[-1])

        ms_modifier.update_ontology_sources()

        assert ontology_sources[-1].source_name == source_name
        assert ontology_sources[-1].source_file == source_file
        assert ontology_sources[-1].source_description == source_description
        assert ontology_sources[-1].source_version == source_version
        expected_logs_count = 1
        assert len(ms_modifier.update_logs) == expected_logs_count

    @pytest.mark.asyncio
    async def test_update_ontology_sources_04(
        self,
        ms_modifier: InvestigationFileModifier,
    ):
        investigation = ms_modifier.model.investigation
        ontology_sources = investigation.ontology_source_references.references
        current_source_count = len(ontology_sources)
        ontology_sources.append(OntologySourceReference(source_name="Test"))

        ms_modifier.update_ontology_sources()
        assert len(ontology_sources) == current_source_count
        assert len(ms_modifier.update_logs) == 1

    @pytest.mark.asyncio
    async def test_update_ontology_sources_05(
        self,
        ms_modifier: InvestigationFileModifier,
    ):
        investigation = ms_modifier.model.investigation
        ontology_sources = investigation.ontology_source_references.references
        current_source_count = len(ontology_sources)
        ontology_sources.append(OntologySourceReference(source_name="Test"))
        ontology_sources.append(OntologySourceReference(source_name="Test2"))

        ms_modifier.update_ontology_sources()
        assert len(ontology_sources) == current_source_count
        assert len(ms_modifier.update_logs) == 1


class TestUpdateProtocolParameters:
    @pytest.mark.asyncio
    async def test_update_protocol_parameters_01(
        self,
        modifier: InvestigationFileModifier,
    ):
        metabolights_model = modifier.model
        study = metabolights_model.investigation.studies[0]
        protocols = study.study_protocols.protocols
        expected_number_of_parameters = len(protocols[0].parameters)
        protocols[0].parameters.append(OntologyAnnotation())
        modifier.update_protocol_parameters()
        assert len(protocols[0].parameters) == expected_number_of_parameters
        assert len(modifier.update_logs) == 1

    @pytest.mark.asyncio
    async def test_update_protocol_parameters_02(
        self,
        modifier: InvestigationFileModifier,
    ):
        metabolights_model = modifier.model
        study = metabolights_model.investigation.studies[0]
        protocols = study.study_protocols.protocols
        expected_number_of_parameters = len(protocols[1].parameters)

        first_param = protocols[1].parameters[0]
        protocols[1].parameters.append(OntologyAnnotation(term="Test"))
        protocols[1].parameters.remove(first_param)
        modifier.update_protocol_parameters()
        assert len(protocols[1].parameters) == expected_number_of_parameters
        expected_logs_count = 2
        assert len(modifier.update_logs) == expected_logs_count

    @pytest.mark.asyncio
    async def test_update_protocol_parameters_03(
        self,
        modifier: InvestigationFileModifier,
    ):
        metabolights_model = modifier.model
        study = metabolights_model.investigation.studies[0]
        protocols = study.study_protocols.protocols
        expected_number_of_parameters = len(protocols[2].parameters)

        first_param = protocols[2].parameters[0]
        protocols[2].parameters.remove(first_param)
        protocols[2].parameters.append(first_param)
        modifier.update_protocol_parameters()
        assert len(protocols[2].parameters) == expected_number_of_parameters
        assert len(modifier.update_logs) == 1

    @pytest.mark.asyncio
    async def test_update_protocol_parameters_04(
        self,
        modifier: InvestigationFileModifier,
    ):
        metabolights_model = modifier.model
        templates = modifier.templates
        study = metabolights_model.investigation.studies[0]
        protocols = study.study_protocols.protocols
        expected_number_of_parameters = len(protocols[2].parameters) + 1
        template_parameters = templates["studyProtocolTemplates"]["NMR"]["protocols"][
            2
        ]["parameters"]

        template_parameters.append("Test")
        modifier.update_protocol_parameters()
        assert expected_number_of_parameters == len(protocols[2].parameters)
        assert len(modifier.update_logs) == 1
