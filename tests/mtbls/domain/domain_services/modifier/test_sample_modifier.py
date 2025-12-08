from typing import Any

import pytest
from metabolights_utils.models.isa.investigation_file import Factor, OntologyAnnotation
from metabolights_utils.models.metabolights.model import MetabolightsStudyModel

from mtbls.domain.domain_services.modifier.sample_modifier import SampleFileModifier


@pytest.fixture(scope="function")
def sample_modifier(
    templates: dict[str, Any],
    control_lists: dict[str, Any],
    metabolights_model: MetabolightsStudyModel,
) -> SampleFileModifier:
    sample_file_name = metabolights_model.investigation.studies[0].file_name
    isa_table_file = metabolights_model.samples[sample_file_name]
    modifier = SampleFileModifier(
        model=metabolights_model,
        isa_table_file=isa_table_file,
        templates=templates,
        control_lists=control_lists,
    )
    return modifier


class TestNoModification:
    @pytest.mark.asyncio
    async def test_modify_no_modification_01(self, sample_modifier: SampleFileModifier):
        sample_modifier.modify()
        assert len(sample_modifier.update_logs) == 0


class TestRemoveTrailingAndPrefixSpaces:
    @pytest.mark.asyncio
    async def test_modifier_remove_trailing_01(
        self, sample_modifier: SampleFileModifier
    ):
        metabolights_model = sample_modifier.model
        sample_file_name = metabolights_model.investigation.studies[0].file_name
        sample_file = metabolights_model.samples[sample_file_name]
        header_def = sample_file.table.headers[0]
        header = header_def.column_header
        header_column_name = header_def.column_name
        new_header = f" {header}"
        header_def.column_header = new_header
        header_def.column_name = header_def.column_name.replace(header, new_header, 1)
        sample_file.table.columns[header_def.column_index] = header_def.column_name
        sample_file.table.data[header_def.column_name] = sample_file.table.data[
            header_column_name
        ]
        del sample_file.table.data[header_column_name]

        sample_modifier.modify()
        assert len(sample_modifier.header_update_actions[sample_file_name]) == 1
        action = sample_modifier.header_update_actions[sample_file_name][0]
        assert len(action.current_headers) == 1
        assert len(action.new_headers) == 1
        assert len(sample_modifier.update_logs) == 1

    @pytest.mark.asyncio
    async def test_modifier_remove_trailing_02(
        self, sample_modifier: SampleFileModifier
    ):
        sample_modifier.max_row_number_limit = 3
        metabolights_model = sample_modifier.model
        sample_file_name = metabolights_model.investigation.studies[0].file_name
        sample_file = metabolights_model.samples[sample_file_name]
        values = sample_file.table.data[sample_file.table.columns[0]]
        expected_values = ", ".join([x for x in values])
        for idx, value in enumerate(values):
            values[idx] = f" {value}"

        sample_modifier.remove_trailing_and_prefix_spaces()
        actual = ", ".join([x for x in values])
        assert actual == expected_values
        assert len(sample_modifier.update_logs) == 1


class TestUpdateOntologyColumns:
    @pytest.mark.asyncio
    async def test_update_ontology_columns_01(
        self, sample_modifier: SampleFileModifier
    ):
        sample_modifier.max_row_number_limit = 3
        sample_modifier.update_ontology_columns()
        assert len(sample_modifier.update_logs) == 0

    @pytest.mark.asyncio
    async def test_update_ontology_columns_02(
        self, sample_modifier: SampleFileModifier
    ):
        sample_modifier.max_row_number_limit = 3
        sample_file_name = sample_modifier.model.investigation.studies[0].file_name
        sample_table = sample_modifier.model.samples[sample_file_name].table
        sample_table.data["Characteristics[Organism]"][0] = "homo sapiens"
        sample_modifier.update_ontology_columns()
        assert sample_table.data["Characteristics[Organism]"][0] == "Homo sapiens"
        assert len(sample_modifier.update_logs) == 1

    @pytest.mark.asyncio
    async def test_update_ontology_columns_03(
        self, sample_modifier: SampleFileModifier
    ):
        sample_modifier.max_row_number_limit = 3
        sample_file_name = sample_modifier.model.investigation.studies[0].file_name
        sample_table = sample_modifier.model.samples[sample_file_name].table
        sample_table.data["Characteristics[Organism]"][0] = "homo sapiens"
        sample_table.data["Term Source REF"][0] = ""
        sample_table.data["Term Accession Number"][0] = ""
        sample_modifier.update_ontology_columns()
        assert sample_table.data["Characteristics[Organism]"][0] == "Homo sapiens"
        assert sample_table.data["Term Source REF"][0] == "NCBITAXON"
        assert (
            sample_table.data["Term Accession Number"][0]
            == "http://purl.obolibrary.org/obo/NCBITaxon_9606"
        )
        assert len(sample_modifier.update_logs) == 1

    @pytest.mark.asyncio
    async def test_update_ontology_columns_04(
        self, sample_modifier: SampleFileModifier
    ):
        sample_modifier.max_row_number_limit = 3
        sample_file_name = sample_modifier.model.investigation.studies[0].file_name
        sample_table = sample_modifier.model.samples[sample_file_name].table
        sample_table.data["Characteristics[Organism]"][0] = ""
        sample_table.data["Term Source REF"][0] = ""
        sample_table.data["Term Accession Number"][0] = (
            "http://purl.obolibrary.org/obo/NCBITaxon_9606"
        )
        sample_modifier.update_ontology_columns()
        assert sample_table.data["Characteristics[Organism]"][0] == "Homo sapiens"
        assert sample_table.data["Term Source REF"][0] == "NCBITAXON"
        assert (
            sample_table.data["Term Accession Number"][0]
            == "http://purl.obolibrary.org/obo/NCBITaxon_9606"
        )
        assert len(sample_modifier.update_logs) == 1

    @pytest.mark.asyncio
    async def test_update_ontology_columns_07(
        self, sample_modifier: SampleFileModifier
    ):
        sample_modifier.max_row_number_limit = 3
        sample_file_name = sample_modifier.model.investigation.studies[0].file_name
        sample_table = sample_modifier.model.samples[sample_file_name].table
        sample_table.data["Characteristics[Organism]"][0] = ""
        sample_table.data["Term Source REF"][0] = "NCBITaxon"
        sample_table.data["Term Accession Number"][0] = ""
        sample_modifier.update_ontology_columns()
        assert sample_table.data["Characteristics[Organism]"][0] == ""
        assert sample_table.data["Term Source REF"][0] == ""
        assert sample_table.data["Term Accession Number"][0] == ""
        assert len(sample_modifier.update_logs) == 1


class TestUpdateSampleFactorValues:
    @pytest.mark.asyncio
    async def test_update_sample_factor_values_01(
        self, sample_modifier: SampleFileModifier
    ):
        sample_modifier.max_row_number_limit = 3
        sample_file_name = sample_modifier.model.investigation.studies[0].file_name
        sample_modifier.update_sample_factor_values()
        assert sample_file_name not in sample_modifier.header_update_actions
        assert len(sample_modifier.update_logs) == 0

    @pytest.mark.asyncio
    async def test_update_sample_factor_values_02(
        self, sample_modifier: SampleFileModifier
    ):
        sample_modifier.max_row_number_limit = 3
        sample_file_name = sample_modifier.model.investigation.studies[0].file_name
        sample_table = sample_modifier.model.samples[sample_file_name].table
        for header in sample_table.headers:
            if header.column_header == "Factor Value[Metabolic syndrome]":
                header.column_header = "Factor Value[metabolic Syndrome]"
                header.column_name = "Factor Value[metabolic Syndrome]"
                sample_table.columns[header.column_index] = (
                    "Factor Value[metabolic Syndrome]"
                )
                sample_table.data["Factor Value[metabolic Syndrome]"] = (
                    sample_table.data["Factor Value[Metabolic syndrome]"]
                )
                del sample_table.data["Factor Value[Metabolic syndrome]"]

        sample_modifier.update_sample_factor_values()
        assert sample_file_name in sample_modifier.header_update_actions
        assert sample_modifier.header_update_actions[sample_file_name]
        assert sample_modifier.header_update_actions[sample_file_name][0]
        assert len(sample_modifier.update_logs) == 1


class TestAddFactorValueColumns:
    @pytest.mark.asyncio
    async def test_add_factor_value_columns_01_no_error(
        self,
        sample_modifier: SampleFileModifier,
    ):
        sample_modifier.max_row_number_limit = 3
        sample_modifier.add_factor_value_columns()
        assert len(sample_modifier.new_header_actions) == 0
        assert len(sample_modifier.update_logs) == 0

    @pytest.mark.asyncio
    async def test_add_factor_value_columns_01_no_study(
        self,
        sample_modifier: SampleFileModifier,
    ):
        sample_modifier.max_row_number_limit = 3
        sample_modifier.model.investigation.studies = []
        sample_modifier.add_factor_value_columns()
        assert len(sample_modifier.new_header_actions) == 0
        assert len(sample_modifier.update_logs) == 0

    @pytest.mark.asyncio
    async def test_add_factor_value_columns_01(
        self, sample_modifier: SampleFileModifier
    ):
        sample_modifier.max_row_number_limit = 3
        sample_file_name = sample_modifier.model.investigation.studies[0].file_name
        study = sample_modifier.model.investigation.studies[0]
        factors = study.study_factors.factors
        factors.append(
            Factor(
                name="test data-Value",
                type=OntologyAnnotation(term="mm", term_source_ref="MTBLS"),
            )
        )

        sample_modifier.add_factor_value_columns()
        assert sample_modifier.new_header_actions
        assert sample_file_name in sample_modifier.new_header_actions
        expected_columns_count = 3
        expected_logs_count = 1
        assert (
            len(sample_modifier.new_header_actions[sample_file_name][0].columns)
            == expected_columns_count
        )
        assert len(sample_modifier.update_logs) == expected_logs_count

    @pytest.mark.asyncio
    async def test_add_factor_value_columns_02(
        self, sample_modifier: SampleFileModifier
    ):
        sample_modifier.max_row_number_limit = 3
        sample_file_name = sample_modifier.model.investigation.studies[0].file_name
        study = sample_modifier.model.investigation.studies[0]
        factors = study.study_factors.factors
        factors.extend(
            [
                Factor(
                    name="Test",
                    type=OntologyAnnotation(term="mm", term_source_ref="MTBLS"),
                ),
                Factor(
                    name="Test 2",
                    type=OntologyAnnotation(term="cm", term_source_ref="MTBLS"),
                ),
            ]
        )
        sample_modifier.add_factor_value_columns()
        assert sample_modifier.new_header_actions
        assert sample_file_name in sample_modifier.new_header_actions
        expected_logs_count = 2
        expected_columns_count = 6
        assert (
            len(sample_modifier.new_header_actions[sample_file_name][0].columns)
            == expected_columns_count
        )
        assert len(sample_modifier.update_logs) == expected_logs_count

    @pytest.mark.asyncio
    async def test_add_factor_value_columns_03(
        self, sample_modifier: SampleFileModifier
    ):
        sample_modifier.max_row_number_limit = 3
        sample_file_name = sample_modifier.model.investigation.studies[0].file_name
        study = sample_modifier.model.investigation.studies[0]
        factors = study.study_factors.factors
        factors.append(
            Factor(
                name="Test", type=OntologyAnnotation(term="mm", term_source_ref="UO")
            )
        )
        sample_modifier.add_factor_value_columns()
        assert sample_modifier.new_header_actions
        assert sample_file_name in sample_modifier.new_header_actions
        expected_logs_count = 1
        expected_columns_count = 4
        assert (
            len(sample_modifier.new_header_actions[sample_file_name][0].columns)
            == expected_columns_count
        )
        assert len(sample_modifier.update_logs) == expected_logs_count

    @pytest.mark.asyncio
    async def test_add_factor_value_columns_04(
        self, sample_modifier: SampleFileModifier
    ):
        sample_modifier.max_row_number_limit = 3
        sample_file_name = sample_modifier.model.investigation.studies[0].file_name
        study = sample_modifier.model.investigation.studies[0]
        factors = study.study_factors.factors
        factors.extend(
            [
                Factor(
                    name="Test",
                    type=OntologyAnnotation(term="mm", term_source_ref="MTBLS"),
                ),
                Factor(
                    name="Test 2",
                    type=OntologyAnnotation(term="cm", term_source_ref="UO"),
                ),
            ]
        )

        sample_modifier.add_factor_value_columns()
        assert sample_modifier.new_header_actions
        actions = sample_modifier.new_header_actions

        assert sample_file_name in actions
        expected_log_items = 2
        expected_column_count = 7
        assert len(actions[sample_file_name][0].columns) == expected_column_count
        assert len(sample_modifier.update_logs) == expected_log_items
