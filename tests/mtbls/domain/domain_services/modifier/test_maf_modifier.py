from typing import Any

import pytest
from metabolights_utils.models.metabolights.model import MetabolightsStudyModel

from mtbls.domain.domain_services.modifier.maf_modifier import MafFileModifier


@pytest.fixture(scope="function")
def maf_modifier(
    templates: dict[str, Any],
    control_lists: dict[str, Any],
    metabolights_model: MetabolightsStudyModel,
) -> MafFileModifier:
    maf_file_name = metabolights_model.referenced_assignment_files[0]
    isa_table_file = metabolights_model.metabolite_assignments[maf_file_name]
    modifier = MafFileModifier(
        model=metabolights_model,
        isa_table_file=isa_table_file,
        templates=templates,
        control_lists=control_lists,
    )
    return modifier


class TestNoModification:
    @pytest.mark.asyncio
    async def test_modify_no_modification_01(self, maf_modifier: MafFileModifier):
        update_logs = maf_modifier.modify()
        assert len(update_logs) == 0


class TestRemoveTrailingAndPrefixSpaces:
    @pytest.mark.asyncio
    async def test_modifier_remove_trailing_01(self, maf_modifier: MafFileModifier):
        metabolights_model = maf_modifier.model
        maf_file_name = metabolights_model.referenced_assignment_files[0]
        maf_file = metabolights_model.metabolite_assignments[maf_file_name]
        header_def = maf_file.table.headers[0]
        header = header_def.column_header
        header_column_name = header_def.column_name
        new_header = f" {header}"
        header_def.column_header = new_header
        header_def.column_name = header_def.column_name.replace(header, new_header, 1)
        maf_file.table.columns[header_def.column_index] = header_def.column_name
        maf_file.table.data[header_def.column_name] = maf_file.table.data[
            header_column_name
        ]
        del maf_file.table.data[header_column_name]

        maf_modifier.remove_trailing_and_prefix_spaces()
        assert len(maf_modifier.header_update_actions[maf_file_name]) == 1
        action = maf_modifier.header_update_actions[maf_file_name][0]
        assert len(action.current_headers) == 1
        assert len(action.new_headers) == 1
        assert len(maf_modifier.update_logs) == 1

    @pytest.mark.asyncio
    async def test_modifier_remove_trailing_02(self, maf_modifier: MafFileModifier):
        maf_modifier.max_row_number_limit = 3
        metabolights_model = maf_modifier.model
        maf_file_name = metabolights_model.referenced_assignment_files[0]
        maf_file = metabolights_model.metabolite_assignments[maf_file_name]
        values = maf_file.table.data[maf_file.table.columns[0]]
        expected_values = ", ".join([x for x in values])
        for idx, value in enumerate(values):
            values[idx] = f" {value}"
        maf_modifier.remove_trailing_and_prefix_spaces()
        actual = ", ".join([x for x in values])
        assert actual == expected_values
        assert len(maf_modifier.update_logs) == 1


class TestAddMafSampleColumns:
    @pytest.mark.asyncio
    async def test_add_maf_sample_columns_01(self, maf_modifier: MafFileModifier):
        maf_modifier.max_row_number_limit = 3
        metabolights_model = maf_modifier.model
        maf_file_name = metabolights_model.referenced_assignment_files[0]

        maf_modifier.add_maf_sample_columns()
        assert maf_file_name not in maf_modifier.new_header_actions
        assert len(maf_modifier.new_header_actions) == 0
        assert len(maf_modifier.update_logs) == 0

    @pytest.mark.asyncio
    async def test_add_maf_sample_columns_02(self, maf_modifier: MafFileModifier):
        maf_modifier.max_row_number_limit = 3
        metabolights_model = maf_modifier.model
        maf_file_name = metabolights_model.referenced_assignment_files[0]
        maf_file = metabolights_model.metabolite_assignments[maf_file_name]
        header_def = maf_file.table.headers[-1]
        header_column_name = header_def.column_name
        maf_file.table.headers.remove(header_def)
        maf_file.table.columns.remove(header_column_name)
        del maf_file.table.data[header_column_name]
        maf_modifier.add_maf_sample_columns()
        assert len(maf_modifier.new_header_actions[maf_file_name]) == 1

        assert len(maf_modifier.update_logs) == 1

    @pytest.mark.asyncio
    async def test_add_maf_sample_columns_03(self, maf_modifier: MafFileModifier):
        maf_modifier.max_row_number_limit = 3
        metabolights_model = maf_modifier.model
        maf_file_name = metabolights_model.referenced_assignment_files[0]
        maf_file = metabolights_model.metabolite_assignments[maf_file_name]
        header_def = maf_file.table.headers[-1]
        header_column_name = header_def.column_name
        maf_file.table.headers.remove(header_def)
        maf_file.table.columns.remove(header_column_name)
        del maf_file.table.data[header_column_name]
        assay = metabolights_model.investigation.studies[0].study_assays.assays[0]
        assay_name = assay.file_name
        sample_value = metabolights_model.assays[assay_name].table.data["Sample Name"][
            0
        ]
        metabolights_model.assays[assay_name].table.data["Sample Name"][0] = ""
        metabolights_model.assays[assay_name].sample_names.remove(sample_value)
        maf_modifier.add_maf_sample_columns()
        assert maf_file_name not in maf_modifier.new_header_actions
        assert len(maf_modifier.new_header_actions) == 0
        assert len(maf_modifier.update_logs) == 0

    @pytest.mark.asyncio
    async def test_add_maf_sample_columns_04(self, maf_modifier: MafFileModifier):
        maf_modifier.model.investigation.studies.clear()
        maf_modifier.add_maf_sample_columns()
        assert len(maf_modifier.new_header_actions) == 0
        assert len(maf_modifier.update_logs) == 0

    @pytest.mark.asyncio
    async def test_add_maf_sample_columns_05(self, maf_modifier: MafFileModifier):
        maf_modifier.model.investigation.studies[0].file_name = "test"
        maf_modifier.add_maf_sample_columns()
        assert len(maf_modifier.new_header_actions) == 0
        assert len(maf_modifier.update_logs) == 0

    @pytest.mark.asyncio
    async def test_add_maf_sample_columns_06(self, maf_modifier: MafFileModifier):
        sample_file = maf_modifier.model.investigation.studies[0].file_name
        table = maf_modifier.model.samples[sample_file].table
        del table.data["Sample Name"]
        table.columns[0] = "Sample Name-Test"
        table.headers[0].column_header = "Sample Name-Test"
        table.headers[0].column_name = "Sample Name-Test"

        maf_modifier.add_maf_sample_columns()
        assert len(maf_modifier.new_header_actions) == 0
        assert len(maf_modifier.update_logs) == 0

    @pytest.mark.asyncio
    async def test_add_maf_sample_columns_07(self, maf_modifier: MafFileModifier):
        sample_file = maf_modifier.model.investigation.studies[0].file_name
        sample_table_file = maf_modifier.model.samples[sample_file]
        sample_table_file.table.data["Sample Name"][0] = ""
        sample_table_file.sample_names.remove(sample_table_file.sample_names[0])

        maf_modifier.add_maf_sample_columns()
        assert len(maf_modifier.new_header_actions) == 0
        assert len(maf_modifier.update_logs) == 0

    @pytest.mark.asyncio
    async def test_add_maf_sample_columns_08(self, maf_modifier: MafFileModifier):
        metabolights_model = maf_modifier.model
        metabolights_model.metabolite_assignments.clear()
        metabolights_model.referenced_assignment_files.clear()

        maf_modifier.add_maf_sample_columns()
        assert len(maf_modifier.new_header_actions) == 0
        assert len(maf_modifier.update_logs) == 0

    @pytest.mark.asyncio
    async def test_add_maf_sample_columns_09(self, maf_modifier: MafFileModifier):
        assay_file = (
            maf_modifier.model.investigation.studies[0].study_assays.assays[0].file_name
        )
        table = maf_modifier.model.assays[assay_file].table
        del table.data["Sample Name"]
        table.columns[0] = "Sample Name-Test"
        table.headers[0].column_header = "Sample Name-Test"
        table.headers[0].column_name = "Sample Name-Test"

        maf_modifier.add_maf_sample_columns()
        assert len(maf_modifier.new_header_actions) == 0
        assert len(maf_modifier.update_logs) == 0
