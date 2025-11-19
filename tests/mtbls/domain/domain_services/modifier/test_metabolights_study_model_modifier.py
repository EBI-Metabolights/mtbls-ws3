from typing import Any

import pytest
from metabolights_utils.models.metabolights.model import MetabolightsStudyModel
from metabolights_utils.tsv.model import (
    TsvAddColumnsAction,
    TsvColumnData,
    TsvUpdateColumnHeaderAction,
)

from mtbls.domain.domain_services.modifier.metabolights_study_model_modifier import (
    MetabolightsStudyModelModifier,
)


@pytest.fixture(scope="function")
def modifier(
    templates: dict[str, Any],
    control_lists: dict[str, Any],
    metabolights_model: MetabolightsStudyModel,
) -> MetabolightsStudyModelModifier:
    modifier = MetabolightsStudyModelModifier(
        model=metabolights_model,
        templates=templates,
        control_lists=control_lists,
    )
    return modifier


@pytest.fixture(scope="function")
def ms_modifier(
    templates: dict[str, Any],
    control_lists: dict[str, Any],
    ms_metabolights_model: MetabolightsStudyModel,
) -> MetabolightsStudyModelModifier:
    modifier = MetabolightsStudyModelModifier(
        model=ms_metabolights_model,
        templates=templates,
        control_lists=control_lists,
    )
    return modifier


@pytest.fixture(scope="function")
def mtbls1_base_modifier(
    templates: dict[str, Any],
    control_lists: dict[str, Any],
    metabolights_model_base_MTBLS1: MetabolightsStudyModel,
) -> MetabolightsStudyModelModifier:
    modifier = MetabolightsStudyModelModifier(
        model=metabolights_model_base_MTBLS1,
        templates=templates,
        control_lists=control_lists,
    )
    return modifier


@pytest.fixture(scope="function")
def ms_base_01_modifier(
    templates: dict[str, Any],
    control_lists: dict[str, Any],
    ms_metabolights_model_base_01: MetabolightsStudyModel,
) -> MetabolightsStudyModelModifier:
    modifier = MetabolightsStudyModelModifier(
        model=ms_metabolights_model_base_01,
        templates=templates,
        control_lists=control_lists,
    )
    return modifier


class TestNoModification:
    @pytest.mark.asyncio
    async def test_modify_no_modification_01(
        self, modifier: MetabolightsStudyModelModifier
    ):
        update_logs = modifier.modify()

        # from pathlib import Path

        # with Path("model_MTBLS1.json").open("w") as f:
        #     f.write(modifier.model.model_dump_json(by_alias=True, indent=4))
        assert len(update_logs) == 0

    @pytest.mark.asyncio
    async def test_modify_no_modification_02(
        self, ms_modifier: MetabolightsStudyModelModifier
    ):
        ms_modifier.modify()
        # from pathlib import Path

        # with Path("model_MTBLS5195.json").open("w") as f:
        #     f.write(ms_modifier.model.model_dump_json(by_alias=True, indent=4))
        assert len(ms_modifier.update_logs) == 0


class TestUpdateColumnHeaders:
    @pytest.mark.asyncio
    async def test_update_column_headers_01(
        self, ms_modifier: MetabolightsStudyModelModifier
    ):
        assay = ms_modifier.model.investigation.studies[0].study_assays.assays[0]
        assay_name = assay.file_name
        assay_table = ms_modifier.model.assays[assay_name].table
        current_column_header_1 = assay_table.headers[0]
        current_column_header_2 = assay_table.headers[-1]
        header_index_1 = current_column_header_1.column_index
        header_index_2 = current_column_header_2.column_index

        new_header_1 = current_column_header_1.column_header + " test"
        new_header_2 = "Protocol REF"
        ms_modifier.header_update_actions[assay_name] = []
        actions = ms_modifier.header_update_actions[assay_name]
        actions.append(TsvUpdateColumnHeaderAction())
        action = actions[0]
        action.current_headers[header_index_1] = current_column_header_1.column_header
        action.current_headers[header_index_2] = current_column_header_2.column_header
        action.new_headers[header_index_1] = new_header_1
        action.new_headers[header_index_2] = new_header_2

        ms_modifier.update_column_headers(ms_modifier.model.assays)

        assert current_column_header_1.column_header == new_header_1
        assert new_header_1 in current_column_header_1.column_name
        assert current_column_header_2.column_header == new_header_2
        assert new_header_2 in current_column_header_2.column_name
        assert len(ms_modifier.update_logs) == 0

    @pytest.mark.asyncio
    async def test_update_column_headers_02(
        self, ms_modifier: MetabolightsStudyModelModifier
    ):
        assay_name = "s_MTBLS1.txt"

        ms_modifier.header_update_actions[assay_name] = []

        ms_modifier.update_column_headers(ms_modifier.model.assays)

        assert assay_name not in ms_modifier.header_update_actions
        assert len(ms_modifier.update_logs) == 0

    @pytest.mark.asyncio
    async def test_update_column_headers_03(
        self,
        ms_modifier: MetabolightsStudyModelModifier,
    ):
        assay = ms_modifier.model.investigation.studies[0].study_assays.assays[0]
        assay_name = assay.file_name
        ms_modifier.header_update_actions[assay_name] = []

        ms_modifier.update_column_headers(ms_modifier.model.assays)

        assert assay_name not in ms_modifier.header_update_actions
        assert len(ms_modifier.update_logs) == 0

    @pytest.mark.asyncio
    async def test_update_column_headers_04(
        self, ms_modifier: MetabolightsStudyModelModifier
    ):
        assay = ms_modifier.model.investigation.studies[0].study_assays.assays[0]
        assay_name = assay.file_name
        ms_modifier.header_update_actions[assay_name] = []
        actions = ms_modifier.header_update_actions[assay_name]
        actions.append(TsvUpdateColumnHeaderAction())
        ms_modifier.update_column_headers(ms_modifier.model.assays)

        assert assay_name not in ms_modifier.header_update_actions
        assert len(ms_modifier.update_logs) == 0

    @pytest.mark.asyncio
    async def test_update_column_headers_05(
        self, ms_modifier: MetabolightsStudyModelModifier
    ):
        assay = ms_modifier.model.investigation.studies[0].study_assays.assays[0]
        assay_name = assay.file_name
        ms_modifier.header_update_actions[assay_name] = []
        actions = ms_modifier.header_update_actions[assay_name]
        actions.append(TsvAddColumnsAction())
        ms_modifier.update_column_headers(ms_modifier.model.assays)

        assert assay_name not in ms_modifier.header_update_actions
        assert len(ms_modifier.update_logs) == 0


class TestAddColumnHeaders:
    @pytest.mark.asyncio
    async def test_add_column_headers_01(
        self, ms_modifier: MetabolightsStudyModelModifier
    ):
        assay = ms_modifier.model.investigation.studies[0].study_assays.assays[0]
        assay_name = assay.file_name
        assay_table = ms_modifier.model.assays[assay_name].table
        columns_count = len(assay_table.columns)
        new_header_1 = "Sample Name"
        new_header_2 = "Protocol REF"
        ms_modifier.new_header_actions[assay_name] = []
        actions = ms_modifier.new_header_actions[assay_name]
        actions.append(TsvAddColumnsAction())
        action = actions[0]
        action.columns[0] = TsvColumnData(header_name=new_header_1)
        action.columns[columns_count] = TsvColumnData(header_name=new_header_2)
        action.cell_default_values[columns_count] = "Sample Protocol"

        ms_modifier.add_new_columns(ms_modifier.model.assays)

        assert "Sample Name.1" in assay_table.columns[0]
        assert "Protocol REF" in assay_table.columns[-1]
        assert len(assay_table.columns) == columns_count + 2
        assert len(ms_modifier.update_logs) == 0

    @pytest.mark.asyncio
    async def test_add_column_headers_02(
        self, ms_modifier: MetabolightsStudyModelModifier
    ):
        assay = ms_modifier.model.investigation.studies[0].study_assays.assays[0]
        assay_name = assay.file_name
        assay_table = ms_modifier.model.assays[assay_name].table
        columns_count = len(assay_table.columns)
        new_header_1 = "Factor Value[Test]"
        new_header_2 = "Protocol REF"
        ms_modifier.new_header_actions[assay_name] = []
        actions = ms_modifier.new_header_actions[assay_name]
        actions.append(TsvAddColumnsAction())
        action = actions[0]
        action.columns[0] = TsvColumnData(header_name=new_header_1)
        action.columns[1] = TsvColumnData(header_name="Unit")
        action.columns[2] = TsvColumnData(header_name="Term Source REF")
        action.columns[3] = TsvColumnData(header_name="Term Accession Number")
        action.columns[4] = TsvColumnData(header_name="Characteristics[Test data]")
        action.columns[5] = TsvColumnData(header_name="Term Source REF")
        action.columns[6] = TsvColumnData(header_name="Term Accession Number")
        action.columns[columns_count + 7] = TsvColumnData(header_name=new_header_2)
        action.columns[columns_count + 8] = TsvColumnData(header_name="Term Source REF")
        action.columns[columns_count + 9] = TsvColumnData(
            header_name="Term Accession Number"
        )
        action.columns[columns_count + 10] = TsvColumnData(header_name="Comment[Test]")
        action.cell_default_values[columns_count + 6] = "Sample Protocol"

        ms_modifier.add_new_columns(ms_modifier.model.assays)

        assert new_header_1 in assay_table.columns[0]
        assert "Unit" in assay_table.columns[1]
        assert "Protocol REF" in assay_table.columns[columns_count + 7]
        assert "Term Source REF" in assay_table.columns[columns_count + 8]
        assert len(assay_table.columns) == columns_count + 11
        assert len(ms_modifier.update_logs) == 0

    @pytest.mark.asyncio
    async def test_add_column_headers_03(
        self, ms_modifier: MetabolightsStudyModelModifier
    ):
        assay = ms_modifier.model.investigation.studies[0].study_assays.assays[0]
        assay_name = assay.file_name

        ms_modifier.new_header_actions[assay_name] = []

        ms_modifier.add_new_columns(ms_modifier.model.assays)

        assert assay_name not in ms_modifier.new_header_actions
        assert len(ms_modifier.update_logs) == 0

    @pytest.mark.asyncio
    async def test_add_column_headers_04(
        self, ms_modifier: MetabolightsStudyModelModifier
    ):
        assay = ms_modifier.model.investigation.studies[0].study_assays.assays[0]
        assay_name = assay.file_name

        ms_modifier.new_header_actions[assay_name] = []
        actions = ms_modifier.new_header_actions[assay_name]
        actions.append(TsvAddColumnsAction())

        ms_modifier.add_new_columns(ms_modifier.model.assays)

        assert assay_name not in ms_modifier.new_header_actions
        assert len(ms_modifier.update_logs) == 0

    @pytest.mark.asyncio
    async def test_add_column_headers_05(
        self, ms_modifier: MetabolightsStudyModelModifier
    ):
        assay_name = "s_MTBLS1.txt"

        ms_modifier.new_header_actions[assay_name] = []

        ms_modifier.add_new_columns(ms_modifier.model.assays)

        assert assay_name in ms_modifier.new_header_actions
        assert len(ms_modifier.update_logs) == 0

    @pytest.mark.asyncio
    async def test_add_column_headers_06(
        self, ms_modifier: MetabolightsStudyModelModifier
    ):
        assay = ms_modifier.model.investigation.studies[0].study_assays.assays[0]
        assay_name = assay.file_name

        ms_modifier.new_header_actions[assay_name] = []
        actions = ms_modifier.new_header_actions[assay_name]
        actions.append(TsvUpdateColumnHeaderAction())

        ms_modifier.add_new_columns(ms_modifier.model.assays)

        assert assay_name not in ms_modifier.new_header_actions
        assert len(ms_modifier.update_logs) == 0


class TestModify:
    @pytest.mark.asyncio
    async def test_modify_01(self, ms_base_01_modifier: MetabolightsStudyModelModifier):
        ms_base_01_modifier.modify()

        assert len(ms_base_01_modifier.update_logs) > 1
        assert len(ms_base_01_modifier.new_header_actions) == 1
