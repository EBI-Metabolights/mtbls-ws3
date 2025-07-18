from typing import Any

import pytest
from metabolights_utils.models.metabolights.model import MetabolightsStudyModel

from mtbls.domain.domain_services.modifier.assay_modifier import AssayFileModifier


@pytest.fixture(scope="function")
def modifier(
    templates: dict[str, Any],
    control_lists: dict[str, Any],
    ms_metabolights_model: MetabolightsStudyModel,
) -> AssayFileModifier:
    assays = ms_metabolights_model.investigation.studies[0].study_assays.assays
    assay_file_name = assays[0].file_name
    isa_table_file = ms_metabolights_model.assays[assay_file_name]
    modifier = AssayFileModifier(
        model=ms_metabolights_model,
        isa_table_file=isa_table_file,
        templates=templates,
        control_lists=control_lists,
    )
    return modifier


all_technique_names = [
    "LC-MS",
    "GC-MS",
    "NMR",
]


class TestUpdateSingleColumn:
    @pytest.mark.asyncio
    async def test_update_single_column_01(self, modifier: AssayFileModifier):
        technique = "LC-MS"
        category = "assayColumns"
        file_name = modifier.isa_table_file.file_path
        table_file = modifier.model.assays[file_name]
        column_header = "Parameter Value[Scan polarity]"
        header = None
        for item in table_file.table.headers:
            if item.column_header == column_header:
                header = item
                break
        table_file.table.data[header.column_name][0] = "POSITIVE"
        table_file.table.data[header.column_name][1] = "NEGATIVE"
        modifier.update_single_column(
            technique=technique, category=category, header=header
        )
        assert table_file.table.data[header.column_name][0] == "positive"
        assert table_file.table.data[header.column_name][1] == "negative"
        assert len(modifier.update_logs) == 1


class TestUpdateOntologyColumns:
    @pytest.mark.asyncio
    async def test_update_ontology_columns_01(self, modifier: AssayFileModifier):
        file_name = modifier.isa_table_file.file_path
        table_file = modifier.model.assays[file_name]
        column_header = "Parameter Value[Chromatography Instrument]"
        header = None
        for item in table_file.table.headers:
            if item.column_header == column_header:
                header = item
                break
        name = table_file.table.columns[header.column_index + 2]
        value = table_file.table.data[header.column_name][0]
        table_file.table.data[header.column_name][0] = value.upper()
        table_file.table.data[name][0] = "Invalid accession"
        expected = "http://www.ebi.ac.uk/metabolights/ontology/MTBLS_000877"
        modifier.update_ontology_columns()
        assert table_file.table.data[header.column_name][0] == value

        assert table_file.table.data[name][0] == expected

        assert len(modifier.update_logs) == 1
