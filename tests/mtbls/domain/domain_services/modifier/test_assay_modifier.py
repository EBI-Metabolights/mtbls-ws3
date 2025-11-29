from typing import Any

import pytest
from metabolights_utils.models.metabolights.model import MetabolightsStudyModel

from mtbls.domain.domain_services.modifier.assay_modifier import AssayFileModifier


@pytest.fixture(scope="function")
def assay_modifier(
    templates: dict[str, Any],
    control_lists: dict[str, Any],
    metabolights_model: MetabolightsStudyModel,
) -> AssayFileModifier:
    assay_file_name = (
        metabolights_model.investigation.studies[0].study_assays.assays[0].file_name
    )
    isa_table_file = metabolights_model.assays[assay_file_name]
    modifier = AssayFileModifier(
        model=metabolights_model,
        isa_table_file=isa_table_file,
        templates=templates,
        control_lists=control_lists,
    )
    return modifier


@pytest.fixture(scope="function")
def ms_assay_modifier(
    templates: dict[str, Any],
    control_lists: dict[str, Any],
    ms_metabolights_model: MetabolightsStudyModel,
) -> AssayFileModifier:
    assay_file_name = (
        ms_metabolights_model.investigation.studies[0].study_assays.assays[0].file_name
    )
    isa_table_file = ms_metabolights_model.assays[assay_file_name]
    modifier = AssayFileModifier(
        model=ms_metabolights_model,
        isa_table_file=isa_table_file,
        templates=templates,
        control_lists=control_lists,
    )
    return modifier


class TestNoModification:
    @pytest.mark.asyncio
    async def test_modify_no_modification_01(self, assay_modifier: AssayFileModifier):
        update_logs = assay_modifier.modify()
        assert len(update_logs) == 0


class TestRemoveTrailingAndPrefixSpaces:
    @pytest.mark.asyncio
    async def test_modifier_remove_trailing_01(self, assay_modifier: AssayFileModifier):
        metabolights_model = assay_modifier.model
        assay_file_name = (
            metabolights_model.investigation.studies[0].study_assays.assays[0].file_name
        )
        assay_file = metabolights_model.assays[assay_file_name]
        header_def = assay_file.table.headers[0]
        header = header_def.column_header
        header_column_name = header_def.column_name
        new_header = f" {header}"
        header_def.column_header = new_header
        header_def.column_name = header_def.column_name.replace(header, new_header, 1)
        assay_file.table.columns[header_def.column_index] = header_def.column_name
        assay_file.table.data[header_def.column_name] = assay_file.table.data[
            header_column_name
        ]
        del assay_file.table.data[header_column_name]

        update_logs = assay_modifier.remove_trailing_and_prefix_spaces()
        assert len(assay_modifier.header_update_actions[assay_file_name]) == 1
        action = assay_modifier.header_update_actions[assay_file_name][0]
        assert len(action.current_headers) == 1
        assert len(action.new_headers) == 1
        assert len(update_logs) == 1

    @pytest.mark.asyncio
    async def test_modifier_remove_trailing_02(self, assay_modifier: AssayFileModifier):
        assay_modifier.max_row_number_limit = 3
        metabolights_model = assay_modifier.model
        assay_file_name = (
            metabolights_model.investigation.studies[0].study_assays.assays[0].file_name
        )
        assay_file = metabolights_model.assays[assay_file_name]
        values = assay_file.table.data[assay_file.table.columns[0]]
        expected_values = ", ".join([x for x in values])
        for idx, value in enumerate(values):
            values[idx] = f" {value}"

        update_logs = assay_modifier.remove_trailing_and_prefix_spaces()
        actual = ", ".join([x for x in values])
        assert actual == expected_values
        assert len(update_logs) == 1


class TestUpdateUpadateOntologyColumn:
    @pytest.mark.asyncio
    async def test_update_ontology_columns_01(self, assay_modifier: AssayFileModifier):
        assay_modifier.max_row_number_limit = 3
        assay_modifier.update_ontology_columns()
        assert len(assay_modifier.update_logs) == 0

    @pytest.mark.asyncio
    async def test_update_ontology_columns_02(self, assay_modifier: AssayFileModifier):
        assay_modifier.max_row_number_limit = 3
        metabolights_model = assay_modifier.model
        assay_file_name = (
            metabolights_model.investigation.studies[0].study_assays.assays[0].file_name
        )
        assay_table = assay_modifier.model.assays[assay_file_name].table
        assay_table.data["Parameter Value[Instrument]"][0] = (
            "Bruker AVANCE DRX 700 MHz spectrometer"
        )
        assay_table.data["Term Source REF.4"][0] = ""
        assay_table.data["Term Accession Number.4"][0] = ""
        assay_modifier.update_ontology_columns()
        assay_table.data["Parameter Value[Instrument]"][0] = (
            "Bruker AVANCE DRX 700 MHz spectrometer"
        )
        assert assay_table.data["Term Source REF.4"][0] == "MTBLS"
        assert (
            assay_table.data["Term Accession Number.4"][0]
            == "http://www.ebi.ac.uk/metabolights/ontology/MTBLS_000410"
        )
        assert len(assay_modifier.update_logs) == 1

    @pytest.mark.asyncio
    async def test_update_ontology_columns_03(self, assay_modifier: AssayFileModifier):
        assay_modifier.max_row_number_limit = 3
        metabolights_model = assay_modifier.model
        assay_file_name = (
            metabolights_model.investigation.studies[0].study_assays.assays[0].file_name
        )
        assay_table = assay_modifier.model.assays[assay_file_name].table
        assay_table.data["Parameter Value[Instrument]"][0] = ""
        assay_table.data["Term Source REF.4"][0] = ""
        assay_table.data["Term Accession Number.4"][0] = (
            "http://www.ebi.ac.uk/metabolights/ontology/MTBLS_000410"
        )
        assay_modifier.update_ontology_columns()
        assert (
            assay_table.data["Parameter Value[Instrument]"][0]
            == "Bruker AVANCE DRX 700 MHz spectrometer"
        )
        assert assay_table.data["Term Source REF.4"][0] == "MTBLS"
        assert (
            assay_table.data["Term Accession Number.4"][0]
            == "http://www.ebi.ac.uk/metabolights/ontology/MTBLS_000410"
        )
        assert len(assay_modifier.update_logs) == 1

    @pytest.mark.asyncio
    async def test_update_ontology_columns_04(self, assay_modifier: AssayFileModifier):
        assay_modifier.max_row_number_limit = 3
        metabolights_model = assay_modifier.model
        assay_file_name = (
            metabolights_model.investigation.studies[0].study_assays.assays[0].file_name
        )
        assay_table = assay_modifier.model.assays[assay_file_name].table
        assay_table.data["Parameter Value[Instrument]"][0] = (
            "Bruker AVANCE DRX 700 MHz spectrometer"
        )
        assay_table.data["Term Source REF.4"][0] = ""
        assay_table.data["Term Accession Number.4"][0] = (
            "http://www.ebi.ac.uk/metabolights/ontology/MTBLS_000410"
        )
        assay_modifier.update_ontology_columns()
        assert (
            assay_table.data["Parameter Value[Instrument]"][0]
            == "Bruker AVANCE DRX 700 MHz spectrometer"
        )
        assert assay_table.data["Term Source REF.4"][0] == "MTBLS"
        assert (
            assay_table.data["Term Accession Number.4"][0]
            == "http://www.ebi.ac.uk/metabolights/ontology/MTBLS_000410"
        )
        assay_modifier.update_ontology_columns()
        assert (
            assay_table.data["Parameter Value[Instrument]"][0]
            == "Bruker AVANCE DRX 700 MHz spectrometer"
        )
        assert assay_table.data["Term Source REF.4"][0] == "MTBLS"
        assert (
            assay_table.data["Term Accession Number.4"][0]
            == "http://www.ebi.ac.uk/metabolights/ontology/MTBLS_000410"
        )
        assert len(assay_modifier.update_logs) == 1

    @pytest.mark.asyncio
    async def test_update_ontology_columns_05(self, assay_modifier: AssayFileModifier):
        assay_modifier.max_row_number_limit = 3
        metabolights_model = assay_modifier.model
        assay_file_name = (
            metabolights_model.investigation.studies[0].study_assays.assays[0].file_name
        )
        assay_table = assay_modifier.model.assays[assay_file_name].table
        assay_table.data["Parameter Value[Instrument]"][0] = (
            "Bruker AVANCE DRX 700 MHz spectrometer"
        )
        assay_table.data["Term Source REF.4"][0] = "MTBLS"
        assay_table.data["Term Accession Number.4"][0] = ""
        assay_modifier.update_ontology_columns()
        assert (
            assay_table.data["Parameter Value[Instrument]"][0]
            == "Bruker AVANCE DRX 700 MHz spectrometer"
        )
        assert assay_table.data["Term Source REF.4"][0] == "MTBLS"
        assert (
            assay_table.data["Term Accession Number.4"][0]
            == "http://www.ebi.ac.uk/metabolights/ontology/MTBLS_000410"
        )
        assay_modifier.update_ontology_columns()
        assert (
            assay_table.data["Parameter Value[Instrument]"][0]
            == "Bruker AVANCE DRX 700 MHz spectrometer"
        )
        assert assay_table.data["Term Source REF.4"][0] == "MTBLS"
        assert (
            assay_table.data["Term Accession Number.4"][0]
            == "http://www.ebi.ac.uk/metabolights/ontology/MTBLS_000410"
        )
        assert len(assay_modifier.update_logs) == 1

    @pytest.mark.asyncio
    async def test_update_ontology_columns_06(self, assay_modifier: AssayFileModifier):
        assay_modifier.max_row_number_limit = 3
        metabolights_model = assay_modifier.model
        assay_file_name = (
            metabolights_model.investigation.studies[0].study_assays.assays[0].file_name
        )
        assay_table = assay_modifier.model.assays[assay_file_name].table
        assay_table.data["Unit"][0] = ""
        assay_table.data["Term Source REF.2"][0] = "UO"
        assay_table.data["Term Accession Number.2"][0] = ""
        assay_modifier.update_ontology_columns()
        assert assay_table.data["Unit"][0] == ""
        assert assay_table.data["Term Source REF.2"][0] == ""
        assert assay_table.data["Term Accession Number.2"][0] == ""
        assert len(assay_modifier.update_logs) == 1

    @pytest.mark.asyncio
    async def test_update_ontology_columns_07(self, assay_modifier: AssayFileModifier):
        assay_modifier.max_row_number_limit = 3
        metabolights_model = assay_modifier.model
        study = metabolights_model.investigation.studies[0]
        assay_file_name = study.study_assays.assays[0].file_name
        assay_table = assay_modifier.model.assays[assay_file_name].table
        assay_table.data["Parameter Value[Instrument]"][0] = ""
        assay_table.data["Term Source REF.4"][0] = "uo"
        assay_table.data["Term Accession Number.4"][0] = ""
        assay_modifier.update_ontology_columns()
        assert assay_table.data["Parameter Value[Instrument]"][0] == ""
        assert assay_table.data["Term Source REF.4"][0] == ""
        assert assay_table.data["Term Accession Number.4"][0] == ""
        assert len(assay_modifier.update_logs) == 1


class Test_rule_f_400_090_001_02:
    @pytest.mark.asyncio
    async def test_rule_f_400_090_001_02_01(self, assay_modifier: AssayFileModifier):
        assay_modifier.max_row_number_limit = 3

        assay_modifier.rule_f_400_090_001_02()
        assert len(assay_modifier.update_logs) == 0

    @pytest.mark.asyncio
    async def test_rule_f_400_090_001_02_02(self, assay_modifier: AssayFileModifier):
        assay_modifier.max_row_number_limit = 3
        metabolights_model = assay_modifier.model
        study = metabolights_model.investigation.studies[0]
        assay_file_name = study.study_assays.assays[0].file_name
        assay_table = assay_modifier.model.assays[assay_file_name].table
        data_file_column = "Free Induction Decay Data File"

        assay_table.data[data_file_column][0] = "ADG10003u_007.zip"
        assay_table.data[data_file_column][1] = "/ADG10003u_007.zip"
        assay_table.data[data_file_column][2] = "ADG10003u_007.zip/"
        assay_table.data[data_file_column][3] = "/ADG10003u_007.zip/"

        assay_modifier.rule_f_400_090_001_02()
        assert assay_table.data[data_file_column][0] == "FILES/ADG10003u_007.zip"
        assert assay_table.data[data_file_column][1] == "FILES/ADG10003u_007.zip"
        assert assay_table.data[data_file_column][2] == "FILES/ADG10003u_007.zip"
        assert assay_table.data[data_file_column][3] == "FILES/ADG10003u_007.zip"
        assert len(assay_modifier.update_logs) == 1

    @pytest.mark.asyncio
    async def test_rule_f_400_090_001_02_03(self, assay_modifier: AssayFileModifier):
        assay_modifier.max_row_number_limit = 3
        metabolights_model = assay_modifier.model
        study = metabolights_model.investigation.studies[0]
        assay_file_name = study.study_assays.assays[0].file_name
        assay_table = assay_modifier.model.assays[assay_file_name].table
        metabolights_model.study_folder_metadata.files["FILES/ADG10003u_007.zip"] = {}
        metabolights_model.study_folder_metadata.files["FILES/ADG10003u_008.zip"] = {}
        metabolights_model.study_folder_metadata.files["FILES/ADG10003u_009.zip"] = {}
        metabolights_model.study_folder_metadata.files["FILES/ADG10003u_010.zip"] = {}
        metabolights_model.study_folder_metadata.files["FILES/ADG10003u_011.zip"] = {}
        metabolights_model.study_folder_metadata.files["FILES/ADG10003u_012.zip"] = {}
        data_file_column = "Free Induction Decay Data File"
        assay_table.data[data_file_column][0] = "adg10003u_007.ZIP"
        assay_table.data[data_file_column][1] = "adg10003u_008.ZIP"
        assay_table.data[data_file_column][2] = "/adg10003u_009.ZIP/"
        assay_table.data[data_file_column][3] = "/adg10003u_010.ZIP"
        assay_table.data[data_file_column][4] = "ADG10003u_011.ZIP/"
        assay_table.data[data_file_column][5] = "FILES/ADG10003u_012.ZIP"

        assay_modifier.rule_f_400_090_001_02()
        assert assay_table.data[data_file_column][0] == "FILES/ADG10003u_007.zip"
        assert assay_table.data[data_file_column][1] == "FILES/ADG10003u_008.zip"
        assert assay_table.data[data_file_column][2] == "FILES/ADG10003u_009.zip"
        assert assay_table.data[data_file_column][3] == "FILES/ADG10003u_010.zip"
        assert assay_table.data[data_file_column][4] == "FILES/ADG10003u_011.zip"
        assert assay_table.data[data_file_column][5] == "FILES/ADG10003u_012.zip"
        assert len(assay_modifier.update_logs) == 1


class Test_rule_a_200_090_004_01:
    @pytest.mark.asyncio
    async def test_rule_a_200_090_004_01_01(self, ms_assay_modifier: AssayFileModifier):
        ms_assay_modifier.rule_a_200_090_004_01()
        assert len(ms_assay_modifier.update_logs) == 0

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "column_name",
        [
            "Data Transformation Name",
            "Extract Name",
            "MS Assay Name",
            "NMR Assay Name",
            "Normalization Name",
        ],
    )
    async def test_rule_a_200_090_004_01_02(
        self, ms_assay_modifier: AssayFileModifier, column_name
    ):
        ms_assay_modifier.max_row_number_limit = 3
        metabolights_model = ms_assay_modifier.model
        study = metabolights_model.investigation.studies[0]
        assay_file_name = study.study_assays.assays[0].file_name
        assay_table = ms_assay_modifier.model.assays[assay_file_name].table
        sample_name_column = "Sample Name"
        data_file_column = "Data Transformation Name"
        length = len(assay_table.data[data_file_column])
        assay_table.data[data_file_column] = [""] * length
        ms_assay_modifier.rule_a_200_090_004_01()
        assert set(assay_table.data[data_file_column]) == set(
            assay_table.data[sample_name_column]
        )

        assert len(ms_assay_modifier.update_logs) == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize("column_name", ["NMR Assay Name"])
    async def test_rule_a_200_090_004_01_03(
        self, assay_modifier: AssayFileModifier, column_name
    ):
        assay_modifier.max_row_number_limit = 3
        metabolights_model = assay_modifier.model
        study = metabolights_model.investigation.studies[0]
        assay_file_name = study.study_assays.assays[0].file_name
        assay_table = assay_modifier.model.assays[assay_file_name].table
        sample_name_column = "Sample Name"

        length = len(assay_table.data[column_name])
        assay_table.data[column_name] = [""] * length
        assay_modifier.rule_a_200_090_004_01()
        assert set(assay_table.data[column_name]) == set(
            assay_table.data[sample_name_column]
        )
        assert len(assay_modifier.update_logs) == 1

    @pytest.mark.asyncio
    async def test_rule_a_200_090_004_01_04(self, ms_assay_modifier: AssayFileModifier):
        ms_assay_modifier.max_row_number_limit = 3
        metabolights_model = ms_assay_modifier.model
        study = metabolights_model.investigation.studies[0]
        assay_file_name = study.study_assays.assays[0].file_name
        assay_table = ms_assay_modifier.model.assays[assay_file_name].table
        sample_name_column = "Sample Name"
        columns = [
            "Data Transformation Name",
            "Extract Name",
            "MS Assay Name",
            "Normalization Name",
        ]

        for column in columns:
            length = len(assay_table.data[column])
            assay_table.data[column] = [""] * length

        ms_assay_modifier.rule_a_200_090_004_01()
        for column in columns:
            assert set(assay_table.data[column]) == set(
                assay_table.data[sample_name_column]
            )
        expected_log_items = 4
        assert len(ms_assay_modifier.update_logs) == expected_log_items


class TestUpdateScanPolarity:
    @pytest.mark.asyncio
    async def test_update_scan_polarity_01(self, ms_assay_modifier: AssayFileModifier):
        ms_assay_modifier.max_row_number_limit = 3
        ms_assay_modifier.update_scan_polarity()
        assert len(ms_assay_modifier.update_logs) == 0

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "polarity",
        ["positive", "negative", "alternating"],
    )
    async def test_update_scan_polarity_02(
        self, ms_assay_modifier: AssayFileModifier, polarity: str
    ):
        ms_assay_modifier.max_row_number_limit = 5
        scan_polarity_name = "Parameter Value[Scan polarity]"
        metabolights_model = ms_assay_modifier.model
        study = metabolights_model.investigation.studies[0]
        assay_file_name = study.study_assays.assays[0].file_name
        assay_table = ms_assay_modifier.model.assays[assay_file_name].table
        assay_table.data[scan_polarity_name][0] = " "
        assay_table.data[scan_polarity_name][1] = f" {polarity[:3]}"
        assay_table.data[scan_polarity_name][2] = f"{polarity} scan"
        assay_table.data[scan_polarity_name][3] = f"{polarity} polarity".upper()
        assay_table.data[scan_polarity_name][4] = f"{polarity} scan "
        assay_table.data[scan_polarity_name][5] = f"{polarity[0:3]}  "
        ms_assay_modifier.update_scan_polarity()
        for idx in range(5):
            assay_table.data[scan_polarity_name][idx] == polarity

        assert len(ms_assay_modifier.update_logs) == 1


class TestUpdateProtocolRefColumns:
    @pytest.mark.asyncio
    async def test_update_protocol_ref_columns_01(
        self, ms_assay_modifier: AssayFileModifier
    ):
        ms_assay_modifier.max_row_number_limit = 3
        ms_assay_modifier.update_protocol_ref_columns()
        assert len(ms_assay_modifier.update_logs) == 0

    @pytest.mark.asyncio
    async def test_update_protocol_ref_columns_02(
        self, ms_assay_modifier: AssayFileModifier
    ):
        ms_assay_modifier.max_row_number_limit = 3
        metabolights_model = ms_assay_modifier.model
        study = metabolights_model.investigation.studies[0]
        assay_file_name = study.study_assays.assays[0].file_name
        assay_table = ms_assay_modifier.model.assays[assay_file_name].table
        data_file_column = "Protocol REF"
        assay_table.data[data_file_column][0] = "d"
        assay_table.data[data_file_column][1] = "Mass spectrometry"
        assay_table.data[data_file_column][2] = ""
        assay_table.data[data_file_column][3] = "d"
        ms_assay_modifier.update_protocol_ref_columns()
        assert assay_table.data[data_file_column][0] == "Extraction"
        assert assay_table.data[data_file_column][1] == "Extraction"
        assert assay_table.data[data_file_column][2] == "Extraction"
        assert assay_table.data[data_file_column][3] == "Extraction"
        assert len(ms_assay_modifier.update_logs) == 1
