import pytest
from metabolights_utils.models.metabolights.model import MetabolightsStudyModel
from metabolights_utils.models.parser.common import ParserMessage

from mtbls.domain.domain_services.modifier.sample_modifier import SampleFileModifier
from mtbls.domain.entities.validation.validation_configuration import (
    FileTemplates,
    ValidationControls,
)


@pytest.fixture(scope="function")
def sample_modifier(
    templates: FileTemplates,
    control_lists: ValidationControls,
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


all_technique_names = [
    "LC-MS",
    "GC-MS",
    "DI-MS",
    "NMR",
    "MRImaging",
    "MSImaging",
    "CE-MS",
    "FIA-MS",
    "GC-FID",
    "GCxGC-MS",
    "LC-DAD",
    "MALDI-MS",
    "MS",
]


class TestGetProtocolTemplate:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("technique", all_technique_names)
    async def test_get_protocol_template_01(
        self, sample_modifier: SampleFileModifier, technique: str
    ):
        template = sample_modifier.get_protocol_template(technique_name=technique)
        assert template

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "technique",
        ["", None, "Invalid Technique"],
    )
    async def test_get_protocol_template_02(
        self, sample_modifier: SampleFileModifier, technique: str
    ):
        template = sample_modifier.get_protocol_template(technique_name=technique)
        assert not template


class TestOrderedProtocolNames:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("technique", all_technique_names)
    async def test_get_ordered_protocol_names_01(
        self, sample_modifier: SampleFileModifier, technique: str
    ):
        template = sample_modifier.get_ordered_protocol_names(technique_name=technique)
        assert template

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "technique",
        ["", None, "Invalid Technique"],
    )
    async def test_get_ordered_protocol_names_02(
        self, sample_modifier: SampleFileModifier, technique: str
    ):
        template = sample_modifier.get_ordered_protocol_names(technique_name=technique)
        assert not template

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "technique",
        ["LC-MS"],
    )
    async def test_get_ordered_protocol_names_03(
        self, sample_modifier: SampleFileModifier, technique: str
    ):
        sample_modifier.templates = {}
        template = sample_modifier.get_ordered_protocol_names(technique_name=technique)
        assert not template


class TestGetProtocolParametersInAssay:
    @pytest.mark.asyncio
    async def test_get_protocol_parameters_in_assay_01(
        self, sample_modifier: SampleFileModifier
    ):
        assay = sample_modifier.model.investigation.studies[0].study_assays.assays[0]
        assay_file = sample_modifier.model.assays[assay.file_name]
        result = sample_modifier.get_protocol_parameters_in_assay(assay_file)
        assert len(result) > 0

    @pytest.mark.asyncio
    async def test_get_protocol_parameters_in_assay_02(
        self, sample_modifier: SampleFileModifier
    ):
        assay = sample_modifier.model.investigation.studies[0].study_assays.assays[0]
        assay_file = sample_modifier.model.assays[assay.file_name]
        assay_file.assay_technique.name = ""
        row_count = len(assay_file.table.data["Protocol REF"])
        assay_file.table.data["Protocol REF"] = [""] * row_count
        assay_file.table.data["Protocol REF.1"] = [""] * row_count
        assay_file.table.data["Protocol REF.2"] = [""] * row_count
        result = sample_modifier.get_protocol_parameters_in_assay(assay_file)
        assert result["Protocol REF"][1] == "Unnamed protocol.1"
        assert result["Protocol REF.1"][1] == "Unnamed protocol.2"
        assert result["Protocol REF.2"][1] == "Unnamed protocol.3"
        assert len(result) > 0


class TestGetProtocolParameters:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("technique", all_technique_names)
    async def test_get_protocol_parameters_01(
        self, sample_modifier: SampleFileModifier, technique: str
    ):
        result_1 = sample_modifier.get_protocol_parameters(techniques=[technique])
        result_2 = sample_modifier.get_protocol_parameters(techniques=[technique])

        assert len(result_1) > 0
        assert result_1 == result_2


class DetectFileParseUpdates:
    @pytest.mark.asyncio
    async def test_detect_file_parse_updates_01(
        self, sample_modifier: SampleFileModifier
    ):
        sample_modifier.update_from_parser_messages()

        assert len(sample_modifier.update_logs) == 0

    @pytest.mark.asyncio
    async def test_detect_file_parse_updates_02(
        self, sample_modifier: SampleFileModifier
    ):
        file_name = sample_modifier.model.investigation.studies[0].file_name

        sample_modifier.model.parser_messages[file_name] = [
            ParserMessage(
                short=f"Removed empty lines from {file_name}",
                detail=f"Removed empty lines from {file_name}",
            ),
            ParserMessage(
                short=f"Removed new line characters from {file_name}",
                detail=f"Removed new line characters from {file_name}",
            ),
            ParserMessage(
                short=f"Removed new line characters from {file_name}",
                detail=f"Removed new line characters from {file_name}",
            ),
        ]
        sample_modifier.update_from_parser_messages()
        expected_log_items = 2
        assert len(sample_modifier.update_logs) == expected_log_items
