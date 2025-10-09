from pathlib import Path
from typing import Annotated, Dict, Set

from metabolights_utils.isatab import IsaTableFileReader, Reader
from metabolights_utils.isatab.reader import IsaTableFileReaderResult
from metabolights_utils.models.isa.assay_file import AssayFile
from metabolights_utils.models.isa.investigation_file import Investigation
from typing_extensions import Doc

from mtbls.presentation.rest_api.core.base import APIBaseModel


class AssayTechnique(APIBaseModel):
    name: str = ""
    main: str = ""
    technique: str = ""
    sub: str = ""

    def __str__(self) -> str:
        return self.name

    def __eq__(self, other):
        if not isinstance(other, AssayTechnique):
            return False
        return self.name == other.name

    def __hash__(self):
        return hash(self.name if self.name else "")

    def __repr__(self):
        return self.__str__()


class AssayClassifier:
    ASSAY_TECHNIQUES_MAPPING = {
        "LC-MS": AssayTechnique(name="LC-MS", main="MS", technique="LC-MS", sub="LC"),
        "LC-DAD": AssayTechnique(
            name="LC-DAD", main="MS", technique="LC-MS", sub="LC-DAD"
        ),
        "GC-MS": AssayTechnique(name="GC-MS", main="MS", technique="GC-MS", sub="GC"),
        "GCxGC-MS": AssayTechnique(
            name="GCxGC-MS", main="MS", technique="GC-MS", sub="Tandem (GCxGC)"
        ),
        "GC-FID": AssayTechnique(
            name="GC-FID", main="MS", technique="GC-MS", sub="GC-FID"
        ),
        "MS": AssayTechnique(
            name="MS", main="MS", technique="Direct Injection", sub="MS"
        ),
        "DI-MS": AssayTechnique(
            name="DI-MS",
            main="MS",
            technique="Direct Injection",
            sub="Direct infusion (DI)",
        ),
        "FIA-MS": AssayTechnique(
            name="FIA-MS",
            main="MS",
            technique="Direct Injection",
            sub="Flow injection analysis (FIA)",
        ),
        "CE-MS": AssayTechnique(
            name="CE-MS",
            main="MS",
            technique="Direct Injection",
            sub="Capillary electrophoresis (CE)",
        ),
        "MALDI-MS": AssayTechnique(
            name="MALDI-MS",
            main="MS",
            technique="Direct Injection",
            sub="Matrix-assisted laser desorption-ionisation imaging "
            "mass spectrometry (MALDI)",
        ),
        "SPE-IMS-MS": AssayTechnique(
            name="SPE-IMS-MS",
            main="MS",
            technique="Direct Injection",
            sub="Solid-Phase Extraction Ion Mobility Spectrometry (SPE-IMS)",
        ),
        "MSImaging": AssayTechnique(
            name="MSImaging", main="MS", technique="MS Imaging", sub="MS Imaging"
        ),
        "NMR": AssayTechnique(
            name="NMR",
            main="NMR",
            technique="NMR",
            sub="Nuclear magnetic resonance",
        ),
        "MRImaging": AssayTechnique(
            name="MRImaging",
            main="NMR",
            technique="MRI",
            sub="Magnetic resonance imaging",
        ),
    }

    ASSAY_PLATFORM_KEYWORDS: Dict[str, str] = {
        "Liquid Chromatography MS": "LC-MS",
        "Diode array detection MS": "LC-DAD",
        "Gas Chromatography MS": "GC-MS",
        "Tandem Gas Chromatography MS": "GCxGC-MS",
        "Flame ionisation detector MS": "GC-FID",
        "Mass spectrometry": "MS",
        "Direct infusion MS": "DI-MS",
        "Flow injection analysis MS": "FIA-MS",
        "Capillary electrophoresis MS": "CE-MS",
        "Matrix-assisted laser desorption-ionisation imaging MS": "MALDI-MS",
        "Solid-Phase Extraction Ion Mobility Spectrometry MS": "SPE-IMS-MS",
        "MS Imaging": "MSImaging",
        "Nuclear Magnetic Resonance (NMR)": "NMR",
        "Magnetic resonance imaging": "MRImaging",
    }

    MANUAL_ASSIGNMENTS: Annotated[
        Set[str], Doc("Curator assignments in DB will be mapped to techniques")
    ] = {
        "3D LAESI imaging MS": "MSImaging",
        "3D MALDI imaging MS": "MSImaging",
        "3D MALDI imaging MS simulation": "MSImaging",
        "3D MALDI imaging MS,3D DESI imaging MS": "MSImaging",
        "CE-TOF-MS": "CE-MS",
        "DI-FT-ICR-MS": "DI-MS",
        "DI-FT-ICR-MS/MS": "DI-MS",
        "DI-LTQ-MS": "DI-MS",
        "DI-LTQ-MS/MS": "DI-MS",
        "FIA-LTQ-MS": "FIA-MS",
        "FIA-LTQ-MS/MS": "FIA-MS",
        "FIA-MS": "FIA-MS",
        "FIA-QTOF-MS": "FIA-MS",
        "GC-Q-MS": "GC-MS",
        "GC-TOF-MS": "GC-MS",
        "HPLC-LTQ-MS": "LC-MS",
        "LC-LTQ-MS": "LC-MS",
        "LC-QTOF-MS": "LC-MS",
        "LC-TOF-MS": "LC-MS",
        "LC-TQ-MS": "LC-MS",
        "MALDI-TOF/TOF-MS": "MALDI-MS",
        "MS": "MS",
        "PTR-MS": "MS",
        "REI-QTOF-MS": "DI-MS",
        "SESI-LTQ-MS": "DI-MS",
        "SPE-IMS-MS": "LC-MS",
        "UPLC-LTQ-MS": "LC-MS",
        "UPLC-LTQ-MS/MS": "LC-MS",
        "UPLC-LTQ-MS/MS,UPLC-TQ-MS/MS": "LC-MS",
        "UPLC-MS/MS": "LC-MS",
        "UPLC-QTOF-MS": "LC-MS",
        "UPLC-QTOF-MS/MS": "LC-MS",
        "UPLC-TQ-MS": "LC-MS",
        "UPLC-TQ-MS MTBLS935 dupe?": "LC-MS",
        "UPLC-TQ-MS MTBLS936 dupe?": "LC-MS",
    }

    @staticmethod
    def find_assay_technique(
        investigation: Investigation,
        assay_path: Path,
        manual_assignment: str,
    ) -> AssayTechnique:
        assay_reader: IsaTableFileReader = Reader.get_assay_file_reader(
            results_per_page=100000
        )
        assay_file_result: IsaTableFileReaderResult = assay_reader.get_headers(
            file_buffer_or_path=assay_path
        )
        assay_file: AssayFile = assay_file_result.isa_table_file
        if not investigation or not investigation.studies:
            return AssayTechnique()

        all_columns = assay_file.table.columns
        for study in investigation.studies:
            for assay in study.study_assays.assays:
                if assay.file_name == assay_file.file_path:
                    for technique in AssayClassifier.ASSAY_PLATFORM_KEYWORDS:
                        if technique in assay.technology_platform:
                            return AssayClassifier.ASSAY_TECHNIQUES_MAPPING[
                                AssayClassifier.ASSAY_PLATFORM_KEYWORDS[technique]
                            ]

        if "NMR Assay Name" in all_columns and "MS Assay Name" in all_columns:
            return AssayTechnique()

        if "NMR Assay Name" in all_columns:
            if (
                "Parameter Value[Tomography]" in all_columns
                and "Parameter Value[Magnetic pulse sequence name]" in all_columns
            ):
                return AssayClassifier.ASSAY_TECHNIQUES_MAPPING["MRImaging"]
            else:
                return AssayClassifier.ASSAY_TECHNIQUES_MAPPING["NMR"]

        elif "MS Assay Name" in all_columns:
            if "Parameter Value[DI Instrument]" in all_columns:
                return AssayClassifier.ASSAY_TECHNIQUES_MAPPING["DI-MS"]
            elif "Parameter Value[FIA Instrument]" in all_columns:
                return AssayClassifier.ASSAY_TECHNIQUES_MAPPING["FIA-MS"]
            elif "Parameter Value[CE Instrument]" in all_columns:
                return AssayClassifier.ASSAY_TECHNIQUES_MAPPING["CE-MS"]
            elif (
                "Parameter Value[Column type 1]" in all_columns
                and "Parameter Value[Column type 2]" in all_columns
            ):
                return AssayClassifier.ASSAY_TECHNIQUES_MAPPING["GCxGC-MS"]
            elif "Parameter Value[SPE-IMS Instrument]" in all_columns:
                return AssayClassifier.ASSAY_TECHNIQUES_MAPPING["SPE-IMS-MS"]
            elif "Thermal Desorption unit" in all_columns:
                return AssayClassifier.ASSAY_TECHNIQUES_MAPPING["TD-GC-MS"]
            elif "Parameter Value[Sectioning instrument]" in all_columns:
                return AssayClassifier.ASSAY_TECHNIQUES_MAPPING["MSImaging"]
            elif (
                "Parameter Value[Signal range]" in all_columns
                and "Parameter Value[Resolution]" in all_columns
            ):
                return AssayClassifier.ASSAY_TECHNIQUES_MAPPING["LC-DAD"]
            else:
                if "Parameter Value[Column type]" in all_columns:
                    column_type_values_result: IsaTableFileReaderResult = (
                        assay_reader.read(
                            file_buffer_or_path=assay_path,
                            offset=0,
                            limit=1000000,
                            selected_columns=["Parameter Value[Column type]"],
                        )
                    )
                    colun_type_data = (
                        column_type_values_result.isa_table_file.table.data
                    )
                    values = colun_type_data["Parameter Value[Column type]"]
                    for i in range(len(values) if len(values) < 3 else 3):
                        if values[i]:
                            column_type = values[i].lower()
                            if "hilic" in column_type or "reverse" in column_type:
                                return AssayClassifier.ASSAY_TECHNIQUES_MAPPING["LC-MS"]
                            elif (
                                "low polarity" in column_type
                                or "high polarity" in column_type
                                or "medium polarity" in column_type
                            ):
                                return AssayClassifier.ASSAY_TECHNIQUES_MAPPING["GC-MS"]

            if manual_assignment:
                if manual_assignment in AssayClassifier.MANUAL_ASSIGNMENTS:
                    manual_map = AssayClassifier.MANUAL_ASSIGNMENTS[manual_assignment]
                    return AssayClassifier.ASSAY_TECHNIQUES_MAPPING[manual_map]

            if "_FIA" in assay_file.file_path:
                return AssayClassifier.ASSAY_TECHNIQUES_MAPPING["FIA-MS"]

        if investigation and investigation.studies:
            for study_item in investigation.studies:
                if (
                    study_item
                    and study_item.study_assays
                    and study_item.study_assays.assays
                ):
                    for assay_item in study_item.study_assays.assays:
                        if assay_item.file_name == assay_file.file_path:
                            if "mass spectrometry" in assay_item.technology_type.term:
                                return AssayClassifier.ASSAY_TECHNIQUES_MAPPING["MS"]
                            elif "NMR spectrometry" in assay_item.technology_type.term:
                                return AssayClassifier.ASSAY_TECHNIQUES_MAPPING["NMR"]
                            return AssayTechnique()

        return AssayTechnique()
