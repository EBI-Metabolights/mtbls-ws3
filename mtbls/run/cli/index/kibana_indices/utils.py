from typing import Literal

from mtbls.application.use_cases.indices.kibana_indices.index_manager import (
    DataIndexConfiguration,
)
from mtbls.domain.enums.study_status import StudyStatus


def get_data_index_configuration(
    scope: Literal["public", "completed", "all"] = "public", test: bool = False
):
    if scope == "public":
        data_index_configuration = DataIndexConfiguration(
            sample_index_name="test-" if test else "" + "sample-kibana-public",
            study_index_name="test-" if test else "" + "study-kibana-public",
            assignment_index_name="test-" if test else "" + "assignment-kibana-public",
            assay_index_name="test-" if test else "" + "assay-kibana-public",
            target_study_status_list=[StudyStatus.PUBLIC],
        )
    elif scope == "completed":
        data_index_configuration = DataIndexConfiguration(
            sample_index_name="test-" if test else "" + "sample-kibana-complete",
            study_index_name="test-" if test else "" + "study-kibana-complete",
            assignment_index_name="test-"
            if test
            else "" + "assignment-kibana-complete",
            assay_index_name="test-" if test else "" + "assay-kibana-complete",
            target_study_status_list=[StudyStatus.PUBLIC, StudyStatus.PRIVATE],
        )
    else:
        raise ValueError(f"invalid scoped option {scope}")
    return data_index_configuration
