from mtbls.domain.shared.async_task.async_task_summary import (
    AsyncTaskSummary,
)
from mtbls.domain.shared.validator.policy import PolicySummaryResult


class StartValidationResponse(AsyncTaskSummary[PolicySummaryResult]): ...


class GetValidationResponse(AsyncTaskSummary[PolicySummaryResult]): ...
