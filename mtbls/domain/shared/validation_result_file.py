from metabolights_utils.common import CamelCaseModel


class ValidationResultFile(CamelCaseModel):
    validation_time: str = ""
    task_id: str = ""
