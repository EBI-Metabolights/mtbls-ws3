from mtbls.domain.component_configs.configuration import BaseConfiguration


class OpaConfiguration(BaseConfiguration):
    validate_schema: bool = False
    timeout_in_seconds: int = 600
    version_url: str = "http://policy-engine:8181/v1/data/metabolights/validation/v2/configuration/version"
    validation_url: str = "http://policy-engine:8181/v1/data/metabolights/validation/v2/report/complete_report?pretty=true"
    templates_url: str = (
        "http://policy-engine:8181/v1/data/metabolights/validation/v2/templates"
    )
    control_lists_url: str = (
        "http://policy-engine:8181/v1/data/metabolights/validation/v2/controls"
    )
    rule_definitions_url: str = "http://policy-engine:8181/v1/data/metabolights/validation/v2/configuration/rules"
