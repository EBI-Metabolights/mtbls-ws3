import datetime
import logging
import re
import time
from typing import Any, Dict, Union

from cachetools import TTLCache
from cachetools_async import cached
from dependency_injector.wiring import Provide, inject
from metabolights_utils.models.metabolights.model import MetabolightsStudyModel

from mtbls.application.decorators.async_task import async_task
from mtbls.application.remote_tasks.common.run_modifier import (
    run_isa_metadata_modifier_task,
)
from mtbls.application.remote_tasks.common.utils import run_coroutine
from mtbls.application.services.interfaces.async_task.async_task_result import (
    AsyncTaskResult,
)
from mtbls.application.services.interfaces.ontology_search_service import (
    OntologySearchService,
)
from mtbls.application.services.interfaces.policy_service import PolicyService
from mtbls.application.services.interfaces.study_metadata_service import (
    StudyMetadataService,
)
from mtbls.application.services.interfaces.study_metadata_service_factory import (
    StudyMetadataServiceFactory,
)
from mtbls.domain.entities.validation.validation_configuration import (
    FieldValueValidation,
    MetadataFileType,
    OntologyValidationType,
    ValidationControls,
)
from mtbls.domain.shared.modifier import StudyMetadataModifierResult, UpdateLog
from mtbls.domain.shared.validator.policy import PolicyResult, PolicyResultList
from mtbls.domain.shared.validator.types import PolicyMessageType, ValidationPhase

logger = logging.getLogger(__name__)


all_validation_phases: list[ValidationPhase] = [
    ValidationPhase.PHASE_1,
    ValidationPhase.PHASE_2,
    ValidationPhase.PHASE_3,
    ValidationPhase.PHASE_4,
]

validation_phase_names = {str(x.value) for x in all_validation_phases}


@async_task(queue="common")
@inject
def run_validation(  # noqa: PLR0913
    *,
    resource_id: str,
    apply_modifiers: bool = True,
    phases: Union[ValidationPhase, None, list[str]] = None,
    serialize_result: bool = True,
    study_metadata_service_factory: StudyMetadataServiceFactory = Provide[
        "services.study_metadata_service_factory"
    ],
    policy_service: PolicyService = Provide["services.policy_service"],
    ontology_search_service: OntologySearchService = Provide[
        "services.ontology_search_service"
    ],
    **kwargs,
) -> AsyncTaskResult:
    try:
        modifier_result = None
        if apply_modifiers:
            logger.info("Run validation with modifiers")
            coroutine = run_validation_task_with_modifiers(
                resource_id,
                study_metadata_service_factory=study_metadata_service_factory,
                policy_service=policy_service,
                phases=phases,
                serialize_result=serialize_result,
                ontology_search_service=ontology_search_service,
            )
        else:
            coroutine = run_validation_task(
                resource_id,
                modifier_result=modifier_result,
                study_metadata_service_factory=study_metadata_service_factory,
                policy_service=policy_service,
                phases=phases,
                serialize_result=serialize_result,
                ontology_search_service=ontology_search_service,
            )
        return run_coroutine(coroutine)

    except Exception as ex:
        logger.error("Validation task execution for %s failed.", resource_id)
        logger.exception(ex)
        raise ex
    finally:
        logger.info("Validation task execution for %s ended.", resource_id)


async def run_validation_task_with_modifiers(
    resource_id: str,
    study_metadata_service_factory: StudyMetadataServiceFactory,
    policy_service: PolicyService,
    phases: Union[ValidationPhase, None, list[str]] = None,
    serialize_result: bool = True,
    ontology_search_service: None | OntologySearchService = None,
) -> Union[Dict[str, Any], PolicyResultList]:
    try:
        modifier_result = await run_isa_metadata_modifier_task(
            resource_id,
            study_metadata_service_factory=study_metadata_service_factory,
            policy_service=policy_service,
            serialize_result=False,
        )
    except Exception as ex:
        logger.error("Error to modify %s: %s", resource_id, ex)
        logger.exception(ex)
        modifier_result = None

    return await run_validation_task(
        resource_id,
        modifier_result=modifier_result,
        study_metadata_service_factory=study_metadata_service_factory,
        policy_service=policy_service,
        phases=phases,
        serialize_result=serialize_result,
        ontology_search_service=ontology_search_service,
    )


async def run_validation_task(  # noqa: PLR0913
    resource_id: str,
    study_metadata_service_factory: StudyMetadataServiceFactory,
    policy_service: PolicyService,
    modifier_result: Union[None, dict, StudyMetadataModifierResult] = None,
    phases: Union[ValidationPhase, None, list[str]] = None,
    serialize_result: bool = True,
    ontology_search_service: None | OntologySearchService = None,
) -> Union[Dict[str, Any], PolicyResultList]:
    logger.info("Running validation for %s", resource_id)
    metadata_service = await study_metadata_service_factory.create_service(resource_id)
    result_list: PolicyResultList = PolicyResultList()
    modifier_result_input: None | StudyMetadataModifierResult = None
    if modifier_result:
        if isinstance(modifier_result, dict):
            modifier_result_input = StudyMetadataModifierResult.model_validate(
                modifier_result
            )
        if modifier_result_input and modifier_result_input.has_error:
            logger.error(
                "Modifier failed for %s: %s.",
                resource_id,
                modifier_result_input.error_message or "",
            )
    if isinstance(phases, ValidationPhase):
        input_phases: list[ValidationPhase] = [phases]
    elif isinstance(phases, list):
        input_phases: list[ValidationPhase] = [
            ValidationPhase(x)
            for x in phases
            if isinstance(x, str) and x in validation_phase_names
        ]
    else:
        input_phases = all_validation_phases

    logger.debug(
        "Running %s validation for phases %s",
        resource_id,
        [str(x) for x in input_phases],
    )

    if not resource_id or not input_phases:
        logger.error("Invalid resource id or phases for %s", resource_id)
        raise ValueError("resource id or phases input is not valid")

    try:
        logger.debug("Get MetaboLights validation input model.")
        model = await get_input_data(metadata_service, input_phases)
        logger.debug("Validate using policy service.")
        policy_result = await validate_by_policy_service(
            resource_id,
            model,
            modifier_result_input,
            policy_service,
            ontology_search_service,
        )
        policy_result.phases = input_phases
        result_list.results.append(policy_result)

    except Exception as ex:
        logger.error("Validation task error for %s.", resource_id)
        logger.exception(ex)
        raise ex
    errors_count = sum(
        1
        for x in policy_result.messages.violations
        if x.type == PolicyMessageType.ERROR
    )

    logger.debug("Validation task ended: Validation Errors %s", errors_count)
    if serialize_result:
        return result_list.model_dump(by_alias=True)

    return result_list


async def validate_by_policy_service(
    resource_id: str,
    model: MetabolightsStudyModel,
    modifier_result: None | StudyMetadataModifierResult,
    policy_service: PolicyService,
    ontology_search_service: None | OntologySearchService = None,
) -> PolicyResult:
    policy_result: PolicyResult = PolicyResult()
    policy_result.resource_id = resource_id
    if modifier_result and modifier_result.resource_id:
        policy_result.metadata_modifier_enabled = True
        if modifier_result.error_message:
            policy_result.metadata_updates = [
                UpdateLog(
                    action="Modifier failed. " + modifier_result.error_message,
                    source="",
                    old_value="",
                    new_value="",
                )
            ]
        elif modifier_result.logs:
            policy_result.metadata_updates = modifier_result.logs
    start_time = time.time()

    for file in model.assays:
        technique = model.assays[file].assay_technique.name
        policy_result.assay_file_techniques[file] = technique

    for file in model.metabolite_assignments:
        technique = model.metabolite_assignments[file].assay_technique.name
        policy_result.maf_file_techniques[file] = technique

    try:
        messages = await policy_service.validate_study(resource_id, model)
        policy_result.start_time = datetime.datetime.fromtimestamp(
            start_time
        ).isoformat()
        policy_result.completion_time = datetime.datetime.fromtimestamp(
            time.time()
        ).isoformat()
        policy_result.messages = messages
    except Exception as ex:
        logger.error("Invalid OPA response or parse error for %s", resource_id)
        logger.exception(ex)
        raise ex
    await post_process_validation_messages(
        model, policy_result, policy_service, ontology_search_service
    )
    return policy_result


def investigation_value_parser(value: str) -> tuple[None | str, None | str, None | str]:
    parts = value.split("\t")
    if len(parts) == 3:
        _, _, part_3 = value.split("\t")
        term, source, accession = [
            x.strip() for x in part_3.strip("[]").split(",", maxsplit=2)
        ]
    else:
        logger.error("The value has no three parts: %s", value)
        return None, None, None

    return term, source, accession


def isa_table_value_parser(value: str) -> tuple[None | str, None | str, None | str]:
    term, source, accession = [
        x.strip() for x in value.strip("[]").split(",", maxsplit=2)
    ]
    return term, source, accession


def find_rule(
    controls: ValidationControls,
    isa_table_type: str,
    study_category: str,
    template_version: str,
    template_name: str,
    created_at: str,
) -> None | FieldValueValidation:
    selected_controls: dict[str, list[FieldValueValidation]] = getattr(
        controls, isa_table_type + "_file_controls"
    )
    control_list = selected_controls.get(template_name, [])
    rule = None
    for control in control_list:
        criteria = control.selection_criteria
        match = all(
            [
                match_equal(criteria.isa_file_type, isa_table_type),
                match_equal(criteria.study_category_filter, study_category),
                match_equal(criteria.template_version_filter, template_version),
                match_equal(criteria.isa_file_template_name_filter, template_name),
                match_ge(criteria.study_created_at_or_after, created_at),
                match_less(criteria.study_created_before, created_at),
            ]
        )
        if match:
            rule = control
            break

    return rule


def match_equal(criterion, val) -> bool:
    if not criterion:
        return True
    if isinstance(criterion, list):
        if any([x for x in criterion if str(x) == val]):
            return True
    elif isinstance(criterion, str) and criterion == val:
        return True

    return False


def match_ge(criterion, val) -> bool:
    if not criterion:
        return True
    return val >= criterion


def match_less(criterion, val) -> bool:
    if not criterion:
        return True
    return val < criterion


def escape(s: str) -> str:
    return s.replace("\t", " ").replace("\n", " ")


@cached(cache=TTLCache(maxsize=2048, ttl=600))
async def search_exact_match_term(
    ontology_search_service: OntologySearchService,
    term: str,
    rule: FieldValueValidation,
):
    return await ontology_search_service.search(term, rule, exact_match=True)


async def post_process_validation_messages(
    model: MetabolightsStudyModel,
    policy_result: PolicyResult,
    policy_service: PolicyService,
    ontology_search_service: None | OntologySearchService = None,
) -> None:
    if not ontology_search_service or not policy_result:
        return None
    controls = await policy_service.get_control_lists()
    file_templates = await policy_service.get_templates()

    search_validation_rules = {
        "rule_a_200_900_001_01": ("assay", isa_table_value_parser),
        "rule_s_200_900_001_01": ("sample", isa_table_value_parser),
        "rule_i_200_900_001_01": ("investigation", investigation_value_parser),
        "rule_m_200_900_001_01": ("assignment", isa_table_value_parser),
    }
    default_rule_value = ("", lambda x: "", "", "", MetadataFileType.INVESTIGATION)

    new_violations = []
    if not controls:
        logger.error("Policy service does not return control lists")
        return
    created_at = model.study_db_metadata.created_at
    sample_template = model.study_db_metadata.sample_template
    template_version = model.study_db_metadata.template_version

    study_category = model.study_db_metadata.study_category
    category_name = study_category.name.lower().replace("_", "-")

    search_keys = set(search_validation_rules.keys())
    for violation in policy_result.messages.violations:
        identifier = violation.identifier
        if identifier not in search_keys:
            new_violations.append(violation)
            continue
        search_params = search_validation_rules.get(identifier, default_rule_value)
        isa_table_type = search_params[0]
        parser = search_params[1]

        template_name = ""
        if isa_table_type == "sample":
            template_name = sample_template
        elif isa_table_type == "assay":
            assay_file = model.assays.get(violation.source_file, None)
            if assay_file:
                template_name = assay_file.assay_technique.name

        new_values = []
        deleted_values = []
        default_controls = file_templates.configuration.default_file_controls.get(
            MetadataFileType(isa_table_type), []
        )
        field_key = violation.source_column_header
        if not field_key:
            field_key = "__default__"
        default_template_key = None
        for default_control in default_controls:
            match = re.match(default_control.key_pattern, field_key)
            if match:
                default_template_key = default_control.default_key
                break
        default_rule = None
        if default_template_key:
            default_rule = find_rule(
                controls,
                isa_table_type,
                category_name,
                template_version,
                default_template_key,
                created_at,
            )

        rule = find_rule(
            controls,
            isa_table_type,
            category_name,
            template_version,
            template_name,
            created_at,
        )
        selected_rule = rule or default_rule
        is_child_rule = (
            rule and rule.validation_type == OntologyValidationType.CHILD_ONTOLOGY_TERM
        )
        parents = []
        if is_child_rule and rule and rule.allowed_parent_ontology_terms:
            parents = rule.allowed_parent_ontology_terms.parents
        for value in violation.values:
            term, source, accession = parser(value)
            if is_exceptional_term(selected_rule, term, source, accession):
                continue

            if is_child_rule and rule and term:
                search = await search_exact_match_term(
                    ontology_search_service, term, rule
                )
                if not search.result:
                    logger.warning("'%s' is not valid or a child of parents.", value)
                    new_values.append(value)
                elif (
                    search.result[0].term_accession_number != accession
                    or search.result[0].term_source_ref != source
                ):
                    logger.warning(
                        "Current: %s search result: [%s, %s, %s]",
                        value,
                        search.result[0].term,
                        search.result[0].term_source_ref,
                        search.result[0].term_accession_number,
                    )
                    new_values.append(value)
                else:
                    deleted_values.append(value)

            elif accession and source:
                search = await ontology_search_service.find_by_accession(
                    accession, ontology=source
                )
                if not search.result:
                    logger.warning("'%s' is not found on ontology service", value)
                    new_values.append(value)
                elif search.result[0].term != term:
                    logger.warning(
                        "Term '%s' (not '%s') is found for %s",
                        search.result[0].term,
                        term,
                        accession,
                    )
                    new_values.append(value)
                else:
                    deleted_values.append(value)

        if new_values:
            if isa_table_type in {"assay", "sample"}:
                field = violation.source_column_header
            else:
                field = new_values[0].split("\t")[0]

            if is_child_rule:
                violation.violation += (
                    f"{field} ontology terms not found on ontology search service"
                    + " or they are not children of parent terms."
                    + "Ontology terms: "
                    + ", ".join([escape(x) for x in new_values])
                    + " Parents: "
                    + ", ".join([str(x) for x in parents])
                )
            else:
                violation.violation = (
                    f"{field} ontology terms not found on ontology search service: "
                    + ", ".join([escape(x) for x in new_values])
                )
            violation.values = new_values
            new_violations.append(violation)
        else:
            logger.debug(
                "Terms in violation are validated "
                "and the violation is removed: "
                "%s %s %s",
                violation.identifier,
                violation.source_file,
                ", ".join(violation.values),
            )
    policy_result.messages.violations = new_violations


def is_exceptional_term(
    default_rule: FieldValueValidation, term: str, source: str, accession: str
):
    if not default_rule:
        return False
    accession = accession or ""
    source = source or ""
    term = term or ""
    if default_rule.allowed_placeholders:
        for item in default_rule.allowed_placeholders:
            if (
                item.term_accession_number == accession
                and item.term_source_ref == source
            ):
                return True
    if default_rule.allowed_other_sources:
        for item in default_rule.allowed_other_sources:
            if (
                accession.startswith(item.accession_prefix)
                and item.source_label == source
            ):
                return True
    if default_rule.allowed_missing_ontology_terms:
        for item in default_rule.allowed_missing_ontology_terms:
            if (
                item.term == term
                and item.term_accession_number == accession
                and item.term_source_ref == source
            ):
                return True
        return False


async def get_input_data(
    metadata_service: StudyMetadataService,
    phases: list[ValidationPhase],
) -> MetabolightsStudyModel:
    phases.sort(key=lambda x: x.value)
    load_sample_file = False
    load_assay_files = False
    load_maf_files = False
    load_folder_metadata = False
    load_db_metadata = True
    for phase in phases:
        if phase == ValidationPhase.PHASE_2:
            load_sample_file = True
            load_assay_files = True
        elif phase == ValidationPhase.PHASE_3:
            load_sample_file = True
            load_maf_files = True
        elif phase == ValidationPhase.PHASE_4:
            load_folder_metadata = True
    return await metadata_service.load_study_model(
        load_sample_file=load_sample_file,
        load_assay_files=load_assay_files,
        load_maf_files=load_maf_files,
        load_folder_metadata=load_folder_metadata,
        load_db_metadata=load_db_metadata,
    )
