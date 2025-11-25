import datetime
import logging
import time
from typing import Any, Dict, Union

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
    OntologyValidationType,
    ValidationControls,
)
from mtbls.domain.shared.modifier import StudyMetadataModifierResult, UpdateLog
from mtbls.domain.shared.validator.policy import PolicyResult, PolicyResultList
from mtbls.domain.shared.validator.types import PolicyMessageType, ValidationPhase

logger = logging.getLogger(__name__)


all_validation_phases = [
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
    modifier_result: Union[dict, StudyMetadataModifierResult] = None,
    phases: Union[ValidationPhase, None, list[str]] = None,
    serialize_result: bool = True,
    ontology_search_service: None | OntologySearchService = None,
) -> Union[Dict[str, Any], PolicyResultList]:
    logger.info("Running validation for %s", resource_id)
    metadata_service = await study_metadata_service_factory.create_service(resource_id)
    result_list: PolicyResultList = PolicyResultList()
    if modifier_result and isinstance(modifier_result, dict):
        modifier_result = StudyMetadataModifierResult.model_validate(modifier_result)
    if modifier_result and modifier_result.has_error:
        logger.error(
            "Modifier failed for %s: %s.",
            resource_id,
            modifier_result.error_message or "",
        )
    if isinstance(phases, ValidationPhase):
        phases = [phases]
    elif isinstance(phases, list):
        phases = [
            ValidationPhase(x)
            for x in phases
            if isinstance(x, str) and x in validation_phase_names
        ]
    else:
        phases = all_validation_phases

    logger.debug(
        "Running %s validation for phases %s", resource_id, [str(x) for x in phases]
    )

    if not resource_id or not phases:
        logger.error("Invalid resource id or phases for %s", resource_id)
        raise ValueError(message="Inputs are not valid")

    try:
        logger.debug("Get MetaboLights validation input model.")
        model = await get_input_data(metadata_service, phases)
        logger.debug("Validate using policy service.")
        policy_result = await validate_by_policy_service(
            resource_id, model, modifier_result, policy_service, ontology_search_service
        )
        policy_result.phases = phases
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
    modifier_result: StudyMetadataModifierResult,
    policy_service: PolicyService,
    ontology_search_service: None | OntologySearchService = None,
) -> PolicyResult:
    policy_result: PolicyResult = PolicyResult()
    policy_result.resource_id = resource_id
    if modifier_result and modifier_result.resource_id:
        policy_result.metadata_modifier_enabled = True
        if modifier_result.error_message:
            policy_result.metadata_updates = [
                UpdateLog(action="Modifier failed. " + modifier_result.error_message)
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


def investigation_value_parser(value: str) -> str:
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


def isa_table_value_parser(value: str):
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
            return control
    return None


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


async def post_process_validation_messages(
    model: MetabolightsStudyModel,
    policy_result: PolicyResult,
    policy_service: PolicyService,
    ontology_search_service: None | OntologySearchService = None,
) -> None:
    if not ontology_search_service or not policy_result:
        return None
    search_validation_rules = {
        "rule_a_200_900_001_01": ("assay", isa_table_value_parser),
        "rule_s_200_900_001_01": ("sample", isa_table_value_parser),
        "rule_i_200_900_001_01": ("investigation", investigation_value_parser),
    }
    new_violations = []
    controls = await policy_service.get_control_lists()
    created_at = model.study_db_metadata.created_at
    sample_template = model.study_db_metadata.sample_template
    template_version = model.study_db_metadata.template_version

    study_category = model.study_db_metadata.study_category.value

    search_keys = set(search_validation_rules.keys())
    for violation in policy_result.messages.violations:
        identifier = violation.identifier
        if identifier not in search_keys:
            new_violations.append(violation)
            continue
        isa_table_type = search_validation_rules.get(identifier)[0]
        template_name = None
        if isa_table_type == "sample":
            template_name == sample_template
        elif isa_table_type == "assay":
            assay_file = model.assays.get(violation.source_file, None)
            if assay_file:
                template_name = assay_file.assay_technique.name

        parser = search_validation_rules.get(identifier)[1]
        new_values = []
        deleted_values = []
        rule = find_rule(
            controls,
            isa_table_type,
            study_category,
            template_version,
            template_name,
            created_at,
        )
        is_child_rule = (
            rule and rule.validation_type == OntologyValidationType.CHILD_ONTOLOGY_TERM
        )
        parents = []
        if is_child_rule:
            parents = rule.allowed_parent_ontology_terms.parents
        for value in violation.values:
            term, source, accession = parser(value)

            if source in {"MTBLS", "WoRM"}:
                new_values.append(value)
                continue

            if is_child_rule:
                search = await ontology_search_service.search(
                    term, rule, exact_match=True
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

            else:
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
            logger.info(
                "Terms in violation are validated "
                "and the violation is removed: "
                "%s %s %s",
                violation.identifier,
                violation.source_file,
                ", ".join(violation.values),
            )
    policy_result.messages.violations = new_violations
    return policy_result


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
