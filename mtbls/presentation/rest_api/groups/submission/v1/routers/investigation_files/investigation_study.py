from logging import getLogger
from typing import Annotated

from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends
from metabolights_utils.models.isa.investigation_file import (
    Assay,
    Factor,
    OntologyAnnotation,
    OntologySourceReference,
    Person,
    Protocol,
    Publication,
    Study,
)

from mtbls.application.services.interfaces.study_metadata_service_factory import (
    StudyMetadataServiceFactory,
)
from mtbls.domain.entities import investigation as inv
from mtbls.domain.exceptions.repository import StudyObjectNotFoundError
from mtbls.domain.shared.permission import StudyPermissionContext
from mtbls.presentation.rest_api.core.responses import APIErrorResponse, APIResponse
from mtbls.presentation.rest_api.groups.auth.v1.routers.dependencies import (
    check_read_permission,
)
from mtbls.presentation.rest_api.groups.submission.v1.routers.investigation_files.extended_collection_template import (
    ExtendedStudyItemCollection,
)
from mtbls.presentation.rest_api.shared.dependencies import get_resource_id

endpoint_prefix = "/submissions/v2/investigation-files"


logger = getLogger(__file__)

router = APIRouter(prefix=endpoint_prefix)


def create_endpoint(api_router: APIRouter):
    for collection in REST_API_COLLECTIONS:
        collection.add_api_routes(api_router=api_router)


REST_API_COLLECTIONS = [
    ExtendedStudyItemCollection(
        endpoint_prefix=endpoint_prefix,
        collection_name="Studies",
        item_name="Study",
        collection_route_path="/{resource_id}/studies",
        collection_jsonpath="studies",
        input_model_class=inv.CommentedStudyItem,
        output_model_class=inv.CommentedStudyItem,
        target_model_class=Study,
        updatable_fields=[
            ("title", str),
            ("description", str),
            ("submission_date", str),
            ("public_release_date", str),
            ("file_name", str),
        ],
    ),
    ExtendedStudyItemCollection(
        endpoint_prefix=endpoint_prefix,
        collection_name="Study Assays",
        item_name="Study Assay",
        collection_route_path="/{resource_id}/study/assays",
        collection_jsonpath="studies[0].assays",
        input_model_class=inv.CommentedAssayItem,
        output_model_class=inv.CommentedAssayItem,
        target_model_class=Assay,
        updatable_fields=[
            ("file_name", str),
            ("technology_platform", str),
            ("measurement_type", inv.OntologyItem),
            ("technology_type", inv.OntologyItem),
        ],
    ),
    ExtendedStudyItemCollection(
        endpoint_prefix=endpoint_prefix,
        collection_name="Study Design Descriptors",
        item_name="Study Design Descriptor",
        collection_route_path="/{resource_id}/study/design-descriptors",
        collection_jsonpath="studies[0].design_descriptors",
        input_model_class=inv.CommentedDesignDescriptor,
        output_model_class=inv.CommentedDesignDescriptor,
        target_model_class=OntologyAnnotation,
    ),
    ExtendedStudyItemCollection(
        endpoint_prefix=endpoint_prefix,
        collection_name="Study Protocols",
        item_name="Study Protocol",
        collection_route_path="/{resource_id}/study/protocols",
        collection_jsonpath="studies[0].protocols",
        input_model_class=inv.CommentedProtocolItem,
        output_model_class=inv.CommentedProtocolItem,
        target_model_class=Protocol,
        ontology_subitems=["parameters"],
        # value_type_subitems=["components"],
        updatable_fields=[
            ("name", str),
            ("description", str),
            ("protocol_type", inv.OntologyItem),
        ],
    ),
    ExtendedStudyItemCollection(
        endpoint_prefix=endpoint_prefix,
        collection_name="Study Factors",
        item_name="Study Factor",
        collection_route_path="/{resource_id}/study/factors",
        collection_jsonpath="studies[0].factors",
        input_model_class=inv.CommentedFactorItem,
        output_model_class=inv.CommentedFactorItem,
        target_model_class=Factor,
        updatable_fields=[
            ("name", str),
            ("type", inv.OntologyItem),
        ],
    ),
    ExtendedStudyItemCollection(
        endpoint_prefix=endpoint_prefix,
        collection_name="Study Publications",
        item_name="Study Publication",
        collection_route_path="/{resource_id}/study/publications",
        collection_jsonpath="studies[0].publications",
        input_model_class=inv.CommentedPublicationItem,
        output_model_class=inv.CommentedPublicationItem,
        target_model_class=Publication,
        updatable_fields=[
            ("doi", str),
            ("title", str),
            ("author_list", str),
            ("pub_med_id", str),
            ("status", inv.OntologyItem),
        ],
    ),
    ExtendedStudyItemCollection(
        endpoint_prefix=endpoint_prefix,
        collection_name="Study Contacts",
        item_name="Study Contact",
        collection_route_path="/{resource_id}/study/contacts",
        collection_jsonpath="studies[0].contacts",
        input_model_class=inv.CommentedPersonItem,
        output_model_class=inv.CommentedPersonItem,
        target_model_class=Person,
        ontology_subitems=["roles"],
    ),
    ExtendedStudyItemCollection(
        endpoint_prefix=endpoint_prefix,
        collection_name="Ontology Source References",
        item_name="Ontology Source Reference",
        collection_route_path="/{resource_id}/ontology-source-references",
        collection_jsonpath="ontology_source_references",
        input_model_class=inv.CommentedOntologySourceReferenceItem,
        output_model_class=inv.CommentedOntologySourceReferenceItem,
        target_model_class=OntologySourceReference,
    ),
]


@router.get(
    "/{resource_id}",
    tags=["Investigation Files"],
    summary="Get Investigation file content",
    description="Get Investigation file content",
    response_model=APIResponse[inv.InvestigationItem],
)
@inject
async def get_investigation_file(
    resource_id: Annotated[str, Depends(get_resource_id)],
    context: Annotated[StudyPermissionContext, Depends(check_read_permission)],
    study_metadata_service_factory: StudyMetadataServiceFactory = Depends(
        Provide[  # noqa: FAST002
            "services.study_metadata_service_factory"
        ]
    ),
):
    metadata_service = await study_metadata_service_factory.create_service(resource_id)
    with metadata_service:
        investigation_item: inv.InvestigationItem = (
            await metadata_service.load_investigation_file()
        )

        if not investigation_item:
            return APIErrorResponse(
                error_message=f"Invalid input for the study {resource_id}"
            )
        return APIResponse(content=investigation_item)


@router.put(
    "/{resource_id}",
    tags=["Investigation Files"],
    summary="Create Investigation file if not exist",
    description="Create Investigation file if not exist",
    response_model=APIResponse[inv.InvestigationItem],
)
@inject
async def create_investigation_file(
    resource_id: Annotated[str, Depends(get_resource_id)],
    context: Annotated[StudyPermissionContext, Depends(check_read_permission)],
    study_metadata_service_factory: StudyMetadataServiceFactory = Depends(
        Provide[  # noqa: FAST002
            "services.study_metadata_service_factory"
        ]
    ),
):
    metadata_service = await study_metadata_service_factory.create_service(resource_id)
    with metadata_service:
        try:
            investigation_item: inv.InvestigationItem = (
                await metadata_service.load_investigation_file()
            )
        except StudyObjectNotFoundError:
            study = context.study
            investigation_item = inv.InvestigationItem(
                identifier=study.accession_number,
                submission_date=study.submission_date.strftime("%Y-%m-%d"),
                public_release_date=study.release_date.strftime("%Y-%m-%d"),
                studies=[
                    inv.ExtendedStudyItem(
                        identifier=study.accession_number,
                        submission_date=study.submission_date.strftime("%Y-%m-%d"),
                        public_release_date=study.release_date.strftime("%Y-%m-%d"),
                    )
                ],
            )
            await metadata_service.save_investigation_file(investigation_item)

        return APIResponse(content=investigation_item)


create_endpoint(router)
