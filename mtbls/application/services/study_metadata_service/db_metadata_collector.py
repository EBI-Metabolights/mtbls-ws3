from metabolights_utils.models.common import ErrorMessage
from metabolights_utils.models.metabolights import model
from metabolights_utils.models.metabolights import model as mtbls_utils_model
from metabolights_utils.provider.async_provider.study_provider import (
    AbstractDbMetadataCollector,
)

from mtbls.application.services.interfaces.repositories.study.study_read_repository import (  # noqa: E501
    StudyReadRepository,
)
from mtbls.application.services.interfaces.repositories.user.user_read_repository import (  # noqa: E501
    UserReadRepository,
)
from mtbls.domain.entities.user import UserOutput
from mtbls.domain.enums.curation_type import CurationType
from mtbls.domain.enums.study_status import StudyStatus
from mtbls.domain.enums.user_role import UserRole
from mtbls.domain.enums.user_status import UserStatus

STUDY_STATUS_MAP = {
    StudyStatus.PROVISIONAL: model.StudyStatus.SUBMITTED,
    StudyStatus.PRIVATE: model.StudyStatus.INCURATION,
    StudyStatus.INREVIEW: model.StudyStatus.INREVIEW,
    StudyStatus.PUBLIC: model.StudyStatus.PUBLIC,
    StudyStatus.DORMANT: model.StudyStatus.DORMANT,
    None: model.StudyStatus.DORMANT,
}
CURATION_REQUEST_MAP = {
    CurationType.NO_CURATION: model.CurationRequest.NO_CURATION,
    CurationType.MANUAL_CURATION: model.CurationRequest.MANUAL_CURATION,
    CurationType.SEMI_AUTOMATED_CURATION: model.CurationRequest.SEMI_AUTOMATED_CURATION,
    None: model.CurationRequest.NO_CURATION,
}
USER_STATUS_MAP = {
    UserStatus.ACTIVE: model.UserStatus.ACTIVE,
    UserStatus.FROZEN: model.UserStatus.FROZEN,
    UserStatus.NEW: model.UserStatus.NEW,
    UserStatus.VERIFIED: model.UserStatus.VERIFIED,
    None: model.UserStatus.NEW,
}
USER_ROLE_MAP = {
    UserRole.ANONYMOUS: model.UserRole.ANONYMOUS,
    UserRole.CURATOR: model.UserRole.CURATOR,
    UserRole.SUBMITTER: model.UserRole.SUBMITTER,
    UserRole.SYSTEM_ADMIN: model.UserRole.CURATOR,
    UserRole.REVIEWER: model.UserRole.ANONYMOUS,
    None: model.UserRole.ANONYMOUS,
}


class DefaultAsyncDbMetadataCollector(AbstractDbMetadataCollector):
    def __init__(
        self,
        study_read_repository: StudyReadRepository,
        user_read_repository: UserReadRepository,
    ):
        self.study_read_repository = study_read_repository
        self.user_read_repository = user_read_repository

    async def get_study_metadata_from_db(
        self, resource_id: str, connection
    ) -> tuple[model.StudyDBMetadata, list[ErrorMessage]]:
        try:
            study = await self.study_read_repository.get_study_by_accession(resource_id)
            metadata = model.StudyDBMetadata(
                db_id=study.id_,
                study_id=study.accession_number,
                numeric_study_id=int(
                    study.accession_number.replace("MTBLS", "").replace("REQ", "")
                ),
                submission_date=study.submission_date.strftime("%Y-%m-%d")
                if study.submission_date
                else "",
                release_date=study.release_date.strftime("%Y-%m-%d")
                if study.release_date
                else "",
                update_date=study.update_date.isoformat() if study.update_date else "",
                status_date=study.status_date.isoformat() if study.status_date else "",
                obfuscation_code=study.obfuscation_code,
                study_size=study.study_size,
                study_types=study.study_type.split(";") if study.study_type else [],
                status=STUDY_STATUS_MAP[study.status],
                curation_request=CURATION_REQUEST_MAP[study.curation_type],
                study_category=mtbls_utils_model.StudyCategory(
                    study.study_category.value
                ),
                sample_template=study.sample_type or "",
                template_version=study.template_version or "",
                reserved_mhd_accession=study.mhd_accession or "",
                reserved_accession=study.reserved_accession or "",
                reserved_submission_id=study.reserved_submission_id or "",
                mhd_model_version=study.mhd_model_version or "",
                dataset_license=study.dataset_license or "",
                dataset_license_version=study.dataset_license_version or "",
                first_public_date=study.first_public_date.isoformat()
                if study.first_public_date
                else "",
                first_private_date=study.first_private_date.isoformat()
                if study.first_private_date
                else "",
                revision_number=study.revision_number or 0,
                revision_date=study.revision_datetime.isoformat()
                if study.revision_datetime
                else "",
            )
            submitters: list[
                UserOutput
            ] = await self.user_read_repository.get_study_submitters_by_accession(
                resource_id
            )

            for submitter in submitters:
                metadata.submitters.append(
                    model.Submitter(
                        db_id=submitter.id_,
                        orcid=submitter.orcid,
                        address=submitter.address,
                        join_date=submitter.join_date.isoformat(),
                        user_name=submitter.username,
                        first_name=submitter.first_name,
                        last_name=submitter.last_name,
                        affiliation=submitter.affiliation,
                        affiliation_url=submitter.affiliation_url,
                        status=USER_STATUS_MAP[submitter.status],
                        submitter_role=USER_ROLE_MAP[submitter.role],
                    )
                )
            return metadata, []
        except Exception as ex:
            return model.StudyDBMetadata(), [
                ErrorMessage(short="Error while loading db metadata", detail=str(ex))
            ]
