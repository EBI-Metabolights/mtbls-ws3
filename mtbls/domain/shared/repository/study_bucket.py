import enum


class StudyBucket(enum.StrEnum):
    PRIVATE_METADATA_FILES = "private_metadata_files"
    DELETED_PRIVATE_METADATA_FILES = "deleted_private_metadata_files"
    PRIVATE_DATA_FILES = "private_data_files"
    DELETED_PRIVATE_DATA_FILES = "deleted_private_data_files"

    INTERNAL_FILES = "internal_files"
    AUDIT_FILES = "audit_files"

    PUBLIC_DATA_FILES = "public_data_files"
    DELETED_PUBLIC_DATA_FILES = "deleted_public_data_files"

    INDICES_CACHE_FILES = "indices_cache_files"

    REPORT_FILES = "report_files"


STUDY_BUCKET_MAP = {x.value: x for x in StudyBucket}
