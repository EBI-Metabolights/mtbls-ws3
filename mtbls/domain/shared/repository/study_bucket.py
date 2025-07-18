import enum


class StudyBucket(enum.StrEnum):
    PRIVATE_METADATA_FILES = "private_metadata_files"
    PUBLIC_METADATA_FILES = "public_metadata_files"

    INTERNAL_FILES = "internal_files"
    AUDIT_FILES = "audit_files"

    PRIVATE_DATA_FILES = "private_data_files"
    PUBLIC_DATA_FILES = "public_data_files"

    DELETED_UPLOADED_FILES = "deleted_uploaded_files"
    DELETED_PRIVATE_METADATA_FILES = "deleted_private_metadata_files"
    DELETED_PRIVATE_DATA_FILES = "deleted_private_data_files"

    UPLOADED_FILES = "uploaded_files"


STUDY_BUCKET_MAP = {x.value: x for x in StudyBucket}
