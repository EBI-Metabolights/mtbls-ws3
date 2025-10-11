from pydantic import BaseModel


class MountedPaths(BaseModel):
    private_metadata_files_root_path: str = ""
    public_studies_root_path: str = ""
    deleted_private_metadata_files_root_path: str = ""

    audit_files_root_path: str = ""
    internal_files_root_path: str = ""

    private_data_files_root_path: str = ""

    deleted_private_data_files_root_path: str = ""
    deleted_public_data_files_root_path: str = ""
    indices_cache_root_path: str = ""
    report_files_path: str = ""


class StudyFolderConfiguration(BaseModel):
    mounted_paths: MountedPaths = MountedPaths()
