import enum


class Entity(enum.StrEnum):
    User = "user"
    Study = "study"
    StudyRevision = "study_revision"
    StudyDataFile = "study_data_file"
    Statistic = "statistic"
    Compound = "compound"
    MtblsDataReuse = "mtbls_data_reuse"
