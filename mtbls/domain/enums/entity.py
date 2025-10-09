import enum


class Entity(enum.StrEnum):
    User = "user"
    Study = "study"
    StudyRevision = "study_revision"
    StudyFile = "study_file"
    Statistic = "statistic"
