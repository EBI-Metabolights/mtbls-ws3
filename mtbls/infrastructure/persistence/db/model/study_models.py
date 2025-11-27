import datetime
from decimal import Decimal

from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    Sequence,
    String,
    Table,
    Text,
    TypeDecorator,
    UniqueConstraint,
    text,
)
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.sql import func

from mtbls.domain.entities.study_file import ResourceCategory
from mtbls.domain.enums.curation_type import CurationType
from mtbls.domain.enums.mhd_share_status import MhdShareStatus
from mtbls.domain.enums.study_category import StudyCategory
from mtbls.domain.enums.study_revision_status import StudyRevisionStatus
from mtbls.domain.enums.study_status import StudyStatus
from mtbls.domain.enums.user_role import UserRole
from mtbls.domain.enums.user_status import UserStatus


class IntEnum(TypeDecorator):
    impl = Integer
    cache_ok = True

    def __init__(self, enumtype, *args, **kwargs):
        super(IntEnum, self).__init__(*args, **kwargs)
        self._enumtype = enumtype

    def process_bind_param(self, value, dialect):
        if isinstance(value, int):
            return value

        return value.value

    def process_result_value(self, value, dialect):
        return self._enumtype(value)


class StrEnum(TypeDecorator):
    impl = String
    cache_ok = True

    def __init__(self, enumtype, *args, **kwargs):
        super(StrEnum, self).__init__(*args, **kwargs)
        self._enumtype = enumtype

    def process_bind_param(self, value, dialect):
        if isinstance(value, self._enumtype):
            return value.value

        return value

    def process_result_value(self, value, dialect):
        return self._enumtype(value)


class Base(AsyncAttrs, DeclarativeBase):
    __none_set__ = frozenset()

    @classmethod
    def get_field_alias(cls, name: str) -> str:
        alias_exceptions = cls.get_field_alias_exceptions()
        if name in alias_exceptions:
            return alias_exceptions[name]

        table_column_names = set(cls.__table__.columns.keys())
        if name not in table_column_names:
            return None
        return name

    @classmethod
    def get_field_alias_exceptions(cls):
        if hasattr(cls, "__field_alias_exceptions__"):
            return cls.__field_alias_exceptions__
        return cls.__none_set__

    @classmethod
    def is_excluded_field(cls, name: str):
        if not name:
            return False
        if hasattr(cls, "__excluded_fields__"):
            return name in cls.__excluded_fields__
        return False


metadata = Base.metadata


hibernate_sequence = Sequence("hibernate_sequence", metadata=metadata)
study_tasks_id_seq = Sequence("study_tasks_id_seq", metadata=metadata)
study_revisions_id_seq = Sequence("study_revisions_id_seq", metadata=metadata)


class StableId(Base):
    __tablename__ = "stableid"
    __field_alias_exceptions__: dict[str, str] = {"id_": "id", "sequence": "seq"}
    __excluded_fields__: set[str] = set()

    id = Column(BigInteger, primary_key=True)
    prefix = Column(String(255))
    seq = Column(BigInteger)


t_study_user = Table(
    "study_user",
    metadata,
    Column("userid", ForeignKey("users.id"), primary_key=True, nullable=False),
    Column("studyid", ForeignKey("studies.id"), primary_key=True, nullable=False),
)


class User(Base):
    __tablename__ = "users"
    __table_args__ = {"sqlite_autoincrement": True}
    __excluded_fields__: set[str] = {"studies"}
    __field_alias_exceptions__: dict[str, str] = {
        "id_": "id",
        "api_token": "apitoken",
        "affiliation_url": "affiliationurl",
        "first_name": "firstname",
        "last_name": "lastname",
        "join_date": "joindate",
        "password_hash": "password",
    }

    id: Mapped[int] = mapped_column(primary_key=True, unique=True, index=True)
    address: Mapped[str] = mapped_column(String(255), nullable=True)
    affiliation: Mapped[str] = mapped_column(String(255), nullable=True)
    affiliationurl: Mapped[str] = mapped_column(String(255), nullable=True)
    apitoken: Mapped[str] = mapped_column(String(255), nullable=False)
    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    firstname: Mapped[str] = mapped_column(String(255), nullable=False)
    joindate: Mapped[datetime.datetime] = mapped_column(
        server_default=func.now(), nullable=False
    )
    lastname: Mapped[str] = mapped_column(String(255), nullable=False)
    password: Mapped[str] = mapped_column(String(255), nullable=False)
    role: Mapped[UserRole] = mapped_column(IntEnum(UserRole), nullable=False)
    status: Mapped[UserStatus] = mapped_column(IntEnum(UserStatus), nullable=False)
    username: Mapped[str] = mapped_column(String(255), nullable=False)
    orcid: Mapped[str] = mapped_column(String(255), nullable=True)
    metaspace_api_key: Mapped[str] = mapped_column(String(255), nullable=True)

    studies = relationship("Study", secondary="study_user", back_populates="submitters")


class StudyRevision(Base):
    __tablename__ = "study_revisions"
    __excluded_fields__: set[str] = {"study"}
    __table_args__ = {"sqlite_autoincrement": True}

    __field_alias_exceptions__: dict[str, str] = {"id_": "id"}

    id: Mapped[int] = mapped_column(
        study_revisions_id_seq, primary_key=True, unique=True, index=True
    )
    accession_number: Mapped[str] = mapped_column(
        String(255),
        ForeignKey("studies.acc", ondelete="CASCADE"),
        nullable=False,
    )
    revision_number: Mapped[int] = mapped_column(BigInteger, nullable=False)
    revision_datetime: Mapped[datetime.datetime] = mapped_column(
        DateTime, nullable=False
    )
    revision_comment: Mapped[str] = mapped_column(String(1024), nullable=False)
    created_by: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[int] = mapped_column(
        IntEnum(StudyRevisionStatus),
        nullable=False,
        default=StudyRevisionStatus.INITIATED,
        server_default=text("0"),
    )
    task_started_at: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=True)
    task_completed_at: Mapped[datetime.datetime] = mapped_column(
        DateTime, nullable=True
    )
    created_by: Mapped[str] = mapped_column(Text, nullable=True)
    mhd_share_status: Mapped[int] = mapped_column(
        IntEnum(MhdShareStatus),
        nullable=True,
        default=MhdShareStatus.INITIATED,
        server_default=text("0"),
    )

    study: Mapped["Study"] = relationship(back_populates="revisions")


class Study(Base):
    __tablename__ = "studies"
    __excluded_fields__: set[str] = {"revisions", "users"}
    __table_args__ = {"sqlite_autoincrement": True}

    __field_alias_exceptions__: dict[str, str] = {
        "id_": "id",
        "accession_number": "acc",
        "obfuscation_code": "obfuscationcode",
        "study_size": "studysize",
        "update_date": "updatedate",
        "submission_date": "submissiondate",
        "study_type": "studytype",
        "release_date": "releasedate",
        "curation_type": "curation_request",
    }

    id: Mapped[int] = mapped_column(primary_key=True, unique=True, index=True)
    acc: Mapped[str] = mapped_column(
        String(255), unique=True, nullable=False, index=True
    )
    obfuscationcode: Mapped[str] = mapped_column(
        String(255), unique=True, nullable=False, index=True
    )
    releasedate: Mapped[datetime.datetime] = mapped_column(nullable=False)
    status: Mapped[StudyStatus] = mapped_column(
        IntEnum(StudyStatus),
        nullable=False,
        default=StudyStatus.DORMANT,
        server_default=text(str(StudyStatus.DORMANT.value)),
    )
    studysize: Mapped[Decimal] = mapped_column(nullable=True)
    updatedate: Mapped[datetime.datetime] = mapped_column(nullable=False)
    submissiondate: Mapped[datetime.datetime] = mapped_column(
        server_default=func.now(),
        nullable=False,
    )
    validations: Mapped[str] = mapped_column(nullable=True)
    studytype: Mapped[str] = mapped_column(String(1000), nullable=True)
    curator: Mapped[str] = mapped_column(nullable=True)
    override: Mapped[str] = mapped_column(nullable=True)
    species: Mapped[str] = mapped_column(nullable=True)
    sample_rows: Mapped[int] = mapped_column(nullable=True)
    assay_rows: Mapped[int] = mapped_column(nullable=True)
    maf_rows: Mapped[int] = mapped_column(nullable=True)
    biostudies_acc: Mapped[str] = mapped_column(
        String(255), unique=False, nullable=True
    )
    placeholder: Mapped[str] = mapped_column(nullable=True)
    validation_status: Mapped[str] = mapped_column(nullable=True)
    status_date: Mapped[datetime.datetime] = mapped_column(nullable=True)
    number_of_files: Mapped[int] = mapped_column(nullable=True)
    comment: Mapped[str] = mapped_column(nullable=True)
    curation_request: Mapped[CurationType] = mapped_column(
        IntEnum(CurationType),
        nullable=False,
        default=CurationType.NO_CURATION,
        server_default=text(str(CurationType.NO_CURATION.value)),
    )
    reserved_accession: Mapped[str] = mapped_column(
        String(50), unique=False, nullable=True
    )
    reserved_submission_id: Mapped[str] = mapped_column(
        String(50), unique=False, nullable=True
    )
    first_public_date: Mapped[datetime.datetime] = mapped_column(nullable=True)
    first_private_date: Mapped[datetime.datetime] = mapped_column(nullable=True)

    dataset_license: Mapped[str] = mapped_column(String(255), nullable=True)
    dataset_license_version: Mapped[str] = mapped_column(String(255), nullable=True)
    dataset_license_agreeing_user: Mapped[str] = mapped_column(
        String(255), nullable=True
    )
    revision_number: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0"), default=0
    )
    revision_datetime: Mapped[datetime.datetime] = mapped_column(nullable=True)
    sample_type: Mapped[str] = mapped_column(
        String(255), nullable=False, server_default=text("minimum"), default="minimum"
    )
    data_policy_agreement: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0"), default=0
    )
    study_category: Mapped[StudyCategory] = mapped_column(
        IntEnum(StudyCategory),
        nullable=False,
        default=StudyCategory.OTHER,
        server_default=text(str(StudyCategory.OTHER.value)),
    )
    template_version: Mapped[str] = mapped_column(
        String(50), nullable=False, server_default=text("1.0"), default="1.0"
    )
    mhd_accession: Mapped[str] = mapped_column(String(50), nullable=True)
    mhd_model_version: Mapped[str] = mapped_column(String(50), nullable=True)

    submitters: Mapped[list[User]] = relationship(
        "User", secondary="study_user", back_populates="studies"
    )
    revisions: Mapped[list["StudyRevision"]] = relationship(
        back_populates="study",
        cascade="all, delete-orphan",
    )
    created_at: Mapped[datetime.datetime] = mapped_column(
        nullable=False, server_default=func.now()
    )
    study_template: Mapped[str] = mapped_column(
        String(50), nullable=False, server_default=text("minimum"), default="minimum"
    )


class Statistic(Base):
    __tablename__ = "ml_stats"
    __table_args__ = {"sqlite_autoincrement": True}
    __excluded_fields__: set[str] = set()

    __field_alias_exceptions__: dict[str, str] = {
        "id_": "id",
        "section": "page_section",
        "name": "str_name",
        "value": "str_value",
    }

    id = Column(Integer, primary_key=True)
    page_section = Column(String(20), nullable=False)
    str_name = Column(String(200), nullable=False)
    str_value = Column(String(200), nullable=False)
    sort_order = Column(BigInteger)


class StudyTasks(Base):
    __tablename__ = "study_tasks"
    __excluded_fields__: set[str] = set()
    __table_args__ = {"sqlite_autoincrement": True}

    __field_alias_exceptions__: dict[str, str] = {
        "id_": "id",
        "study_accession": "study_acc",
    }

    id = Column(Integer, primary_key=True)
    study_acc = Column(String(255), nullable=False)
    task_name = Column(String(255), nullable=False)
    last_request_time = Column(DateTime, nullable=False)
    last_request_executed = Column(DateTime, nullable=False)
    last_execution_time = Column(String(255), nullable=False)
    last_execution_status = Column(String(255), nullable=False)
    last_execution_message = Column(Text)
    (UniqueConstraint("study_acc", "task_name"),)


class StudyFile(Base):
    __tablename__ = "study_files"
    __table_args__ = {"sqlite_autoincrement": True}
    __excluded_fields__: set[str] = set()
    __field_alias_exceptions__: dict[str, str] = {
        "id_": "id",
    }
    id = Column(Integer, hibernate_sequence, primary_key=True)
    bucket_name = Column(String(255), nullable=False)
    resource_id = Column(String(255), nullable=False)
    numeric_resource_id = Column(Integer, nullable=True)
    basename = Column(String(512), nullable=False)
    object_key = Column(String(1024), nullable=False)
    parent_object_key = Column(String(1024), nullable=False)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    size_in_bytes: Mapped[Decimal] = mapped_column(nullable=True)
    size_in_str = Column(String(128), nullable=True)
    is_directory = Column(Integer, nullable=False, default=0)
    is_link = Column(Integer, nullable=False, default=0)
    hashes = Column(String(1024), nullable=True)
    permission_in_oct = Column(String(64), nullable=False, default="")
    extension = Column(String(64), nullable=False, default="")
    category = mapped_column(
        StrEnum(ResourceCategory),
        nullable=False,
        default=ResourceCategory.UNKNOWN_RESOURCE,
    )
    tags = Column(String(1024), nullable=True)
    (UniqueConstraint("bucket_name", "resource_id", "object_key"),)
