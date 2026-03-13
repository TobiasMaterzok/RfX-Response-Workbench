from __future__ import annotations

import uuid
from datetime import date, datetime

from sqlalchemy import (
    JSON,
    Boolean,
    Date,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Index,
    Integer,
    LargeBinary,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin, UUIDPrimaryKeyMixin
from app.models.enums import (
    AnswerStatus,
    ApprovalStatus,
    ArtifactBuildKind,
    ArtifactBuildStatus,
    BulkFillEventType,
    BulkFillRowStatus,
    BulkFillStatus,
    CaseStatus,
    EvidenceSourceKind,
    ExecutionRunKind,
    ExecutionRunStatus,
    ExportMode,
    ExportStatus,
    LLMCaptureStatus,
    MembershipRole,
    MessageRole,
    ModelInvocationKind,
    QuestionnaireRowStatus,
    ReproducibilityLevel,
    ReproducibilityMode,
    SourceManifestKind,
    UploadKind,
)
from app.models.vector import EmbeddingVector


def value_enum(enum_cls) -> Enum:  # type: ignore[type-arg]
    return Enum(
        enum_cls,
        native_enum=False,
        values_callable=lambda members: [item.value for item in members],
    )


class User(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "users"

    email: Mapped[str] = mapped_column(String(320), unique=True, nullable=False)
    display_name: Mapped[str] = mapped_column(String(200), nullable=False)

    memberships: Mapped[list[Membership]] = relationship(back_populates="user")


class Tenant(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "tenants"

    name: Mapped[str] = mapped_column(String(200), nullable=False)
    slug: Mapped[str] = mapped_column(String(120), unique=True, nullable=False)

    memberships: Mapped[list[Membership]] = relationship(back_populates="tenant")


class Membership(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "memberships"
    __table_args__ = (UniqueConstraint("tenant_id", "user_id"),)

    tenant_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tenants.id"), nullable=False)
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id"), nullable=False)
    role: Mapped[MembershipRole] = mapped_column(
        value_enum(MembershipRole), nullable=False
    )

    tenant: Mapped[Tenant] = relationship(back_populates="memberships")
    user: Mapped[User] = relationship(back_populates="memberships")


class RfxCase(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "rfx_cases"

    tenant_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tenants.id"), nullable=False)
    created_by_user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id"), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    client_name: Mapped[str | None] = mapped_column(String(255))
    language: Mapped[str] = mapped_column(String(16), default="unknown", nullable=False)
    pipeline_profile_name: Mapped[str | None] = mapped_column(String(120))
    pipeline_config_json: Mapped[dict[str, object] | None] = mapped_column(JSON)
    pipeline_config_hash: Mapped[str | None] = mapped_column(String(64))
    index_config_hash: Mapped[str | None] = mapped_column(String(64))
    creation_run_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("execution_runs.id"))
    current_pdf_build_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("artifact_builds.id"))
    case_profile_build_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("artifact_builds.id"))
    status: Mapped[CaseStatus] = mapped_column(
        value_enum(CaseStatus), default=CaseStatus.ACTIVE, nullable=False
    )

    uploads: Mapped[list[Upload]] = relationship(back_populates="case")
    questionnaires: Mapped[list[Questionnaire]] = relationship(back_populates="case")
    case_profiles: Mapped[list[CaseProfile]] = relationship(back_populates="case")
    chats: Mapped[list[ChatThread]] = relationship(back_populates="case")


class Upload(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "uploads"

    tenant_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tenants.id"), nullable=False)
    case_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("rfx_cases.id"))
    kind: Mapped[UploadKind] = mapped_column(value_enum(UploadKind), nullable=False)
    original_file_name: Mapped[str] = mapped_column(String(255), nullable=False)
    media_type: Mapped[str] = mapped_column(String(120), nullable=False)
    object_key: Mapped[str] = mapped_column(String(512), nullable=False, unique=True)
    file_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    size_bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    payload: Mapped[bytes | None] = mapped_column(LargeBinary)

    case: Mapped[RfxCase | None] = relationship(back_populates="uploads")


class RepoSnapshot(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "repo_snapshots"
    __table_args__ = (UniqueConstraint("git_commit_sha", "git_dirty", "git_diff_hash"),)

    git_commit_sha: Mapped[str] = mapped_column(String(64), nullable=False)
    git_dirty: Mapped[bool] = mapped_column(Boolean, nullable=False)
    git_diff_hash: Mapped[str | None] = mapped_column(String(64))
    git_diff_text: Mapped[str | None] = mapped_column(Text)
    snapshot_hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)


class RuntimeSnapshot(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "runtime_snapshots"
    __table_args__ = (UniqueConstraint("snapshot_hash"),)

    python_version: Mapped[str] = mapped_column(String(64), nullable=False)
    backend_lock_hash: Mapped[str | None] = mapped_column(String(64))
    backend_lock_file: Mapped[str | None] = mapped_column(String(255))
    frontend_lock_hash: Mapped[str | None] = mapped_column(String(64))
    frontend_lock_file: Mapped[str | None] = mapped_column(String(255))
    alembic_head: Mapped[str | None] = mapped_column(String(64))
    db_vendor: Mapped[str | None] = mapped_column(String(64))
    db_version: Mapped[str | None] = mapped_column(String(255))
    pgvector_version: Mapped[str | None] = mapped_column(String(64))
    os_name: Mapped[str | None] = mapped_column(String(128))
    os_arch: Mapped[str | None] = mapped_column(String(64))
    package_versions_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False)
    env_fingerprint_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    env_fingerprint_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False)
    snapshot_hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)


class SourceManifest(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "source_manifests"
    __table_args__ = (UniqueConstraint("manifest_hash"),)

    tenant_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("tenants.id"))
    case_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("rfx_cases.id"))
    kind: Mapped[SourceManifestKind] = mapped_column(
        value_enum(SourceManifestKind), nullable=False
    )
    manifest_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    manifest_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False)


class ExecutionRun(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "execution_runs"

    tenant_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("tenants.id"))
    case_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("rfx_cases.id"))
    user_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("users.id"))
    parent_run_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("execution_runs.id"))
    repo_snapshot_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("repo_snapshots.id"), nullable=False
    )
    runtime_snapshot_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("runtime_snapshots.id"), nullable=False
    )
    source_manifest_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("source_manifests.id")
    )
    kind: Mapped[ExecutionRunKind] = mapped_column(
        value_enum(ExecutionRunKind), nullable=False
    )
    status: Mapped[ExecutionRunStatus] = mapped_column(
        value_enum(ExecutionRunStatus), nullable=False
    )
    reproducibility_level: Mapped[ReproducibilityLevel] = mapped_column(
        value_enum(ReproducibilityLevel), nullable=False
    )
    reproducibility_mode: Mapped[ReproducibilityMode] = mapped_column(
        value_enum(ReproducibilityMode), nullable=False
    )
    pipeline_profile_name: Mapped[str | None] = mapped_column(String(120))
    pipeline_config_hash: Mapped[str | None] = mapped_column(String(64))
    index_config_hash: Mapped[str | None] = mapped_column(String(64))
    runtime_config_hash: Mapped[str | None] = mapped_column(String(64))
    inputs_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False, default=dict)
    outputs_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False, default=dict)
    replay_json: Mapped[dict[str, object] | None] = mapped_column(JSON)
    external_context_json: Mapped[dict[str, object]] = mapped_column(
        JSON, nullable=False, default=dict
    )
    diagnostics_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False, default=dict)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    error_detail: Mapped[str | None] = mapped_column(Text)


class ArtifactBuild(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "artifact_builds"

    tenant_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("tenants.id"))
    case_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("rfx_cases.id"))
    dataset_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("historical_datasets.id"))
    repo_snapshot_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("repo_snapshots.id"), nullable=False
    )
    runtime_snapshot_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("runtime_snapshots.id"), nullable=False
    )
    source_manifest_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("source_manifests.id")
    )
    created_by_run_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("execution_runs.id"), nullable=False
    )
    replaced_build_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("artifact_builds.id"))
    kind: Mapped[ArtifactBuildKind] = mapped_column(
        value_enum(ArtifactBuildKind), nullable=False
    )
    status: Mapped[ArtifactBuildStatus] = mapped_column(
        value_enum(ArtifactBuildStatus), nullable=False
    )
    pipeline_profile_name: Mapped[str | None] = mapped_column(String(120))
    index_config_hash: Mapped[str | None] = mapped_column(String(64))
    compatibility_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    algorithm_version: Mapped[str | None] = mapped_column(String(120))
    tokenizer_identity: Mapped[str | None] = mapped_column(String(120))
    tokenizer_version: Mapped[str | None] = mapped_column(String(64))
    parser_identity: Mapped[str | None] = mapped_column(String(120))
    parser_version: Mapped[str | None] = mapped_column(String(64))
    embedding_model: Mapped[str | None] = mapped_column(String(120))
    metadata_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False, default=dict)


class ModelInvocation(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "model_invocations"

    tenant_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("tenants.id"))
    case_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("rfx_cases.id"))
    execution_run_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("execution_runs.id"), nullable=False
    )
    kind: Mapped[ModelInvocationKind] = mapped_column(
        value_enum(ModelInvocationKind), nullable=False
    )
    provider_name: Mapped[str] = mapped_column(String(64), nullable=False)
    endpoint_kind: Mapped[str] = mapped_column(String(64), nullable=False)
    requested_model_id: Mapped[str | None] = mapped_column(String(255))
    actual_model_id: Mapped[str | None] = mapped_column(String(255))
    reasoning_effort: Mapped[str | None] = mapped_column(String(32))
    temperature: Mapped[float | None] = mapped_column(Float)
    embedding_model_id: Mapped[str | None] = mapped_column(String(255))
    tokenizer_identity: Mapped[str | None] = mapped_column(String(120))
    tokenizer_version: Mapped[str | None] = mapped_column(String(64))
    sdk_version: Mapped[str | None] = mapped_column(String(64))
    request_payload_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    request_payload_text: Mapped[str] = mapped_column(Text, nullable=False)
    request_artifact_key: Mapped[str] = mapped_column(String(512), nullable=False)
    response_payload_hash: Mapped[str | None] = mapped_column(String(64))
    response_payload_text: Mapped[str | None] = mapped_column(Text)
    response_artifact_key: Mapped[str | None] = mapped_column(String(512))
    provider_response_id: Mapped[str | None] = mapped_column(String(255))
    remote_store: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    service_tier: Mapped[str | None] = mapped_column(String(64))
    usage_json: Mapped[dict[str, object] | None] = mapped_column(JSON)
    metadata_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False, default=dict)


class HistoricalDataset(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "historical_datasets"
    __table_args__ = (UniqueConstraint("tenant_id", "slug"),)

    tenant_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tenants.id"), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    slug: Mapped[str] = mapped_column(String(120), nullable=False)
    schema_version: Mapped[str] = mapped_column(String(64), nullable=False)
    creation_run_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("execution_runs.id"))
    artifact_build_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("artifact_builds.id"))
    approval_status: Mapped[ApprovalStatus] = mapped_column(
        value_enum(ApprovalStatus), nullable=False
    )

    workbooks: Mapped[list[HistoricalWorkbook]] = relationship(back_populates="dataset")


class HistoricalWorkbook(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "historical_workbooks"
    __table_args__ = (UniqueConstraint("dataset_id", "source_file_name"),)

    tenant_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tenants.id"), nullable=False)
    dataset_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("historical_datasets.id"), nullable=False
    )
    upload_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("uploads.id"))
    client_name: Mapped[str] = mapped_column(String(255), nullable=False)
    language: Mapped[str] = mapped_column(String(16), nullable=False)
    source_file_name: Mapped[str] = mapped_column(String(255), nullable=False)
    source_sheet_name: Mapped[str] = mapped_column(String(255), nullable=False)
    file_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    schema_version: Mapped[str] = mapped_column(String(64), nullable=False)
    approval_status: Mapped[ApprovalStatus] = mapped_column(
        value_enum(ApprovalStatus), nullable=False
    )

    dataset: Mapped[HistoricalDataset] = relationship(back_populates="workbooks")
    client_package: Mapped[HistoricalClientPackage | None] = relationship(
        back_populates="workbook"
    )
    rows: Mapped[list[HistoricalQARow]] = relationship(back_populates="workbook")


class HistoricalClientPackage(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "historical_client_packages"
    __table_args__ = (
        UniqueConstraint("dataset_id", "client_slug"),
        UniqueConstraint("workbook_id"),
    )

    tenant_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tenants.id"), nullable=False)
    dataset_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("historical_datasets.id"), nullable=False
    )
    workbook_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("historical_workbooks.id"), nullable=False
    )
    client_slug: Mapped[str] = mapped_column(String(120), nullable=False)
    client_name: Mapped[str] = mapped_column(String(255), nullable=False)
    language: Mapped[str] = mapped_column(String(16), nullable=False)
    source_pdf_file_name: Mapped[str] = mapped_column(String(255), nullable=False)
    source_pdf_file_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    pdf_object_key: Mapped[str] = mapped_column(String(512), nullable=False, unique=True)
    pdf_size_bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    pipeline_profile_name: Mapped[str | None] = mapped_column(String(120))
    index_config_json: Mapped[dict[str, object] | None] = mapped_column(JSON)
    index_config_hash: Mapped[str | None] = mapped_column(String(64))

    workbook: Mapped[HistoricalWorkbook] = relationship(back_populates="client_package")
    rows: Mapped[list[HistoricalQARow]] = relationship(back_populates="client_package")
    case_profile: Mapped[HistoricalCaseProfile | None] = relationship(
        back_populates="client_package"
    )


class HistoricalQARow(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "historical_qa_rows"
    __table_args__ = (UniqueConstraint("workbook_id", "source_row_id"),)

    tenant_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tenants.id"), nullable=False)
    dataset_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("historical_datasets.id"), nullable=False
    )
    workbook_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("historical_workbooks.id"), nullable=False
    )
    client_package_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("historical_client_packages.id")
    )
    historical_case_profile_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("historical_case_profiles.id")
    )
    client_name: Mapped[str] = mapped_column(String(255), nullable=False)
    source_file_name: Mapped[str] = mapped_column(String(255), nullable=False)
    source_sheet_name: Mapped[str] = mapped_column(String(255), nullable=False)
    source_row_number: Mapped[int] = mapped_column(Integer, nullable=False)
    source_row_id: Mapped[str] = mapped_column(String(255), nullable=False)
    language: Mapped[str] = mapped_column(String(16), nullable=False)
    language_confidence: Mapped[float] = mapped_column(Float, nullable=False)
    approval_status: Mapped[ApprovalStatus] = mapped_column(
        value_enum(ApprovalStatus), nullable=False
    )
    context_raw: Mapped[str] = mapped_column(Text, nullable=False)
    question_raw: Mapped[str] = mapped_column(Text, nullable=False)
    answer_raw: Mapped[str] = mapped_column(Text, nullable=False)
    normalized_text: Mapped[str] = mapped_column(Text, nullable=False)
    file_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    schema_version: Mapped[str] = mapped_column(String(64), nullable=False)
    embedding: Mapped[list[float] | None] = mapped_column(EmbeddingVector())

    workbook: Mapped[HistoricalWorkbook] = relationship(back_populates="rows")
    client_package: Mapped[HistoricalClientPackage | None] = relationship(back_populates="rows")
    historical_case_profile: Mapped[HistoricalCaseProfile | None] = relationship()


class HistoricalCaseProfile(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "historical_case_profiles"
    __table_args__ = (UniqueConstraint("client_package_id"),)

    tenant_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tenants.id"), nullable=False)
    client_package_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("historical_client_packages.id"), nullable=False
    )
    schema_version: Mapped[str] = mapped_column(String(64), nullable=False)
    prompt_set_version: Mapped[str] = mapped_column(String(64), nullable=False)
    model: Mapped[str] = mapped_column(String(120), nullable=False)
    summary: Mapped[str] = mapped_column(Text, nullable=False)
    source_file_name: Mapped[str] = mapped_column(String(255), nullable=False)
    source_file_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    language: Mapped[str] = mapped_column(String(16), nullable=False)
    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    signature_version: Mapped[str] = mapped_column(String(64), nullable=False)
    signature_embedding_model: Mapped[str] = mapped_column(String(120), nullable=False)
    signature_fields_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False)
    signature_text: Mapped[str] = mapped_column(Text, nullable=False)
    signature_embedding: Mapped[list[float] | None] = mapped_column(EmbeddingVector())
    document: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False)

    client_package: Mapped[HistoricalClientPackage] = relationship(back_populates="case_profile")
    items: Mapped[list[HistoricalCaseProfileItem]] = relationship(back_populates="case_profile")


class HistoricalCaseProfileItem(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "historical_case_profile_items"
    __table_args__ = (UniqueConstraint("historical_case_profile_id", "analysis_item_id"),)

    tenant_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tenants.id"), nullable=False)
    historical_case_profile_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("historical_case_profiles.id"), nullable=False
    )
    analysis_item_id: Mapped[str] = mapped_column(String(80), nullable=False)
    position: Mapped[int] = mapped_column(Integer, nullable=False)
    prompt: Mapped[str] = mapped_column(Text, nullable=False)
    answer: Mapped[str] = mapped_column(Text, nullable=False)
    confidence: Mapped[str] = mapped_column(String(16), nullable=False)
    citations: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    normalized_text: Mapped[str] = mapped_column(Text, nullable=False)
    embedding: Mapped[list[float] | None] = mapped_column(EmbeddingVector())

    case_profile: Mapped[HistoricalCaseProfile] = relationship(back_populates="items")


class ProductTruthRecord(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "product_truth_records"

    tenant_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tenants.id"), nullable=False)
    product_area: Mapped[str] = mapped_column(String(255), nullable=False)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    body: Mapped[str] = mapped_column(Text, nullable=False)
    language: Mapped[str] = mapped_column(String(16), nullable=False)
    source_file_name: Mapped[str] = mapped_column(String(255), nullable=False)
    source_section: Mapped[str] = mapped_column(String(255), nullable=False)
    effective_from: Mapped[date] = mapped_column(Date, nullable=False)
    effective_to: Mapped[date | None] = mapped_column(Date)
    version: Mapped[str] = mapped_column(String(64), nullable=False)
    pipeline_profile_name: Mapped[str | None] = mapped_column(String(120))
    index_config_json: Mapped[dict[str, object] | None] = mapped_column(JSON)
    index_config_hash: Mapped[str | None] = mapped_column(String(64))
    artifact_build_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("artifact_builds.id"))
    approval_status: Mapped[ApprovalStatus] = mapped_column(
        value_enum(ApprovalStatus), nullable=False
    )
    file_hash: Mapped[str] = mapped_column(String(64), nullable=False)

    chunks: Mapped[list[ProductTruthChunk]] = relationship(back_populates="truth_record")


class ProductTruthChunk(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "product_truth_chunks"
    __table_args__ = (UniqueConstraint("truth_record_id", "chunk_index"),)

    tenant_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tenants.id"), nullable=False)
    truth_record_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("product_truth_records.id"), nullable=False
    )
    chunk_index: Mapped[int] = mapped_column(Integer, nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    language: Mapped[str] = mapped_column(String(16), nullable=False)
    file_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    approval_status: Mapped[ApprovalStatus] = mapped_column(
        value_enum(ApprovalStatus), nullable=False
    )
    embedding: Mapped[list[float] | None] = mapped_column(EmbeddingVector())

    truth_record: Mapped[ProductTruthRecord] = relationship(back_populates="chunks")


class PdfPage(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "pdf_pages"
    __table_args__ = (UniqueConstraint("upload_id", "page_number"),)

    tenant_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tenants.id"), nullable=False)
    case_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("rfx_cases.id"), nullable=False)
    upload_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("uploads.id"), nullable=False)
    page_number: Mapped[int] = mapped_column(Integer, nullable=False)
    extracted_text: Mapped[str] = mapped_column(Text, nullable=False)
    text_hash: Mapped[str] = mapped_column(String(64), nullable=False)


class PdfChunk(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "pdf_chunks"
    __table_args__ = (
        UniqueConstraint("upload_id", "page_number", "chunk_index"),
        Index("ix_pdf_chunks_tenant_case", "tenant_id", "case_id"),
        Index("ix_pdf_chunks_upload_page", "upload_id", "page_number"),
    )

    tenant_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tenants.id"), nullable=False)
    case_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("rfx_cases.id"), nullable=False)
    upload_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("uploads.id"), nullable=False)
    page_number: Mapped[int] = mapped_column(Integer, nullable=False)
    chunk_index: Mapped[int] = mapped_column(Integer, nullable=False)
    start_offset: Mapped[int] = mapped_column(Integer, nullable=False)
    end_offset: Mapped[int] = mapped_column(Integer, nullable=False)
    chunking_version: Mapped[str] = mapped_column(String(64), nullable=False)
    embedding_model: Mapped[str] = mapped_column(String(120), nullable=False)
    index_config_hash: Mapped[str | None] = mapped_column(String(64))
    artifact_build_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("artifact_builds.id"))
    chunk_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    embedding: Mapped[list[float] | None] = mapped_column(EmbeddingVector())


class CaseProfile(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "case_profiles"
    __table_args__ = (UniqueConstraint("case_id", "prompt_set_version"),)

    tenant_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tenants.id"), nullable=False)
    case_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("rfx_cases.id"), nullable=False)
    source_pdf_upload_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("uploads.id"), nullable=False
    )
    schema_version: Mapped[str] = mapped_column(String(64), nullable=False)
    prompt_set_version: Mapped[str] = mapped_column(String(64), nullable=False)
    model: Mapped[str] = mapped_column(String(120), nullable=False)
    summary: Mapped[str] = mapped_column(Text, nullable=False)
    source_file_name: Mapped[str] = mapped_column(String(255), nullable=False)
    source_file_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    language: Mapped[str] = mapped_column(String(16), nullable=False)
    pipeline_profile_name: Mapped[str | None] = mapped_column(String(120))
    index_config_json: Mapped[dict[str, object] | None] = mapped_column(JSON)
    index_config_hash: Mapped[str | None] = mapped_column(String(64))
    artifact_build_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("artifact_builds.id"))
    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    document: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False)

    case: Mapped[RfxCase] = relationship(back_populates="case_profiles")
    items: Mapped[list[CaseProfileItem]] = relationship(back_populates="case_profile")


class CaseProfileItem(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "case_profile_items"
    __table_args__ = (UniqueConstraint("case_profile_id", "analysis_item_id"),)

    tenant_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tenants.id"), nullable=False)
    case_profile_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("case_profiles.id"), nullable=False
    )
    case_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("rfx_cases.id"), nullable=False)
    analysis_item_id: Mapped[str] = mapped_column(String(80), nullable=False)
    position: Mapped[int] = mapped_column(Integer, nullable=False)
    prompt: Mapped[str] = mapped_column(Text, nullable=False)
    answer: Mapped[str] = mapped_column(Text, nullable=False)
    confidence: Mapped[str] = mapped_column(String(16), nullable=False)
    citations: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    normalized_text: Mapped[str] = mapped_column(Text, nullable=False)
    embedding: Mapped[list[float] | None] = mapped_column(EmbeddingVector())

    case_profile: Mapped[CaseProfile] = relationship(back_populates="items")


class Questionnaire(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "questionnaires"
    __table_args__ = (UniqueConstraint("case_id"),)

    tenant_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tenants.id"), nullable=False)
    case_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("rfx_cases.id"), nullable=False)
    upload_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("uploads.id"), nullable=False)
    source_file_name: Mapped[str] = mapped_column(String(255), nullable=False)
    source_sheet_name: Mapped[str] = mapped_column(String(255), nullable=False)
    file_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    schema_version: Mapped[str] = mapped_column(String(64), nullable=False)

    case: Mapped[RfxCase] = relationship(back_populates="questionnaires")
    rows: Mapped[list[QuestionnaireRow]] = relationship(back_populates="questionnaire")


class QuestionnaireRow(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "questionnaire_rows"
    __table_args__ = (UniqueConstraint("questionnaire_id", "source_row_id"),)

    tenant_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tenants.id"), nullable=False)
    case_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("rfx_cases.id"), nullable=False)
    questionnaire_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("questionnaires.id"), nullable=False
    )
    source_sheet_name: Mapped[str] = mapped_column(String(255), nullable=False)
    source_row_number: Mapped[int] = mapped_column(Integer, nullable=False)
    source_row_id: Mapped[str] = mapped_column(String(255), nullable=False)
    context_raw: Mapped[str] = mapped_column(Text, nullable=False)
    question_raw: Mapped[str] = mapped_column(Text, nullable=False)
    answer_raw: Mapped[str] = mapped_column(Text, nullable=False, default="")
    normalized_text: Mapped[str] = mapped_column(Text, nullable=False)
    review_status: Mapped[QuestionnaireRowStatus] = mapped_column(
        value_enum(QuestionnaireRowStatus),
        default=QuestionnaireRowStatus.NOT_STARTED,
        nullable=False,
    )
    approved_answer_version_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("answer_versions.id")
    )
    last_error_detail: Mapped[str | None] = mapped_column(Text)

    questionnaire: Mapped[Questionnaire] = relationship(back_populates="rows")
    chats: Mapped[list[ChatThread]] = relationship(back_populates="questionnaire_row")


class ChatThread(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "chat_threads"

    tenant_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tenants.id"), nullable=False)
    case_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("rfx_cases.id"), nullable=False)
    questionnaire_row_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("questionnaire_rows.id"), nullable=False
    )
    created_by_user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id"), nullable=False)
    title: Mapped[str] = mapped_column(String(255), nullable=False)

    case: Mapped[RfxCase] = relationship(back_populates="chats")
    questionnaire_row: Mapped[QuestionnaireRow] = relationship(back_populates="chats")
    messages: Mapped[list[ChatMessage]] = relationship(back_populates="thread")


class RetrievalRun(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "retrieval_runs"

    tenant_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tenants.id"), nullable=False)
    case_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("rfx_cases.id"), nullable=False)
    questionnaire_row_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("questionnaire_rows.id"), nullable=False
    )
    chat_thread_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("chat_threads.id"), nullable=False)
    execution_run_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("execution_runs.id"))
    query_text: Mapped[str] = mapped_column(Text, nullable=False)
    request_context: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False)
    prompt_authority_order: Mapped[list[str]] = mapped_column(JSON, nullable=False)

    evidence_items: Mapped[list[RetrievalSnapshotItem]] = relationship(
        back_populates="retrieval_run"
    )


class RetrievalSnapshotItem(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "retrieval_snapshot_items"
    __table_args__ = (UniqueConstraint("retrieval_run_id", "rank"),)

    tenant_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tenants.id"), nullable=False)
    case_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("rfx_cases.id"), nullable=False)
    retrieval_run_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("retrieval_runs.id"), nullable=False
    )
    source_kind: Mapped[EvidenceSourceKind] = mapped_column(
        value_enum(EvidenceSourceKind), nullable=False
    )
    source_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    source_label: Mapped[str] = mapped_column(String(255), nullable=False)
    source_title: Mapped[str] = mapped_column(String(255), nullable=False)
    excerpt: Mapped[str] = mapped_column(Text, nullable=False)
    metadata_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False)
    score: Mapped[float] = mapped_column(Float, nullable=False)
    rank: Mapped[int] = mapped_column(Integer, nullable=False)

    retrieval_run: Mapped[RetrievalRun] = relationship(back_populates="evidence_items")
    answer_links: Mapped[list[EvidenceLink]] = relationship(back_populates="snapshot_item")


class AnswerVersion(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "answer_versions"
    __table_args__ = (UniqueConstraint("questionnaire_row_id", "version_number"),)

    tenant_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tenants.id"), nullable=False)
    case_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("rfx_cases.id"), nullable=False)
    questionnaire_row_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("questionnaire_rows.id"), nullable=False
    )
    chat_thread_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("chat_threads.id"), nullable=False)
    retrieval_run_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("retrieval_runs.id"), nullable=False
    )
    execution_run_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("execution_runs.id"))
    model_invocation_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("model_invocations.id")
    )
    version_number: Mapped[int] = mapped_column(Integer, nullable=False)
    answer_text: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[AnswerStatus] = mapped_column(
        value_enum(AnswerStatus), nullable=False
    )
    model: Mapped[str] = mapped_column(String(120), nullable=False)
    prompt_version: Mapped[str] = mapped_column(String(64), nullable=False)
    llm_capture_status: Mapped[LLMCaptureStatus] = mapped_column(
        value_enum(LLMCaptureStatus),
        nullable=False,
    )
    llm_request_text: Mapped[str | None] = mapped_column(Text)
    llm_response_text: Mapped[str | None] = mapped_column(Text)

    evidence_links: Mapped[list[EvidenceLink]] = relationship(back_populates="answer_version")


class EvidenceLink(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "evidence_links"
    __table_args__ = (UniqueConstraint("answer_version_id", "snapshot_item_id"),)

    answer_version_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("answer_versions.id"), nullable=False
    )
    retrieval_run_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("retrieval_runs.id"), nullable=False
    )
    snapshot_item_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("retrieval_snapshot_items.id"), nullable=False
    )

    answer_version: Mapped[AnswerVersion] = relationship(back_populates="evidence_links")
    snapshot_item: Mapped[RetrievalSnapshotItem] = relationship(back_populates="answer_links")


class ChatMessage(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "chat_messages"

    tenant_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tenants.id"), nullable=False)
    case_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("rfx_cases.id"), nullable=False)
    questionnaire_row_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("questionnaire_rows.id"), nullable=False
    )
    thread_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("chat_threads.id"), nullable=False)
    role: Mapped[MessageRole] = mapped_column(value_enum(MessageRole), nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    answer_version_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("answer_versions.id"))
    retrieval_run_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("retrieval_runs.id"))

    thread: Mapped[ChatThread] = relationship(back_populates="messages")


class ExportJob(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "export_jobs"

    tenant_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tenants.id"), nullable=False)
    case_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("rfx_cases.id"), nullable=False)
    questionnaire_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("questionnaires.id"), nullable=False
    )
    source_upload_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("uploads.id"), nullable=False)
    output_upload_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("uploads.id"))
    execution_run_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("execution_runs.id"))
    export_mode: Mapped[ExportMode] = mapped_column(
        value_enum(ExportMode),
        nullable=False,
    )
    status: Mapped[ExportStatus] = mapped_column(
        value_enum(ExportStatus), nullable=False
    )
    row_mapping_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False)
    metadata_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False, default=dict)
    error_detail: Mapped[str | None] = mapped_column(Text)


class BulkFillRequest(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "bulk_fill_requests"

    tenant_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tenants.id"), nullable=False)
    case_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("rfx_cases.id"), nullable=False)
    questionnaire_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("questionnaires.id"), nullable=False
    )
    execution_run_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("execution_runs.id"))
    parent_request_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("bulk_fill_requests.id")
    )
    requested_by_user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id"), nullable=False)
    status: Mapped[BulkFillStatus] = mapped_column(
        value_enum(BulkFillStatus), nullable=False
    )
    claim_id: Mapped[str | None] = mapped_column(String(64))
    runner_id: Mapped[str | None] = mapped_column(String(255))
    execution_mode: Mapped[str | None] = mapped_column(String(64))
    claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    cancel_requested_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    stale_detected_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    config_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False)
    summary_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False, default=dict)
    error_detail: Mapped[str | None] = mapped_column(Text)


class BulkFillRowExecution(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "bulk_fill_row_executions"
    __table_args__ = (UniqueConstraint("bulk_fill_request_id", "questionnaire_row_id", "attempt_number"),)

    tenant_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tenants.id"), nullable=False)
    case_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("rfx_cases.id"), nullable=False)
    bulk_fill_request_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("bulk_fill_requests.id"), nullable=False
    )
    execution_run_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("execution_runs.id"))
    questionnaire_row_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("questionnaire_rows.id"), nullable=False
    )
    answer_version_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("answer_versions.id"))
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[BulkFillRowStatus] = mapped_column(
        value_enum(BulkFillRowStatus), nullable=False
    )
    diagnostics_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False, default=dict)
    error_detail: Mapped[str | None] = mapped_column(Text)


class BulkFillJobEvent(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "bulk_fill_job_events"

    tenant_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tenants.id"), nullable=False)
    case_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("rfx_cases.id"), nullable=False)
    bulk_fill_request_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("bulk_fill_requests.id"), nullable=False
    )
    bulk_fill_row_execution_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("bulk_fill_row_executions.id")
    )
    event_type: Mapped[BulkFillEventType] = mapped_column(
        value_enum(BulkFillEventType), nullable=False
    )
    runner_id: Mapped[str | None] = mapped_column(String(255))
    message: Mapped[str | None] = mapped_column(Text)
    metadata_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False, default=dict)
