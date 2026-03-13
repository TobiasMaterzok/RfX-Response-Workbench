from __future__ import annotations

from enum import StrEnum


class MembershipRole(StrEnum):
    ADMIN = "admin"
    MEMBER = "member"


class CaseStatus(StrEnum):
    ACTIVE = "active"
    PROFILE_FAILED = "profile_failed"


class UploadKind(StrEnum):
    CASE_PDF = "case_pdf"
    QUESTIONNAIRE_XLSX = "questionnaire_xlsx"
    EXPORT_XLSX = "export_xlsx"
    EXPORT_CSV = "export_csv"
    EXPORT_ZIP = "export_zip"
    HISTORICAL_WORKBOOK = "historical_workbook"
    PRODUCT_TRUTH_SOURCE = "product_truth_source"


class ApprovalStatus(StrEnum):
    APPROVED = "approved"
    DRAFT = "draft"
    REJECTED = "rejected"


class MessageRole(StrEnum):
    USER = "user"
    ASSISTANT = "assistant"
    SYSTEM = "system"


class AnswerStatus(StrEnum):
    DRAFT = "draft"
    ACCEPTED = "accepted"


class LLMCaptureStatus(StrEnum):
    CAPTURED = "captured"


class EvidenceSourceKind(StrEnum):
    CASE_PROFILE_ITEM = "case_profile_item"
    HISTORICAL_QA_ROW = "historical_qa_row"
    PRODUCT_TRUTH_CHUNK = "product_truth_chunk"
    PDF_CHUNK = "pdf_chunk"
    PDF_PAGE = "pdf_page"
    QUESTIONNAIRE_ROW = "questionnaire_row"


class ExportStatus(StrEnum):
    PENDING = "pending"
    COMPLETED = "completed"
    FAILED = "failed"


class ExportMode(StrEnum):
    APPROVED_ONLY = "approved_only"
    LATEST_AVAILABLE = "latest_available"


class QuestionnaireRowStatus(StrEnum):
    NOT_STARTED = "not_started"
    RUNNING = "running"
    NEEDS_REVIEW = "needs_review"
    APPROVED = "approved"
    REJECTED = "rejected"
    FAILED = "failed"
    SKIPPED = "skipped"


class BulkFillStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    CANCEL_REQUESTED = "cancel_requested"
    COMPLETED = "completed"
    COMPLETED_WITH_FAILURES = "completed_with_failures"
    FAILED = "failed"
    CANCELLED = "cancelled"
    ORPHANED = "orphaned"


class BulkFillRowStatus(StrEnum):
    NOT_STARTED = "not_started"
    RUNNING = "running"
    DRAFTED = "drafted"
    FAILED = "failed"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"


class BulkFillEventType(StrEnum):
    CREATED = "created"
    RETRY_CREATED = "retry_created"
    RESUME_CREATED = "resume_created"
    CLAIMED = "claimed"
    STARTED = "started"
    HEARTBEAT = "heartbeat"
    PROGRESS = "progress"
    CANCEL_REQUESTED = "cancel_requested"
    CANCELLED = "cancelled"
    ROW_STARTED = "row_started"
    ROW_SUCCEEDED = "row_succeeded"
    ROW_FAILED = "row_failed"
    COMPLETED = "completed"
    FAILED = "failed"
    ORPHANED = "orphaned"


class ReproducibilityMode(StrEnum):
    BEST_EFFORT = "best_effort"
    STRICT_EVAL = "strict_eval"


class ReproducibilityLevel(StrEnum):
    AUDIT_COMPLETE = "audit_complete"
    OPERATIONALLY_REPLAYABLE = "operationally_replayable"
    DETERMINISTIC_NON_LLM = "deterministic_non_llm"
    STRICT_EVAL_ELIGIBLE = "strict_eval_eligible"


class ExecutionRunKind(StrEnum):
    HISTORICAL_IMPORT = "historical_import"
    HISTORICAL_REIMPORT = "historical_reimport"
    PRODUCT_TRUTH_IMPORT = "product_truth_import"
    PRODUCT_TRUTH_REIMPORT = "product_truth_reimport"
    LIVE_CASE_CREATE = "live_case_create"
    LIVE_CASE_REBUILD = "live_case_rebuild"
    RETRIEVAL = "retrieval"
    ROW_DRAFT = "row_draft"
    ROW_REVISION = "row_revision"
    BULK_FILL_JOB = "bulk_fill_job"
    BULK_FILL_ROW_ATTEMPT = "bulk_fill_row_attempt"
    EXPORT = "export"


class ExecutionRunStatus(StrEnum):
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class SourceManifestKind(StrEnum):
    HISTORICAL_IMPORT_SOURCE = "historical_import_source"
    PRODUCT_TRUTH_IMPORT_SOURCE = "product_truth_import_source"
    LIVE_CASE_INPUT = "live_case_input"
    EXPORT_INPUT = "export_input"


class ArtifactBuildKind(StrEnum):
    CURRENT_PDF = "current_pdf"
    CASE_PROFILE = "case_profile"
    HISTORICAL_CORPUS = "historical_corpus"
    PRODUCT_TRUTH_CORPUS = "product_truth_corpus"


class ArtifactBuildStatus(StrEnum):
    ACTIVE = "active"
    REPLACED = "replaced"


class ModelInvocationKind(StrEnum):
    CASE_PROFILE_EXTRACTION = "case_profile_extraction"
    ANSWER_GENERATION = "answer_generation"
    EMBEDDING = "embedding"
