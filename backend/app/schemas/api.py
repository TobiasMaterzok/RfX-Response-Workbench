from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field

ThreadStateLiteral = Literal[
    "none",
    "answer_available",
    "failed_no_answer",
    "pending_no_answer",
]


class SessionContextResponse(BaseModel):
    tenant_id: UUID
    tenant_slug: str
    tenant_name: str
    user_id: UUID
    user_email: str
    user_name: str


class PipelineConfigResponse(BaseModel):
    profile_name: str
    config_hash: str
    index_config_hash: str
    runtime_config_hash: str
    artifact_index_hashes: dict[str, str]
    config: dict[str, object]
    config_schema: dict[str, object]


class CaseSummaryResponse(BaseModel):
    id: UUID
    name: str
    client_name: str | None
    language: str
    status: str
    created_at: datetime
    updated_at: datetime


class QuestionnaireRowResponse(BaseModel):
    id: UUID
    source_row_id: str
    source_row_number: int
    context: str
    question: str
    current_answer: str
    review_status: str
    approved_answer_version_id: UUID | None
    approved_answer_text: str | None
    last_error_detail: str | None
    last_bulk_fill_request_id: UUID | None = None
    last_bulk_fill_row_execution_id: UUID | None = None
    last_bulk_fill_status: str | None = None
    last_bulk_fill_attempt_number: int | None = None
    latest_attempt_thread_id: UUID | None = None
    latest_attempt_state: ThreadStateLiteral = "none"


class ChatThreadResponse(BaseModel):
    id: UUID
    questionnaire_row_id: UUID
    title: str
    updated_at: datetime


class CaseProfileSummaryResponse(BaseModel):
    schema_version: str
    prompt_set_version: str
    summary: str
    generated_at: datetime


class CaseDetailResponse(BaseModel):
    id: UUID
    name: str
    client_name: str | None
    language: str
    status: str
    created_at: datetime
    updated_at: datetime
    profile: CaseProfileSummaryResponse | None
    latest_bulk_fill: BulkFillSummaryResponse | None = None
    bulk_fill_history: list[BulkFillSummaryResponse] = []
    questionnaire_rows: list[QuestionnaireRowResponse]
    chats: list[ChatThreadResponse]


class EvidenceResponse(BaseModel):
    id: UUID
    source_kind: Literal[
        "case_profile_item",
        "historical_qa_row",
        "product_truth_chunk",
        "pdf_chunk",
        "pdf_page",
    ]
    source_label: str
    source_title: str
    excerpt: str
    score: float
    metadata: dict[str, object]


class RetrievalStageResponse(BaseModel):
    corpus: str
    stage: str
    mode: str
    candidate_count: int
    broadened: bool
    skipped: bool
    reason: str | None = None


class RetrievalSummaryResponse(BaseModel):
    strategy_version: str
    pipeline_profile_name: str | None
    pipeline_config_hash: str
    index_config_hash: str
    revision_mode: str
    revision_classifier_version: str | None
    revision_reason: str | None
    retrieval_action: str
    retrieval_action_reason: str | None
    reused_from_retrieval_run_id: UUID | None
    candidate_generation_mode: str
    broadened: bool
    sufficiency: str
    degraded: bool
    notes: list[str]
    stages: list[RetrievalStageResponse]


class AnswerVersionResponse(BaseModel):
    id: UUID
    chat_thread_id: UUID
    retrieval_run_id: UUID
    version_number: int
    answer_text: str
    status: str
    pipeline_profile_name: str | None
    pipeline_config_hash: str
    created_at: datetime
    model: str
    generation_path: str
    llm_capture_stage: str | None
    prompt_version: str
    llm_capture_status: str
    llm_request_text: str | None
    llm_response_text: str | None


class ChatMessageResponse(BaseModel):
    id: UUID
    role: str
    content: str
    created_at: datetime
    answer_version_id: UUID | None = None


class DraftRequest(BaseModel):
    message: str = Field(min_length=1)
    thread_id: UUID | None = None
    pipeline_profile: str | None = None
    pipeline_override: dict[str, object] | None = None
    reproducibility_mode: Literal["best_effort", "strict_eval"] = "best_effort"
    revision_mode_override: Literal["style_only", "content_change"] | None = None


class DraftResponse(BaseModel):
    thread: ChatThreadResponse
    messages: list[ChatMessageResponse]
    answer_version: AnswerVersionResponse
    retrieval: RetrievalSummaryResponse
    evidence: list[EvidenceResponse]


class ThreadDetailResponse(BaseModel):
    thread: ChatThreadResponse
    thread_state: ThreadStateLiteral
    messages: list[ChatMessageResponse]
    answer_version: AnswerVersionResponse | None
    retrieval: RetrievalSummaryResponse | None
    evidence: list[EvidenceResponse]
    failure_detail: str | None


RawTraceScopeLiteral = Literal["selected_answer_version", "latest_attempt"]
RawTraceAvailabilityLiteral = Literal["available", "missing"]


class RawTraceStageResponse(BaseModel):
    availability: RawTraceAvailabilityLiteral
    source_type: str | None
    source_execution_run_id: UUID | None
    source_answer_version_id: UUID | None
    model_invocation_id: UUID | None
    prompt_family: str | None
    prompt_version: str | None
    requested_model_id: str | None
    actual_model_id: str | None
    reasoning_effort: str | None
    temperature: float | None
    provider_response_id: str | None
    service_tier: str | None
    usage_json: dict[str, object] | None
    request_payload_text: str | None
    response_payload_text: str | None


class RawTraceResponse(BaseModel):
    scope: RawTraceScopeLiteral
    row_id: UUID
    thread_id: UUID | None
    execution_run_id: UUID | None
    answer_version_id: UUID | None
    generation_path: str | None
    latest_attempt_state: ThreadStateLiteral
    failure_detail: str | None
    planning_stage: RawTraceStageResponse
    rendering_stage: RawTraceStageResponse


class ExportRequestBody(BaseModel):
    mode: Literal["approved_only", "latest_available"]
    reproducibility_mode: Literal["best_effort", "strict_eval"] = "best_effort"


class ExportResponse(BaseModel):
    export_job_id: UUID
    status: str
    export_mode: str
    includes_unapproved_drafts: bool
    placeholder_row_count: int
    download_upload_id: UUID
    csv_download_upload_id: UUID
    zip_download_upload_id: UUID


class BulkFillRequestBody(BaseModel):
    note: str | None = None
    pipeline_profile: str | None = None
    pipeline_override: dict[str, object] | None = None
    reproducibility_mode: Literal["best_effort", "strict_eval"] = "best_effort"


class BulkFillSummaryResponse(BaseModel):
    id: UUID
    parent_request_id: UUID | None
    status: str
    created_at: datetime
    updated_at: datetime
    claim_id: str | None
    runner_id: str | None
    execution_mode: str | None
    claimed_at: datetime | None
    started_at: datetime | None
    heartbeat_at: datetime | None
    finished_at: datetime | None
    cancel_requested_at: datetime | None
    stale_detected_at: datetime | None
    summary: dict[str, object]
    error_detail: str | None
    config: dict[str, object]


class BulkFillResponse(BaseModel):
    request: BulkFillSummaryResponse


class BulkFillRowExecutionResponse(BaseModel):
    id: UUID
    questionnaire_row_id: UUID
    answer_version_id: UUID | None
    attempt_number: int
    status: str
    diagnostics: dict[str, object]
    error_detail: str | None
    created_at: datetime
    updated_at: datetime


class BulkFillJobEventResponse(BaseModel):
    id: UUID
    event_type: str
    runner_id: str | None
    message: str | None
    metadata: dict[str, object]
    created_at: datetime
    bulk_fill_row_execution_id: UUID | None


class BulkFillJobDetailResponse(BaseModel):
    request: BulkFillSummaryResponse
    rows: list[BulkFillRowExecutionResponse]
    events: list[BulkFillJobEventResponse]


class ApproveRowRequest(BaseModel):
    answer_version_id: UUID


class RejectRowRequest(BaseModel):
    answer_version_id: UUID | None = None


class DevTableSummaryResponse(BaseModel):
    name: str
    row_count: int
    case_filter_supported: bool


class DevTableListResponse(BaseModel):
    tables: list[DevTableSummaryResponse]


class DevTableRowsResponse(BaseModel):
    table_name: str
    row_count: int
    case_filter_applied: bool
    columns: list[str]
    rows: list[dict[str, object]]
