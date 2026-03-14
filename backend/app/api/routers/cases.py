from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Literal, cast
from uuid import UUID

from fastapi import APIRouter, Depends, File, Form, UploadFile
from fastapi.responses import StreamingResponse
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.api.deps import get_container, get_session, get_user_context
from app.exceptions import ValidationFailure
from app.models.entities import (
    AnswerVersion,
    BulkFillJobEvent,
    BulkFillRequest,
    BulkFillRowExecution,
    CaseProfile,
    ChatMessage,
    ChatThread,
    ExecutionRun,
    Questionnaire,
    QuestionnaireRow,
    RetrievalRun,
    RetrievalSnapshotItem,
    RfxCase,
    Upload,
)
from app.models.enums import (
    ExecutionRunKind,
    ExecutionRunStatus,
    ExportMode,
    QuestionnaireRowStatus,
    ReproducibilityMode,
)
from app.schemas.api import (
    AnswerVersionResponse,
    ApproveRowRequest,
    BulkFillJobDetailResponse,
    BulkFillJobEventResponse,
    BulkFillRequestBody,
    BulkFillResponse,
    BulkFillRowExecutionResponse,
    BulkFillSummaryResponse,
    CaseDetailResponse,
    CaseProfileSummaryResponse,
    CaseSummaryResponse,
    ChatMessageResponse,
    ChatThreadResponse,
    DraftRequest,
    DraftResponse,
    EvidenceResponse,
    ExportRequestBody,
    ExportResponse,
    QuestionnaireRowResponse,
    RejectRowRequest,
    RetrievalStageResponse,
    RetrievalSummaryResponse,
    ThreadDetailResponse,
    ThreadStateLiteral,
)
from app.services.answers import draft_answer_for_row, list_thread_messages
from app.services.bulk_fill import (
    _require_request_scope,
    approve_answer_version,
    cancel_bulk_fill_request,
    create_initial_bulk_fill_request,
    latest_bulk_fill_request,
    latest_row_execution,
    list_bulk_fill_request_events,
    list_bulk_fill_requests,
    list_bulk_fill_row_executions,
    reject_row_answer,
    resume_bulk_fill_request,
    retry_failed_bulk_fill_request,
)
from app.services.cases import create_case_from_uploads, require_case_scope, require_row_scope
from app.services.container import ServiceContainer
from app.services.exports import export_questionnaire
from app.services.identity import UserContext

router = APIRouter(prefix="/api/cases", tags=["cases"])


@dataclass(frozen=True)
class _ThreadWorkspaceProjection:
    thread: ChatThread | None
    state: ThreadStateLiteral
    answer_version: AnswerVersion | None
    retrieval_run: RetrievalRun | None
    failure_detail: str | None


def _parse_pipeline_override(payload: str | None) -> dict[str, object] | None:
    if payload is None:
        return None
    try:
        parsed = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise ValidationFailure(f"Pipeline override JSON is invalid: {exc}") from exc
    if not isinstance(parsed, dict):
        raise ValidationFailure("Pipeline override must be a JSON object.")
    return parsed


def _latest_answer_version_for_thread(
    session: Session,
    *,
    thread_id: UUID,
) -> AnswerVersion | None:
    return session.scalar(
        select(AnswerVersion)
        .where(AnswerVersion.chat_thread_id == thread_id)
        .order_by(AnswerVersion.version_number.desc())
    )


def _latest_answer_version_for_row(
    session: Session,
    *,
    row_id: UUID,
) -> AnswerVersion | None:
    return session.scalar(
        select(AnswerVersion)
        .where(AnswerVersion.questionnaire_row_id == row_id)
        .order_by(AnswerVersion.version_number.desc())
    )


def _latest_retrieval_run_for_thread(
    session: Session,
    *,
    thread_id: UUID,
) -> RetrievalRun | None:
    return session.scalar(
        select(RetrievalRun)
        .where(RetrievalRun.chat_thread_id == thread_id)
        .order_by(RetrievalRun.created_at.desc())
    )


def _latest_thread_for_row(
    session: Session,
    *,
    row: QuestionnaireRow,
) -> ChatThread | None:
    return session.scalar(
        select(ChatThread)
        .where(
            ChatThread.case_id == row.case_id,
            ChatThread.questionnaire_row_id == row.id,
        )
        .order_by(ChatThread.updated_at.desc(), ChatThread.created_at.desc())
    )


def _failed_run_for_thread(
    session: Session,
    *,
    row: QuestionnaireRow,
    thread: ChatThread,
    retrieval_run: RetrievalRun | None,
) -> ExecutionRun | None:
    if retrieval_run is not None and retrieval_run.execution_run_id is not None:
        retrieval_execution = session.get(ExecutionRun, retrieval_run.execution_run_id)
        if retrieval_execution is not None:
            if retrieval_execution.parent_run_id is not None:
                parent_run = session.get(ExecutionRun, retrieval_execution.parent_run_id)
                if parent_run is not None and parent_run.status == ExecutionRunStatus.FAILED:
                    return parent_run
            if retrieval_execution.status == ExecutionRunStatus.FAILED:
                return retrieval_execution
    thread_keyed_runs = [
        run
        for run in session.scalars(
            select(ExecutionRun)
            .where(
                ExecutionRun.case_id == row.case_id,
                ExecutionRun.kind.in_(
                    {
                        ExecutionRunKind.ROW_DRAFT,
                        ExecutionRunKind.ROW_REVISION,
                        ExecutionRunKind.BULK_FILL_ROW_ATTEMPT,
                    }
                ),
            )
            .order_by(ExecutionRun.created_at.desc())
        ).all()
        if run.inputs_json.get("thread_id") == str(thread.id)
    ]
    return next(
        (run for run in thread_keyed_runs if run.status == ExecutionRunStatus.FAILED),
        None,
    )


def _thread_workspace_projection(
    session: Session,
    *,
    row: QuestionnaireRow,
    thread: ChatThread | None,
) -> _ThreadWorkspaceProjection:
    if thread is None:
        return _ThreadWorkspaceProjection(
            thread=None,
            state="none",
            answer_version=None,
            retrieval_run=None,
            failure_detail=None,
        )
    answer_version = _latest_answer_version_for_thread(session, thread_id=thread.id)
    if answer_version is not None:
        retrieval_run = session.get(RetrievalRun, answer_version.retrieval_run_id)
        if retrieval_run is None:
            raise ValidationFailure(
                f"Answer version {answer_version.id} is missing retrieval run {answer_version.retrieval_run_id}."
            )
        return _ThreadWorkspaceProjection(
            thread=thread,
            state="answer_available",
            answer_version=answer_version,
            retrieval_run=retrieval_run,
            failure_detail=None,
        )
    retrieval_run = _latest_retrieval_run_for_thread(session, thread_id=thread.id)
    failed_run = _failed_run_for_thread(
        session,
        row=row,
        thread=thread,
        retrieval_run=retrieval_run,
    )
    if failed_run is not None:
        return _ThreadWorkspaceProjection(
            thread=thread,
            state="failed_no_answer",
            answer_version=None,
            retrieval_run=retrieval_run,
            failure_detail=failed_run.error_detail,
        )
    if row.review_status == QuestionnaireRowStatus.FAILED:
        return _ThreadWorkspaceProjection(
            thread=thread,
            state="failed_no_answer",
            answer_version=None,
            retrieval_run=retrieval_run,
            failure_detail=row.last_error_detail,
        )
    return _ThreadWorkspaceProjection(
        thread=thread,
        state="pending_no_answer",
        answer_version=None,
        retrieval_run=retrieval_run,
        failure_detail=None,
    )


def _thread_detail_response(
    session: Session,
    *,
    row: QuestionnaireRow,
    thread: ChatThread,
) -> ThreadDetailResponse:
    projection = _thread_workspace_projection(session, row=row, thread=thread)
    if projection.thread is None:
        raise ValidationFailure(f"Thread {thread.id} is not available for row {row.id}.")
    messages = list_thread_messages(session, thread_id=thread.id)
    evidence_items = (
        session.scalars(
            select(RetrievalSnapshotItem)
            .where(RetrievalSnapshotItem.retrieval_run_id == projection.retrieval_run.id)
            .order_by(RetrievalSnapshotItem.rank.asc())
        ).all()
        if projection.retrieval_run is not None
        else []
    )
    return ThreadDetailResponse(
        thread=_thread_response(thread),
        thread_state=projection.state,
        messages=[_message_response(message) for message in messages],
        answer_version=(
            _answer_response(session, projection.answer_version)
            if projection.answer_version is not None
            else None
        ),
        retrieval=(
            _retrieval_response(projection.retrieval_run)
            if projection.retrieval_run is not None
            else None
        ),
        evidence=[_evidence_response(item) for item in evidence_items],
        failure_detail=projection.failure_detail,
    )


def _row_response(
    session: Session,
    row: QuestionnaireRow,
    answer_text: str,
) -> QuestionnaireRowResponse:
    approved_answer_text: str | None = None
    if row.approved_answer_version_id is not None:
        approved_version = session.get(AnswerVersion, row.approved_answer_version_id)
        if approved_version is None or approved_version.case_id != row.case_id or approved_version.questionnaire_row_id != row.id:
            raise ValidationFailure(
                f"Questionnaire row {row.id} has invalid approved answer reference {row.approved_answer_version_id}."
            )
        approved_answer_text = approved_version.answer_text
    last_execution = latest_row_execution(session, row_id=row.id)
    latest_attempt = _thread_workspace_projection(
        session,
        row=row,
        thread=_latest_thread_for_row(session, row=row),
    )
    return QuestionnaireRowResponse(
        id=row.id,
        source_row_id=row.source_row_id,
        source_row_number=row.source_row_number,
        context=row.context_raw,
        question=row.question_raw,
        current_answer=answer_text,
        review_status=row.review_status.value,
        approved_answer_version_id=row.approved_answer_version_id,
        approved_answer_text=approved_answer_text,
        last_error_detail=row.last_error_detail,
        last_bulk_fill_request_id=last_execution.bulk_fill_request_id if last_execution else None,
        last_bulk_fill_row_execution_id=last_execution.id if last_execution else None,
        last_bulk_fill_status=last_execution.status.value if last_execution else None,
        last_bulk_fill_attempt_number=last_execution.attempt_number if last_execution else None,
        latest_attempt_thread_id=latest_attempt.thread.id if latest_attempt.thread else None,
        latest_attempt_state=latest_attempt.state,
    )


def _row_response_with_latest_answer(
    session: Session,
    *,
    row: QuestionnaireRow,
) -> QuestionnaireRowResponse:
    latest_answer = _latest_answer_version_for_row(session, row_id=row.id)
    return _row_response(
        session,
        row,
        latest_answer.answer_text if latest_answer else row.answer_raw,
    )


def _thread_response(thread: ChatThread) -> ChatThreadResponse:
    return ChatThreadResponse(
        id=thread.id,
        questionnaire_row_id=thread.questionnaire_row_id,
        title=thread.title,
        updated_at=thread.updated_at,
    )


def _answer_response(session: Session, version: AnswerVersion) -> AnswerVersionResponse:
    retrieval_run = session.get(RetrievalRun, version.retrieval_run_id)
    if retrieval_run is None:
        raise ValidationFailure(
            f"Answer version {version.id} is missing retrieval run {version.retrieval_run_id}."
        )
    generation_path = "two_stage_plan_render"
    if version.execution_run_id is not None:
        execution_run = session.get(ExecutionRun, version.execution_run_id)
        if execution_run is not None:
            persisted_path = execution_run.outputs_json.get("generation_path")
            if isinstance(persisted_path, str) and persisted_path.strip():
                generation_path = persisted_path
    pipeline = retrieval_run.request_context.get("pipeline", {})
    if not isinstance(pipeline, dict):
        raise ValidationFailure(
            f"Answer version {version.id} retrieval run is missing pipeline metadata."
        )
    return AnswerVersionResponse(
        id=version.id,
        chat_thread_id=version.chat_thread_id,
        retrieval_run_id=version.retrieval_run_id,
        version_number=version.version_number,
        answer_text=version.answer_text,
        status=version.status.value,
        pipeline_profile_name=cast(str | None, pipeline.get("profile_name")),
        pipeline_config_hash=str(pipeline.get("config_hash", "")),
        created_at=version.created_at,
        model=version.model,
        generation_path=generation_path,
        llm_capture_stage="answer_rendering"
        if version.llm_capture_status.value == "captured"
        else None,
        prompt_version=version.prompt_version,
        llm_capture_status=version.llm_capture_status.value,
        llm_request_text=version.llm_request_text,
        llm_response_text=version.llm_response_text,
    )


def _message_response(message: ChatMessage) -> ChatMessageResponse:
    return ChatMessageResponse(
        id=message.id,
        role=message.role.value,
        content=message.content,
        created_at=message.created_at,
        answer_version_id=message.answer_version_id,
    )


def _evidence_response(item: RetrievalSnapshotItem) -> EvidenceResponse:
    return EvidenceResponse(
        id=item.id,
        source_kind=cast(
            Literal[
                "case_profile_item",
                "historical_qa_row",
                "product_truth_chunk",
                "pdf_chunk",
                "pdf_page",
            ],
            item.source_kind.value,
        ),
        source_label=item.source_label,
        source_title=item.source_title,
        excerpt=item.excerpt,
        score=item.score,
        metadata=item.metadata_json,
    )


def _retrieval_response(run: RetrievalRun) -> RetrievalSummaryResponse:
    execution = run.request_context.get("retrieval_execution", {})
    if not isinstance(execution, dict):
        raise ValidationFailure(f"Retrieval run {run.id} is missing execution metadata.")
    pipeline = run.request_context.get("pipeline", {})
    if not isinstance(pipeline, dict):
        raise ValidationFailure(f"Retrieval run {run.id} is missing pipeline metadata.")
    sufficiency = execution.get("sufficiency", {})
    if not isinstance(sufficiency, dict):
        raise ValidationFailure(f"Retrieval run {run.id} is missing sufficiency metadata.")
    revision_classifier = run.request_context.get("revision_classifier", {})
    if not isinstance(revision_classifier, dict):
        raise ValidationFailure(f"Retrieval run {run.id} has invalid revision classifier metadata.")
    stages = execution.get("stages", [])
    if not isinstance(stages, list):
        raise ValidationFailure(f"Retrieval run {run.id} has invalid candidate generation stages.")
    return RetrievalSummaryResponse(
        strategy_version=str(run.request_context.get("strategy_version", "")),
        pipeline_profile_name=cast(str | None, pipeline.get("profile_name")),
        pipeline_config_hash=str(pipeline.get("config_hash", "")),
        index_config_hash=str(pipeline.get("index_config_hash", "")),
        revision_mode=str(run.request_context.get("revision_mode", "")),
        revision_classifier_version=cast(
            str | None,
            revision_classifier.get("version"),
        ),
        revision_reason=cast(
            str | None,
            revision_classifier.get("reason"),
        ),
        retrieval_action=str(run.request_context.get("retrieval_action", "")),
        retrieval_action_reason=cast(
            str | None,
            run.request_context.get("retrieval_action_reason"),
        ),
        reused_from_retrieval_run_id=UUID(value)
        if isinstance((value := run.request_context.get("reused_from_retrieval_run_id")), str)
        else None,
        candidate_generation_mode=str(execution.get("candidate_generation_mode", "")),
        broadened=bool(execution.get("broadened", False)),
        sufficiency=str(sufficiency.get("status", "")),
        degraded=bool(sufficiency.get("degraded", False)),
        notes=[str(item) for item in sufficiency.get("notes", []) if isinstance(item, str)],
        stages=[
            RetrievalStageResponse(
                corpus=str(item.get("corpus", "")),
                stage=str(item.get("stage", "")),
                mode=str(item.get("mode", "")),
                candidate_count=int(item.get("candidate_count", 0)),
                broadened=bool(item.get("broadened", False)),
                skipped=bool(item.get("skipped", False)),
                reason=cast(str | None, item.get("reason")),
            )
            for item in stages
            if isinstance(item, dict)
        ],
    )


def _bulk_fill_summary_response(request: BulkFillRequest) -> BulkFillSummaryResponse:
    return BulkFillSummaryResponse(
        id=request.id,
        parent_request_id=request.parent_request_id,
        status=request.status.value,
        created_at=request.created_at,
        updated_at=request.updated_at,
        claim_id=request.claim_id,
        runner_id=request.runner_id,
        execution_mode=request.execution_mode,
        claimed_at=request.claimed_at,
        started_at=request.started_at,
        heartbeat_at=request.heartbeat_at,
        finished_at=request.finished_at,
        cancel_requested_at=request.cancel_requested_at,
        stale_detected_at=request.stale_detected_at,
        summary=request.summary_json,
        error_detail=request.error_detail,
        config=request.config_json,
    )


def _bulk_fill_row_execution_response(
    row: BulkFillRowExecution,
) -> BulkFillRowExecutionResponse:
    return BulkFillRowExecutionResponse(
        id=row.id,
        questionnaire_row_id=row.questionnaire_row_id,
        answer_version_id=row.answer_version_id,
        attempt_number=row.attempt_number,
        status=row.status.value,
        diagnostics=row.diagnostics_json,
        error_detail=row.error_detail,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


def _bulk_fill_job_event_response(event: BulkFillJobEvent) -> BulkFillJobEventResponse:
    return BulkFillJobEventResponse(
        id=event.id,
        event_type=event.event_type.value,
        runner_id=event.runner_id,
        message=event.message,
        metadata=event.metadata_json,
        created_at=event.created_at,
        bulk_fill_row_execution_id=event.bulk_fill_row_execution_id,
    )


def _require_case_questionnaire_for_bulk_fill(
    session: Session,
    *,
    case_id: UUID,
    tenant_id: UUID,
) -> tuple[RfxCase, Questionnaire]:
    case = require_case_scope(session, case_id=case_id, tenant_id=tenant_id)
    questionnaire = session.scalar(select(Questionnaire).where(Questionnaire.case_id == case.id))
    if questionnaire is None:
        raise ValidationFailure(f"Case {case.id} has no questionnaire for bulk fill.")
    return case, questionnaire


def _bulk_fill_response(request: BulkFillRequest) -> BulkFillResponse:
    return BulkFillResponse(request=_bulk_fill_summary_response(request))


@router.get("", response_model=list[CaseSummaryResponse])
def list_cases(
    session: Session = Depends(get_session),
    user_context: UserContext = Depends(get_user_context),
) -> list[CaseSummaryResponse]:
    cases = session.scalars(
        select(RfxCase)
        .where(RfxCase.tenant_id == user_context.tenant.id)
        .order_by(RfxCase.updated_at.desc())
    ).all()
    return [
        CaseSummaryResponse(
            id=case.id,
            name=case.name,
            client_name=case.client_name,
            language=case.language,
            status=case.status.value,
            created_at=case.created_at,
            updated_at=case.updated_at,
        )
        for case in cases
    ]


@router.post("", response_model=CaseDetailResponse)
async def create_case(
    name: str = Form(...),
    client_name: str | None = Form(default=None),
    pipeline_profile: str | None = Form(default=None),
    pipeline_override: str | None = Form(default=None),
    reproducibility_mode: str = Form(default="best_effort"),
    pdf: UploadFile = File(...),
    questionnaire: UploadFile | None = File(default=None),
    container: ServiceContainer = Depends(get_container),
    session: Session = Depends(get_session),
    user_context: UserContext = Depends(get_user_context),
) -> CaseDetailResponse:
    pdf_payload = await pdf.read()
    questionnaire_payload = await questionnaire.read() if questionnaire is not None else None
    case = create_case_from_uploads(
        session,
        storage=container.storage,
        ai_service=container.ai_service,
        tenant_id=user_context.tenant.id,
        user_id=user_context.user.id,
        case_name=name,
        client_name=client_name,
        pdf_file_name=pdf.filename or "uploaded.pdf",
        pdf_media_type=pdf.content_type or "application/pdf",
        pdf_payload=pdf_payload,
        questionnaire_file_name=questionnaire.filename if questionnaire else None,
        questionnaire_media_type=questionnaire.content_type if questionnaire else None,
        questionnaire_payload=questionnaire_payload,
        settings=container.settings,
        pipeline_profile_name=pipeline_profile,
        pipeline_override=_parse_pipeline_override(pipeline_override),
        reproducibility_mode=ReproducibilityMode(reproducibility_mode),
    )
    session.flush()
    return get_case(case.id, session=session, user_context=user_context)


@router.get("/{case_id}", response_model=CaseDetailResponse)
def get_case(
    case_id: UUID,
    session: Session = Depends(get_session),
    user_context: UserContext = Depends(get_user_context),
) -> CaseDetailResponse:
    case = require_case_scope(session, case_id=case_id, tenant_id=user_context.tenant.id)
    profile = session.scalar(
        select(CaseProfile)
        .where(CaseProfile.case_id == case.id)
        .order_by(CaseProfile.created_at.desc())
    )
    rows = session.scalars(
        select(QuestionnaireRow)
        .where(QuestionnaireRow.case_id == case.id)
        .order_by(QuestionnaireRow.source_row_number.asc())
    ).all()
    row_responses = [_row_response_with_latest_answer(session, row=row) for row in rows]
    threads = session.scalars(
        select(ChatThread)
        .where(ChatThread.case_id == case.id)
        .order_by(ChatThread.updated_at.desc())
    ).all()
    latest_bulk_fill = latest_bulk_fill_request(session, case_id=case.id)
    bulk_fill_history = list_bulk_fill_requests(session, case_id=case.id)
    return CaseDetailResponse(
        id=case.id,
        name=case.name,
        client_name=case.client_name,
        language=case.language,
        status=case.status.value,
        created_at=case.created_at,
        updated_at=case.updated_at,
        profile=CaseProfileSummaryResponse(
            schema_version=profile.schema_version,
            prompt_set_version=profile.prompt_set_version,
            summary=profile.summary,
            generated_at=profile.generated_at,
        )
        if profile
        else None,
        latest_bulk_fill=_bulk_fill_summary_response(latest_bulk_fill)
        if latest_bulk_fill
        else None,
        bulk_fill_history=[_bulk_fill_summary_response(item) for item in bulk_fill_history],
        questionnaire_rows=row_responses,
        chats=[_thread_response(thread) for thread in threads],
    )


@router.get("/{case_id}/rows/{row_id}/answers", response_model=list[AnswerVersionResponse])
def list_answer_versions(
    case_id: UUID,
    row_id: UUID,
    session: Session = Depends(get_session),
    user_context: UserContext = Depends(get_user_context),
) -> list[AnswerVersionResponse]:
    case = require_case_scope(session, case_id=case_id, tenant_id=user_context.tenant.id)
    row = require_row_scope(session, row_id=row_id, case=case)
    versions = session.scalars(
        select(AnswerVersion)
        .where(AnswerVersion.questionnaire_row_id == row.id)
        .order_by(AnswerVersion.version_number.desc())
    ).all()
    return [_answer_response(session, version) for version in versions]


@router.get("/{case_id}/threads/{thread_id}", response_model=ThreadDetailResponse)
def get_thread(
    case_id: UUID,
    thread_id: UUID,
    session: Session = Depends(get_session),
    user_context: UserContext = Depends(get_user_context),
) -> ThreadDetailResponse:
    case = require_case_scope(session, case_id=case_id, tenant_id=user_context.tenant.id)
    thread = session.get(ChatThread, thread_id)
    if thread is None or thread.case_id != case.id or thread.tenant_id != case.tenant_id:
        raise ValidationFailure(f"Thread {thread_id} is not available for case {case.id}.")
    row = require_row_scope(session, row_id=thread.questionnaire_row_id, case=case)
    return _thread_detail_response(session, row=row, thread=thread)


@router.post("/{case_id}/rows/{row_id}/draft", response_model=DraftResponse)
def draft_row_answer(
    case_id: UUID,
    row_id: UUID,
    body: DraftRequest,
    container: ServiceContainer = Depends(get_container),
    session: Session = Depends(get_session),
    user_context: UserContext = Depends(get_user_context),
) -> DraftResponse:
    case = require_case_scope(session, case_id=case_id, tenant_id=user_context.tenant.id)
    row = require_row_scope(session, row_id=row_id, case=case)
    thread = session.get(ChatThread, body.thread_id) if body.thread_id else None
    result = draft_answer_for_row(
        session,
        ai_service=container.ai_service,
        case=case,
        row=row,
        user_id=user_context.user.id,
        user_message=body.message,
        thread=thread,
        settings=container.settings,
        pipeline_profile_name=body.pipeline_profile,
        pipeline_override=body.pipeline_override,
        reproducibility_mode=ReproducibilityMode(body.reproducibility_mode),
        revision_mode_override=body.revision_mode_override,
    )
    retrieval_run = session.get(RetrievalRun, result.answer_version.retrieval_run_id)
    if retrieval_run is None:
        raise ValidationFailure(
            f"Answer version {result.answer_version.id} is missing retrieval run {result.answer_version.retrieval_run_id}."
        )
    return DraftResponse(
        thread=_thread_response(result.thread),
        messages=[_message_response(message) for message in result.messages],
        answer_version=_answer_response(session, result.answer_version),
        retrieval=_retrieval_response(retrieval_run),
        evidence=[_evidence_response(item) for item in result.evidence_items],
    )


@router.post("/{case_id}/export", response_model=ExportResponse)
def export_case_questionnaire(
    case_id: UUID,
    body: ExportRequestBody,
    container: ServiceContainer = Depends(get_container),
    session: Session = Depends(get_session),
    user_context: UserContext = Depends(get_user_context),
) -> ExportResponse:
    case = require_case_scope(session, case_id=case_id, tenant_id=user_context.tenant.id)
    questionnaire = session.scalar(select(Questionnaire).where(Questionnaire.case_id == case.id))
    if questionnaire is None:
        raise ValidationFailure(f"Case {case.id} has no questionnaire to export.")
    upload = session.get(Upload, questionnaire.upload_id)
    if upload is None:
        raise ValidationFailure(
            f"Questionnaire {questionnaire.id} is missing its source upload provenance."
        )
    export_job = export_questionnaire(
        session,
        storage=container.storage,
        settings=container.settings,
        questionnaire=questionnaire,
        upload=upload,
        mode=ExportMode(body.mode),
        user_id=user_context.user.id,
        reproducibility_mode=ReproducibilityMode(body.reproducibility_mode),
    )
    if export_job.output_upload_id is None:
        raise ValidationFailure(f"Export job {export_job.id} completed without an output upload.")
    csv_upload_id = export_job.metadata_json.get("csv_upload_id")
    if not isinstance(csv_upload_id, str):
        raise ValidationFailure(f"Export job {export_job.id} completed without a CSV output upload.")
    zip_upload_id = export_job.metadata_json.get("zip_upload_id")
    if not isinstance(zip_upload_id, str):
        raise ValidationFailure(f"Export job {export_job.id} completed without a ZIP output upload.")
    return ExportResponse(
        export_job_id=export_job.id,
        status=export_job.status.value,
        export_mode=export_job.export_mode.value,
        includes_unapproved_drafts=bool(
            export_job.metadata_json.get("includes_unapproved_drafts", False)
        ),
        placeholder_row_count=int(export_job.metadata_json.get("placeholder_row_count", 0)),
        download_upload_id=export_job.output_upload_id,
        csv_download_upload_id=UUID(csv_upload_id),
        zip_download_upload_id=UUID(zip_upload_id),
    )


@router.get("/downloads/{upload_id}")
def download_upload(
    upload_id: UUID,
    container: ServiceContainer = Depends(get_container),
    session: Session = Depends(get_session),
    user_context: UserContext = Depends(get_user_context),
) -> StreamingResponse:
    upload = session.get(Upload, upload_id)
    if upload is None or upload.tenant_id != user_context.tenant.id:
        raise ValidationFailure(f"Upload {upload_id} is not available.")
    payload = container.storage.read_bytes(upload.object_key)
    return StreamingResponse(
        iter([payload]),
        media_type=upload.media_type,
        headers={"Content-Disposition": f'attachment; filename="{upload.original_file_name}"'},
    )


@router.get("/{case_id}/bulk-fill-jobs", response_model=list[BulkFillSummaryResponse])
def list_case_bulk_fill_jobs(
    case_id: UUID,
    session: Session = Depends(get_session),
    user_context: UserContext = Depends(get_user_context),
) -> list[BulkFillSummaryResponse]:
    case = require_case_scope(session, case_id=case_id, tenant_id=user_context.tenant.id)
    return [
        _bulk_fill_summary_response(item)
        for item in list_bulk_fill_requests(session, case_id=case.id)
    ]


@router.get("/{case_id}/bulk-fill-jobs/{request_id}", response_model=BulkFillJobDetailResponse)
def get_case_bulk_fill_job(
    case_id: UUID,
    request_id: UUID,
    session: Session = Depends(get_session),
    user_context: UserContext = Depends(get_user_context),
) -> BulkFillJobDetailResponse:
    case = require_case_scope(session, case_id=case_id, tenant_id=user_context.tenant.id)
    request = _require_request_scope(session, request_id=request_id, case=case)
    return BulkFillJobDetailResponse(
        request=_bulk_fill_summary_response(request),
        rows=[
            _bulk_fill_row_execution_response(row)
            for row in list_bulk_fill_row_executions(session, request_id=request.id)
        ],
        events=[
            _bulk_fill_job_event_response(event)
            for event in list_bulk_fill_request_events(session, request_id=request.id)
        ],
    )


@router.post("/{case_id}/bulk-fill", response_model=BulkFillResponse)
def request_bulk_fill(
    case_id: UUID,
    body: BulkFillRequestBody,
    container: ServiceContainer = Depends(get_container),
    session: Session = Depends(get_session),
    user_context: UserContext = Depends(get_user_context),
) -> BulkFillResponse:
    case, questionnaire = _require_case_questionnaire_for_bulk_fill(
        session,
        case_id=case_id,
        tenant_id=user_context.tenant.id,
    )
    request = create_initial_bulk_fill_request(
        session,
        case=case,
        questionnaire=questionnaire,
        user_id=user_context.user.id,
        note=body.note,
        settings=container.settings,
        pipeline_profile_name=body.pipeline_profile,
        pipeline_override=body.pipeline_override,
        reproducibility_mode=ReproducibilityMode(body.reproducibility_mode),
    )
    return _bulk_fill_response(request)


@router.post("/{case_id}/bulk-fill/{request_id}/retry-failed", response_model=BulkFillResponse)
def retry_failed_bulk_fill(
    case_id: UUID,
    request_id: UUID,
    session: Session = Depends(get_session),
    user_context: UserContext = Depends(get_user_context),
) -> BulkFillResponse:
    case, questionnaire = _require_case_questionnaire_for_bulk_fill(
        session,
        case_id=case_id,
        tenant_id=user_context.tenant.id,
    )
    source_request = _require_request_scope(session, request_id=request_id, case=case)
    request = retry_failed_bulk_fill_request(
        session,
        case=case,
        questionnaire=questionnaire,
        source_request=source_request,
        user_id=user_context.user.id,
    )
    return _bulk_fill_response(request)


@router.post("/{case_id}/bulk-fill/{request_id}/resume", response_model=BulkFillResponse)
def resume_bulk_fill(
    case_id: UUID,
    request_id: UUID,
    session: Session = Depends(get_session),
    user_context: UserContext = Depends(get_user_context),
) -> BulkFillResponse:
    case, questionnaire = _require_case_questionnaire_for_bulk_fill(
        session,
        case_id=case_id,
        tenant_id=user_context.tenant.id,
    )
    source_request = _require_request_scope(session, request_id=request_id, case=case)
    request = resume_bulk_fill_request(
        session,
        case=case,
        questionnaire=questionnaire,
        source_request=source_request,
        user_id=user_context.user.id,
    )
    return _bulk_fill_response(request)


@router.post("/{case_id}/bulk-fill/{request_id}/cancel", response_model=BulkFillResponse)
def cancel_bulk_fill(
    case_id: UUID,
    request_id: UUID,
    session: Session = Depends(get_session),
    user_context: UserContext = Depends(get_user_context),
) -> BulkFillResponse:
    case = require_case_scope(session, case_id=case_id, tenant_id=user_context.tenant.id)
    request = _require_request_scope(session, request_id=request_id, case=case)
    request = cancel_bulk_fill_request(session, request=request)
    return _bulk_fill_response(request)


@router.post("/{case_id}/rows/{row_id}/approve", response_model=QuestionnaireRowResponse)
def approve_row_answer(
    case_id: UUID,
    row_id: UUID,
    body: ApproveRowRequest,
    session: Session = Depends(get_session),
    user_context: UserContext = Depends(get_user_context),
) -> QuestionnaireRowResponse:
    case = require_case_scope(session, case_id=case_id, tenant_id=user_context.tenant.id)
    row = require_row_scope(session, row_id=row_id, case=case)
    approve_answer_version(
        session,
        case=case,
        row=row,
        answer_version_id=body.answer_version_id,
    )
    return _row_response_with_latest_answer(session, row=row)


@router.post("/{case_id}/rows/{row_id}/reject", response_model=QuestionnaireRowResponse)
def reject_row_answer_route(
    case_id: UUID,
    row_id: UUID,
    body: RejectRowRequest,
    session: Session = Depends(get_session),
    user_context: UserContext = Depends(get_user_context),
) -> QuestionnaireRowResponse:
    case = require_case_scope(session, case_id=case_id, tenant_id=user_context.tenant.id)
    row = require_row_scope(session, row_id=row_id, case=case)
    reject_row_answer(
        session,
        case=case,
        row=row,
        answer_version_id=body.answer_version_id,
    )
    return _row_response_with_latest_answer(session, row=row)
