from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import cast
from uuid import UUID, uuid4

from sqlalchemy import func, select, update
from sqlalchemy.orm import Session

from app.config import Settings, get_settings
from app.exceptions import ValidationFailure
from app.models.entities import (
    AnswerVersion,
    BulkFillJobEvent,
    BulkFillRequest,
    BulkFillRowExecution,
    ChatThread,
    ExecutionRun,
    Questionnaire,
    QuestionnaireRow,
    RetrievalRun,
    RetrievalSnapshotItem,
    RfxCase,
)
from app.models.enums import (
    AnswerStatus,
    BulkFillEventType,
    BulkFillRowStatus,
    BulkFillStatus,
    ExecutionRunKind,
    QuestionnaireRowStatus,
    ReproducibilityMode,
)
from app.pipeline.config import (
    PipelineSelection,
    assert_pipeline_runtime_compatibility,
    resolve_pipeline_selection,
)
from app.services.ai import OpenAIAIService
from app.services.answers import draft_answer_for_row
from app.services.container import ServiceContainer
from app.services.reproducibility import fail_execution_run, finish_execution_run, start_repro_run

ACTIVE_BULK_FILL_STATUSES = {
    BulkFillStatus.QUEUED,
    BulkFillStatus.RUNNING,
    BulkFillStatus.CANCEL_REQUESTED,
}
RESUMABLE_BULK_FILL_STATUSES = {
    BulkFillStatus.FAILED,
    BulkFillStatus.CANCELLED,
    BulkFillStatus.COMPLETED_WITH_FAILURES,
    BulkFillStatus.ORPHANED,
}
DEFAULT_BULK_FILL_MESSAGE = "Draft a grounded questionnaire answer for this row."
STALE_CLAIM_AFTER = timedelta(minutes=10)
MISSING_OPENAI_KEY_ERROR = "Bulk-fill worker requires OPENAI_API_KEY for row drafting."


def _record_event(
    session: Session,
    *,
    request: BulkFillRequest,
    event_type: BulkFillEventType,
    runner_id: str | None = None,
    row_execution: BulkFillRowExecution | None = None,
    message: str | None = None,
    metadata: dict[str, object] | None = None,
) -> None:
    session.add(
        BulkFillJobEvent(
            tenant_id=request.tenant_id,
            case_id=request.case_id,
            bulk_fill_request_id=request.id,
            bulk_fill_row_execution_id=row_execution.id if row_execution else None,
            event_type=event_type,
            runner_id=runner_id,
            message=message,
            metadata_json=metadata or {},
        )
    )


def _ordered_questionnaire_rows(
    session: Session,
    *,
    questionnaire_id: UUID,
) -> list[QuestionnaireRow]:
    return list(
        session.scalars(
            select(QuestionnaireRow)
            .where(QuestionnaireRow.questionnaire_id == questionnaire_id)
            .order_by(QuestionnaireRow.source_row_number.asc())
        ).all()
    )


def _unapproved_rows(rows: list[QuestionnaireRow]) -> list[QuestionnaireRow]:
    return [row for row in rows if row.approved_answer_version_id is None]


def _ensure_bulk_fill_generation_available(container: ServiceContainer) -> None:
    if isinstance(container.ai_service, OpenAIAIService) and not container.settings.openai_api_key:
        raise ValidationFailure(MISSING_OPENAI_KEY_ERROR)


def _active_request_for_case(session: Session, *, case_id: UUID) -> BulkFillRequest | None:
    return session.scalar(
        select(BulkFillRequest)
        .where(
            BulkFillRequest.case_id == case_id,
            BulkFillRequest.status.in_(ACTIVE_BULK_FILL_STATUSES),
        )
        .order_by(BulkFillRequest.created_at.desc())
    )


def latest_bulk_fill_request(
    session: Session,
    *,
    case_id: UUID,
) -> BulkFillRequest | None:
    return session.scalar(
        select(BulkFillRequest)
        .where(BulkFillRequest.case_id == case_id)
        .order_by(BulkFillRequest.created_at.desc())
    )


def list_bulk_fill_requests(
    session: Session,
    *,
    case_id: UUID,
) -> list[BulkFillRequest]:
    return list(
        session.scalars(
            select(BulkFillRequest)
            .where(BulkFillRequest.case_id == case_id)
            .order_by(BulkFillRequest.created_at.desc())
        ).all()
    )


def list_bulk_fill_request_events(
    session: Session,
    *,
    request_id: UUID,
) -> list[BulkFillJobEvent]:
    return list(
        session.scalars(
            select(BulkFillJobEvent)
            .where(BulkFillJobEvent.bulk_fill_request_id == request_id)
            .order_by(BulkFillJobEvent.created_at.asc())
        ).all()
    )


def list_bulk_fill_row_executions(
    session: Session,
    *,
    request_id: UUID,
) -> list[BulkFillRowExecution]:
    return list(
        session.scalars(
            select(BulkFillRowExecution)
            .where(BulkFillRowExecution.bulk_fill_request_id == request_id)
            .order_by(BulkFillRowExecution.created_at.asc())
        ).all()
    )


def latest_row_execution(
    session: Session,
    *,
    row_id: UUID,
) -> BulkFillRowExecution | None:
    return session.scalar(
        select(BulkFillRowExecution)
        .where(BulkFillRowExecution.questionnaire_row_id == row_id)
        .order_by(BulkFillRowExecution.created_at.desc())
    )


def _attempt_number_for_row(session: Session, *, row_id: UUID) -> int:
    return (
        session.scalar(
            select(func.coalesce(func.max(BulkFillRowExecution.attempt_number), 0) + 1).where(
                BulkFillRowExecution.questionnaire_row_id == row_id
            )
        )
        or 1
    )


def _refresh_request_summary(session: Session, request: BulkFillRequest) -> dict[str, object]:
    row_executions = session.scalars(
        select(BulkFillRowExecution).where(BulkFillRowExecution.bulk_fill_request_id == request.id)
    ).all()
    row_execution_counts = {status.value: 0 for status in BulkFillRowStatus}
    for row_execution in row_executions:
        row_execution_counts[row_execution.status.value] += 1
    questionnaire_rows = session.scalars(
        select(QuestionnaireRow)
        .where(QuestionnaireRow.questionnaire_id == request.questionnaire_id)
        .order_by(QuestionnaireRow.source_row_number.asc())
    ).all()
    review_status_counts = {status.value: 0 for status in QuestionnaireRowStatus}
    for questionnaire_row in questionnaire_rows:
        review_status_counts[questionnaire_row.review_status.value] += 1
    summary = {
        "total_rows": len(row_executions),
        "row_execution_counts": row_execution_counts,
        "review_status_counts": review_status_counts,
    }
    request.summary_json = summary
    return summary


def _require_request_scope(
    session: Session,
    *,
    request_id: UUID,
    case: RfxCase,
) -> BulkFillRequest:
    request = session.get(BulkFillRequest, request_id)
    if request is None or request.case_id != case.id or request.tenant_id != case.tenant_id:
        raise ValidationFailure(f"Bulk-fill request {request_id} is not available for case {case.id}.")
    return request


def _latest_thread_for_row(session: Session, *, row_id: UUID, case_id: UUID) -> ChatThread | None:
    return session.scalar(
        select(ChatThread)
        .where(ChatThread.questionnaire_row_id == row_id, ChatThread.case_id == case_id)
        .order_by(ChatThread.updated_at.desc())
    )


def create_bulk_fill_request(
    session: Session,
    *,
    case: RfxCase,
    questionnaire: Questionnaire,
    user_id: UUID,
    note: str | None,
    action: str,
    parent_request: BulkFillRequest | None = None,
    target_rows: list[QuestionnaireRow],
    settings: Settings | None = None,
    pipeline_profile_name: str | None = None,
    pipeline_override: dict[str, object] | None = None,
    pipeline_selection: PipelineSelection | None = None,
    reproducibility_mode: ReproducibilityMode = ReproducibilityMode.BEST_EFFORT,
) -> BulkFillRequest:
    target_rows = _unapproved_rows(target_rows)
    if pipeline_selection is None:
        if parent_request is not None and "resolved_pipeline_config" in parent_request.config_json:
            pipeline_selection = resolve_pipeline_selection(
                settings or get_settings(),
                pinned_config=parent_request.config_json["resolved_pipeline_config"],  # type: ignore[arg-type]
                pinned_profile_name=parent_request.config_json.get("pipeline_profile_name"),  # type: ignore[arg-type]
            )
        else:
            pipeline_selection = resolve_pipeline_selection(
                settings or get_settings(),
                profile_name=pipeline_profile_name,
                override=pipeline_override,
                pinned_config=case.pipeline_config_json,
                pinned_profile_name=case.pipeline_profile_name,
            )
    active = _active_request_for_case(session, case_id=case.id)
    if active is not None:
        raise ValidationFailure(
            f"Case {case.id} already has an active bulk-fill request {active.id} with status {active.status.value}."
        )
    if not target_rows:
        if action == "initial":
            raise ValidationFailure(
                f"Case {case.id} has no eligible bulk-fill rows because all rows are already approved."
            )
        raise ValidationFailure(
            f"Bulk-fill request for case {case.id} has no eligible unapproved target rows."
        )
    assert_pipeline_runtime_compatibility(
        session,
        case=case,
        selection=pipeline_selection,
    )
    request = BulkFillRequest(
        tenant_id=case.tenant_id,
        case_id=case.id,
        questionnaire_id=questionnaire.id,
        parent_request_id=parent_request.id if parent_request else None,
        requested_by_user_id=user_id,
        status=BulkFillStatus.QUEUED,
        claim_id=None,
        runner_id=None,
        execution_mode=None,
        claimed_at=None,
        started_at=None,
        heartbeat_at=None,
        finished_at=None,
        cancel_requested_at=None,
        stale_detected_at=None,
        execution_run_id=None,
        config_json={
            "note": note or "",
            "action": action,
            "row_ids": [str(row.id) for row in target_rows],
            "message": DEFAULT_BULK_FILL_MESSAGE,
            "reproducibility_mode": reproducibility_mode.value,
            "pipeline_profile_name": pipeline_selection.profile_name,
            "resolved_pipeline_config": pipeline_selection.resolved_config,
            "pipeline_config_hash": pipeline_selection.config_hash,
            "index_config_hash": pipeline_selection.index_config_hash,
        },
        summary_json={},
        error_detail=None,
    )
    session.add(request)
    session.flush()
    job_run = start_repro_run(
        session,
        storage=None,
        settings=settings or get_settings(),
        kind=ExecutionRunKind.BULK_FILL_JOB,
        mode=reproducibility_mode,
        tenant_id=case.tenant_id,
        case_id=case.id,
        user_id=user_id,
        pipeline_profile_name=pipeline_selection.profile_name,
        pipeline_config_hash=pipeline_selection.config_hash,
        index_config_hash=pipeline_selection.index_config_hash,
        runtime_config_hash=pipeline_selection.runtime_config_hash,
        inputs_json={
            "action": action,
            "note": note or "",
            "row_ids": [str(row.id) for row in target_rows],
        },
    )
    request.execution_run_id = job_run.execution_run.id
    event_type = {
        "initial": BulkFillEventType.CREATED,
        "retry_failed": BulkFillEventType.RETRY_CREATED,
        "resume": BulkFillEventType.RESUME_CREATED,
    }.get(action, BulkFillEventType.CREATED)
    _record_event(
        session,
        request=request,
        event_type=event_type,
        message=f"Bulk-fill request created for action {action}.",
        metadata={"parent_request_id": str(parent_request.id) if parent_request else None},
    )
    for row in target_rows:
        row_execution = BulkFillRowExecution(
            tenant_id=case.tenant_id,
            case_id=case.id,
            bulk_fill_request_id=request.id,
            questionnaire_row_id=row.id,
            answer_version_id=None,
            attempt_number=_attempt_number_for_row(session, row_id=row.id),
            status=BulkFillRowStatus.NOT_STARTED,
            diagnostics_json={},
            error_detail=None,
        )
        session.add(row_execution)
    session.flush()
    _refresh_request_summary(session, request)
    return request


def create_initial_bulk_fill_request(
    session: Session,
    *,
    case: RfxCase,
    questionnaire: Questionnaire,
    user_id: UUID,
    note: str | None,
    settings: Settings | None = None,
    pipeline_profile_name: str | None = None,
    pipeline_override: dict[str, object] | None = None,
    pipeline_selection: PipelineSelection | None = None,
    reproducibility_mode: ReproducibilityMode = ReproducibilityMode.BEST_EFFORT,
) -> BulkFillRequest:
    return create_bulk_fill_request(
        session,
        case=case,
        questionnaire=questionnaire,
        user_id=user_id,
        note=note,
        action="initial",
        target_rows=_ordered_questionnaire_rows(session, questionnaire_id=questionnaire.id),
        settings=settings,
        pipeline_profile_name=pipeline_profile_name,
        pipeline_override=pipeline_override,
        pipeline_selection=pipeline_selection,
        reproducibility_mode=reproducibility_mode,
    )


def retry_failed_bulk_fill_request(
    session: Session,
    *,
    case: RfxCase,
    questionnaire: Questionnaire,
    source_request: BulkFillRequest,
    user_id: UUID,
    settings: Settings | None = None,
) -> BulkFillRequest:
    if source_request.status not in RESUMABLE_BULK_FILL_STATUSES:
        raise ValidationFailure(
            f"Bulk-fill request {source_request.id} with status {source_request.status.value} cannot retry failed rows."
        )
    failed_row_ids = session.scalars(
        select(BulkFillRowExecution.questionnaire_row_id).where(
            BulkFillRowExecution.bulk_fill_request_id == source_request.id,
            BulkFillRowExecution.status == BulkFillRowStatus.FAILED,
        )
    ).all()
    if not failed_row_ids:
        raise ValidationFailure(f"Bulk-fill request {source_request.id} has no failed rows to retry.")
    target_rows = [
        row
        for row in _ordered_questionnaire_rows(session, questionnaire_id=questionnaire.id)
        if row.id in set(failed_row_ids)
    ]
    return create_bulk_fill_request(
        session,
        case=case,
        questionnaire=questionnaire,
        user_id=user_id,
        note="retry_failed",
        action="retry_failed",
        parent_request=source_request,
        target_rows=target_rows,
        settings=settings,
        reproducibility_mode=ReproducibilityMode(
            str(source_request.config_json.get("reproducibility_mode", "best_effort"))
        ),
    )


def resume_bulk_fill_request(
    session: Session,
    *,
    case: RfxCase,
    questionnaire: Questionnaire,
    source_request: BulkFillRequest,
    user_id: UUID,
    settings: Settings | None = None,
) -> BulkFillRequest:
    if source_request.status not in RESUMABLE_BULK_FILL_STATUSES:
        raise ValidationFailure(
            f"Bulk-fill request {source_request.id} with status {source_request.status.value} cannot be resumed."
        )
    resumable_row_ids = session.scalars(
        select(BulkFillRowExecution.questionnaire_row_id).where(
            BulkFillRowExecution.bulk_fill_request_id == source_request.id,
            BulkFillRowExecution.status.in_(
                {
                    BulkFillRowStatus.NOT_STARTED,
                    BulkFillRowStatus.RUNNING,
                    BulkFillRowStatus.FAILED,
                }
            ),
        )
    ).all()
    if not resumable_row_ids:
        raise ValidationFailure(f"Bulk-fill request {source_request.id} has no resumable rows.")
    target_rows = [
        row
        for row in _ordered_questionnaire_rows(session, questionnaire_id=questionnaire.id)
        if row.id in set(resumable_row_ids)
    ]
    return create_bulk_fill_request(
        session,
        case=case,
        questionnaire=questionnaire,
        user_id=user_id,
        note="resume",
        action="resume",
        parent_request=source_request,
        target_rows=target_rows,
        settings=settings,
        reproducibility_mode=ReproducibilityMode(
            str(source_request.config_json.get("reproducibility_mode", "best_effort"))
        ),
    )


def cancel_bulk_fill_request(
    session: Session,
    *,
    request: BulkFillRequest,
) -> BulkFillRequest:
    if request.status == BulkFillStatus.QUEUED:
        request.status = BulkFillStatus.CANCELLED
        request.finished_at = datetime.now(UTC)
        pending_rows = session.scalars(
            select(BulkFillRowExecution).where(
                BulkFillRowExecution.bulk_fill_request_id == request.id,
                BulkFillRowExecution.status == BulkFillRowStatus.NOT_STARTED,
            )
        ).all()
        for row_execution in pending_rows:
            row_execution.status = BulkFillRowStatus.CANCELLED
        _record_event(
            session,
            request=request,
            event_type=BulkFillEventType.CANCEL_REQUESTED,
            message="Cancellation requested while job was queued; job cancelled immediately.",
        )
        _record_event(
            session,
            request=request,
            event_type=BulkFillEventType.CANCELLED,
            message="Queued job cancelled before worker claim.",
        )
        _refresh_request_summary(session, request)
        return request
    if request.status == BulkFillStatus.RUNNING:
        request.status = BulkFillStatus.CANCEL_REQUESTED
        request.cancel_requested_at = datetime.now(UTC)
        _record_event(
            session,
            request=request,
            event_type=BulkFillEventType.CANCEL_REQUESTED,
            runner_id=request.runner_id,
            message="Cancellation requested while job was running.",
        )
        _refresh_request_summary(session, request)
        return request
    if request.status == BulkFillStatus.CANCEL_REQUESTED:
        raise ValidationFailure(f"Bulk-fill request {request.id} already has cancellation requested.")
    raise ValidationFailure(
        f"Bulk-fill request {request.id} with status {request.status.value} cannot be cancelled."
    )


def approve_answer_version(
    session: Session,
    *,
    case: RfxCase,
    row: QuestionnaireRow,
    answer_version_id: UUID,
) -> AnswerVersion:
    version = session.get(AnswerVersion, answer_version_id)
    if version is None:
        raise ValidationFailure(f"Answer version {answer_version_id} does not exist.")
    if version.case_id != case.id or version.questionnaire_row_id != row.id or version.tenant_id != case.tenant_id:
        raise ValidationFailure(
            f"Answer version {answer_version_id} cannot be approved for row {row.id} in case {case.id}."
        )
    retrieval_run = session.get(RetrievalRun, version.retrieval_run_id)
    if retrieval_run is None:
        raise ValidationFailure(
            f"Answer version {answer_version_id} is missing retrieval run {version.retrieval_run_id}."
        )
    snapshot_count = session.scalar(
        select(func.count())
        .select_from(RetrievalSnapshotItem)
        .where(RetrievalSnapshotItem.retrieval_run_id == retrieval_run.id)
    ) or 0
    if snapshot_count == 0:
        raise ValidationFailure(
            f"Answer version {answer_version_id} is missing retrieval snapshot evidence."
        )
    version.status = AnswerStatus.ACCEPTED
    row.approved_answer_version_id = version.id
    row.review_status = QuestionnaireRowStatus.APPROVED
    row.last_error_detail = None
    return version


def reject_row_answer(
    session: Session,
    *,
    case: RfxCase,
    row: QuestionnaireRow,
    answer_version_id: UUID | None,
) -> None:
    if answer_version_id is not None:
        version = session.get(AnswerVersion, answer_version_id)
        if version is None:
            raise ValidationFailure(f"Answer version {answer_version_id} does not exist.")
        if version.case_id != case.id or version.questionnaire_row_id != row.id or version.tenant_id != case.tenant_id:
            raise ValidationFailure(
                f"Answer version {answer_version_id} cannot be rejected for row {row.id} in case {case.id}."
            )
        if row.approved_answer_version_id == version.id:
            row.approved_answer_version_id = None
    row.review_status = QuestionnaireRowStatus.REJECTED


def _verify_claim(
    request: BulkFillRequest,
    *,
    runner_id: str,
    claim_id: str,
) -> None:
    if request.claim_id != claim_id or request.runner_id != runner_id:
        raise ValidationFailure(
            f"Bulk-fill request {request.id} claim mismatch: expected runner={runner_id} claim={claim_id}, observed runner={request.runner_id} claim={request.claim_id}."
        )


def _claim_request(
    session: Session,
    *,
    request_id: UUID,
    runner_id: str,
    execution_mode: str,
) -> BulkFillRequest | None:
    claim_id = uuid4().hex
    claimed_at = datetime.now(UTC)
    result = session.execute(
        update(BulkFillRequest)
        .where(
            BulkFillRequest.id == request_id,
            BulkFillRequest.status == BulkFillStatus.QUEUED,
        )
        .values(
            status=BulkFillStatus.RUNNING,
            claim_id=claim_id,
            runner_id=runner_id,
            execution_mode=execution_mode,
            claimed_at=claimed_at,
            started_at=claimed_at,
            heartbeat_at=claimed_at,
            error_detail=None,
        )
    )
    if result.rowcount != 1:
        return None
    request = session.get(BulkFillRequest, request_id)
    if request is None:
        raise ValidationFailure(f"Bulk-fill request {request_id} disappeared after claim.")
    _record_event(
        session,
        request=request,
        event_type=BulkFillEventType.CLAIMED,
        runner_id=runner_id,
        message="Worker claimed queued bulk-fill request.",
        metadata={"execution_mode": execution_mode, "claim_id": claim_id},
    )
    _record_event(
        session,
        request=request,
        event_type=BulkFillEventType.STARTED,
        runner_id=runner_id,
        message="Worker started bulk-fill execution.",
    )
    _refresh_request_summary(session, request)
    return request


def detect_orphaned_bulk_fill_requests(
    session: Session,
    *,
    stale_after: timedelta = STALE_CLAIM_AFTER,
) -> list[BulkFillRequest]:
    cutoff = datetime.now(UTC) - stale_after
    stale_requests = session.scalars(
        select(BulkFillRequest).where(
            BulkFillRequest.status.in_(
                {BulkFillStatus.RUNNING, BulkFillStatus.CANCEL_REQUESTED}
            ),
            BulkFillRequest.heartbeat_at.is_not(None),
            BulkFillRequest.heartbeat_at < cutoff,
        )
    ).all()
    for request in stale_requests:
        request.status = BulkFillStatus.ORPHANED
        request.stale_detected_at = datetime.now(UTC)
        request.finished_at = request.stale_detected_at
        request.error_detail = (
            request.error_detail
            or "Worker heartbeat expired; request marked orphaned and requires explicit recovery."
        )
        _record_event(
            session,
            request=request,
            event_type=BulkFillEventType.ORPHANED,
            runner_id=request.runner_id,
            message="Worker heartbeat expired; request marked orphaned.",
            metadata={"heartbeat_at": request.heartbeat_at.isoformat() if request.heartbeat_at else None},
        )
        _refresh_request_summary(session, request)
    return list(stale_requests)


def claim_next_bulk_fill_request(
    session: Session,
    *,
    runner_id: str,
    execution_mode: str,
) -> BulkFillRequest | None:
    detect_orphaned_bulk_fill_requests(session)
    candidate_ids = session.scalars(
        select(BulkFillRequest.id)
        .where(BulkFillRequest.status == BulkFillStatus.QUEUED)
        .order_by(BulkFillRequest.created_at.asc())
    ).all()
    for request_id in candidate_ids:
        request = _claim_request(
            session,
            request_id=request_id,
            runner_id=runner_id,
            execution_mode=execution_mode,
        )
        if request is not None:
            return request
    return None


def _mark_pending_rows_cancelled(session: Session, request: BulkFillRequest) -> None:
    pending = session.scalars(
        select(BulkFillRowExecution).where(
            BulkFillRowExecution.bulk_fill_request_id == request.id,
            BulkFillRowExecution.status == BulkFillRowStatus.NOT_STARTED,
        )
    ).all()
    for row_execution in pending:
        row_execution.status = BulkFillRowStatus.CANCELLED


def _finalize_request_status(session: Session, request: BulkFillRequest, *, runner_id: str) -> None:
    failed_count = session.scalar(
        select(func.count())
        .select_from(BulkFillRowExecution)
        .where(
            BulkFillRowExecution.bulk_fill_request_id == request.id,
            BulkFillRowExecution.status == BulkFillRowStatus.FAILED,
        )
    ) or 0
    request.finished_at = datetime.now(UTC)
    if request.status == BulkFillStatus.CANCEL_REQUESTED:
        request.status = BulkFillStatus.CANCELLED
        _mark_pending_rows_cancelled(session, request)
        _record_event(
            session,
            request=request,
            event_type=BulkFillEventType.CANCELLED,
            runner_id=runner_id,
            message="Bulk-fill cancelled after cooperative stop at row boundary.",
        )
    else:
        request.status = (
            BulkFillStatus.COMPLETED_WITH_FAILURES if failed_count else BulkFillStatus.COMPLETED
        )
        _record_event(
            session,
            request=request,
            event_type=BulkFillEventType.COMPLETED,
            runner_id=runner_id,
            message=f"Bulk-fill finished with status {request.status.value}.",
            metadata={"failed_rows": failed_count},
        )
    _refresh_request_summary(session, request)
    if request.execution_run_id is not None:
        run = session.get(ExecutionRun, request.execution_run_id)
        if run is not None:
            finish_execution_run(
                run,
                outputs_json={
                    "bulk_fill_request_id": str(request.id),
                    "status": request.status.value,
                    "failed_rows": failed_count,
                },
            )


def _execute_claimed_request(
    container: ServiceContainer,
    *,
    request_id: UUID,
    runner_id: str,
    claim_id: str,
) -> None:
    _ensure_bulk_fill_generation_available(container)
    while True:
        with container.session_factory() as session:
            request = session.get(BulkFillRequest, request_id)
            if request is None:
                raise ValidationFailure(f"Bulk-fill request {request_id} disappeared during execution.")
            _verify_claim(request, runner_id=runner_id, claim_id=claim_id)
            if request.status == BulkFillStatus.CANCEL_REQUESTED:
                _finalize_request_status(session, request, runner_id=runner_id)
                session.commit()
                return
            next_row_execution = session.scalar(
                select(BulkFillRowExecution)
                .join(QuestionnaireRow, QuestionnaireRow.id == BulkFillRowExecution.questionnaire_row_id)
                .where(
                    BulkFillRowExecution.bulk_fill_request_id == request.id,
                    BulkFillRowExecution.status == BulkFillRowStatus.NOT_STARTED,
                )
                .order_by(QuestionnaireRow.source_row_number.asc())
            )
            if next_row_execution is None:
                _finalize_request_status(session, request, runner_id=runner_id)
                session.commit()
                return
            row = session.get(QuestionnaireRow, next_row_execution.questionnaire_row_id)
            if row is None:
                raise ValidationFailure(
                    f"Bulk-fill row execution {next_row_execution.id} references missing questionnaire row {next_row_execution.questionnaire_row_id}."
                )
            if row.approved_answer_version_id is not None:
                next_row_execution.status = BulkFillRowStatus.SKIPPED
                next_row_execution.error_detail = None
                next_row_execution.diagnostics_json = {
                    "skip_reason": "row_approved_before_execution",
                    "approved_answer_version_id": str(row.approved_answer_version_id),
                }
                request.heartbeat_at = datetime.now(UTC)
                _record_event(
                    session,
                    request=request,
                    event_type=BulkFillEventType.PROGRESS,
                    runner_id=runner_id,
                    row_execution=next_row_execution,
                    message=f"Skipped row {row.source_row_id} because it already has an approved answer.",
                    metadata={"approved_answer_version_id": str(row.approved_answer_version_id)},
                )
                _refresh_request_summary(session, request)
                session.commit()
                continue
            next_row_execution.status = BulkFillRowStatus.RUNNING
            if row.approved_answer_version_id is None:
                row.review_status = QuestionnaireRowStatus.RUNNING
            request.heartbeat_at = datetime.now(UTC)
            _record_event(
                session,
                request=request,
                event_type=BulkFillEventType.ROW_STARTED,
                runner_id=runner_id,
                row_execution=next_row_execution,
                message=f"Started row {row.source_row_id}.",
            )
            _refresh_request_summary(session, request)
            session.commit()
            current_row_execution_id = next_row_execution.id

        with container.session_factory() as session:
            request = session.get(BulkFillRequest, request_id)
            current_row_execution = session.get(BulkFillRowExecution, current_row_execution_id)
            if request is None or current_row_execution is None:
                raise ValidationFailure(f"Bulk-fill request {request_id} lost persisted execution state.")
            _verify_claim(request, runner_id=runner_id, claim_id=claim_id)
            case = session.get(RfxCase, request.case_id)
            row = session.get(QuestionnaireRow, current_row_execution.questionnaire_row_id)
            if case is None or row is None:
                raise ValidationFailure(f"Bulk-fill request {request.id} lost case/row provenance.")
            row_run = start_repro_run(
                session,
                storage=None,
                settings=container.settings,
                kind=ExecutionRunKind.BULK_FILL_ROW_ATTEMPT,
                mode=ReproducibilityMode(str(request.config_json.get("reproducibility_mode", "best_effort"))),
                tenant_id=request.tenant_id,
                case_id=request.case_id,
                user_id=request.requested_by_user_id,
                parent_run_id=request.execution_run_id,
                pipeline_profile_name=cast(str | None, request.config_json.get("pipeline_profile_name")),
                pipeline_config_hash=cast(str | None, request.config_json.get("pipeline_config_hash")),
                index_config_hash=cast(str | None, request.config_json.get("index_config_hash")),
                runtime_config_hash=None,
                inputs_json={
                    "bulk_fill_request_id": str(request.id),
                    "row_execution_id": str(current_row_execution.id),
                    "questionnaire_row_id": str(row.id),
                },
            ).execution_run
            current_row_execution.execution_run_id = row_run.id
            pipeline_selection = resolve_pipeline_selection(
                container.settings,
                pinned_config=request.config_json["resolved_pipeline_config"],  # type: ignore[arg-type]
                pinned_profile_name=request.config_json.get("pipeline_profile_name"),  # type: ignore[arg-type]
            )
            try:
                result = draft_answer_for_row(
                    session,
                    ai_service=container.ai_service,
                    case=case,
                    row=row,
                    user_id=request.requested_by_user_id,
                    user_message=str(request.config_json.get("message", DEFAULT_BULK_FILL_MESSAGE)),
                    thread=None,
                    settings=container.settings,
                    pipeline_selection=pipeline_selection,
                    reproducibility_mode=ReproducibilityMode(
                        str(request.config_json.get("reproducibility_mode", "best_effort"))
                    ),
                    execution_run_kind=ExecutionRunKind.BULK_FILL_ROW_ATTEMPT,
                    parent_run_id=request.execution_run_id,
                    existing_execution_run=row_run,
                    render_with_thread_history=False,
                )
                retrieval_run = session.get(RetrievalRun, result.answer_version.retrieval_run_id)
                if retrieval_run is None:
                    raise ValidationFailure(
                        f"Bulk-filled answer version {result.answer_version.id} is missing retrieval run."
                    )
                snapshot_count = session.scalar(
                    select(func.count())
                    .select_from(RetrievalSnapshotItem)
                    .where(RetrievalSnapshotItem.retrieval_run_id == retrieval_run.id)
                ) or 0
                if snapshot_count == 0:
                    raise ValidationFailure(
                        f"Bulk-filled answer version {result.answer_version.id} is missing retrieval snapshot evidence."
                    )
                retrieval_execution = retrieval_run.request_context.get("retrieval_execution", {})
                sufficiency = (
                    retrieval_execution.get("sufficiency", {})
                    if isinstance(retrieval_execution, dict)
                    and isinstance(retrieval_execution.get("sufficiency", {}), dict)
                    else {}
                )
                current_row_execution.status = BulkFillRowStatus.DRAFTED
                current_row_execution.answer_version_id = result.answer_version.id
                current_row_execution.error_detail = None
                current_row_execution.diagnostics_json = {
                    "answer_version_id": str(result.answer_version.id),
                    "retrieval_sufficiency": sufficiency.get("status"),
                }
                row.review_status = QuestionnaireRowStatus.NEEDS_REVIEW
                row.last_error_detail = None
                _record_event(
                    session,
                    request=request,
                    event_type=BulkFillEventType.ROW_SUCCEEDED,
                    runner_id=runner_id,
                    row_execution=current_row_execution,
                    message=f"Row {row.source_row_id} drafted successfully.",
                    metadata={"answer_version_id": str(result.answer_version.id)},
                )
            except Exception as exc:
                fail_execution_run(
                    row_run,
                    error_detail=str(exc),
                    diagnostics_json={"phase": "bulk_fill_row_execution"},
                )
                current_row_execution.status = BulkFillRowStatus.FAILED
                current_row_execution.error_detail = str(exc)
                current_row_execution.diagnostics_json = {
                    "error_type": exc.__class__.__name__,
                }
                if row.approved_answer_version_id is None:
                    row.review_status = QuestionnaireRowStatus.FAILED
                row.last_error_detail = str(exc)
                _record_event(
                    session,
                    request=request,
                    event_type=BulkFillEventType.ROW_FAILED,
                    runner_id=runner_id,
                    row_execution=current_row_execution,
                    message=f"Row {row.source_row_id} failed during bulk-fill.",
                    metadata={"error_detail": str(exc)},
                )
            request.heartbeat_at = datetime.now(UTC)
            _record_event(
                session,
                request=request,
                event_type=BulkFillEventType.PROGRESS,
                runner_id=runner_id,
                message="Bulk-fill progress updated after row execution.",
                metadata=request.summary_json,
            )
            _refresh_request_summary(session, request)
            session.commit()


def execute_bulk_fill_request(
    container: ServiceContainer,
    *,
    request_id: UUID,
    runner_id: str = "inline-test-runner",
    execution_mode: str = "inline",
) -> None:
    try:
        with container.session_factory() as session:
            request = _claim_request(
                session,
                request_id=request_id,
                runner_id=runner_id,
                execution_mode=execution_mode,
            )
            if request is None:
                existing = session.get(BulkFillRequest, request_id)
                if existing is None:
                    raise ValidationFailure(f"Bulk-fill request {request_id} does not exist.")
                if existing.status == BulkFillStatus.ORPHANED:
                    raise ValidationFailure(
                        f"Bulk-fill request {request_id} is orphaned and requires explicit resume."
                    )
                raise ValidationFailure(
                    f"Bulk-fill request {request_id} cannot be claimed from status {existing.status.value}."
                )
            claim_id = request.claim_id
            if claim_id is None:
                raise ValidationFailure(f"Bulk-fill request {request.id} was claimed without a claim ID.")
            session.commit()
        _execute_claimed_request(
            container,
            request_id=request_id,
            runner_id=runner_id,
            claim_id=claim_id,
        )
    except Exception as exc:
        with container.session_factory() as session:
            request = session.get(BulkFillRequest, request_id)
            if request is not None and request.status not in {
                BulkFillStatus.COMPLETED,
                BulkFillStatus.COMPLETED_WITH_FAILURES,
                BulkFillStatus.CANCELLED,
                BulkFillStatus.ORPHANED,
            }:
                request.status = BulkFillStatus.FAILED
                request.finished_at = datetime.now(UTC)
                request.error_detail = str(exc)
                if request.execution_run_id is not None:
                    run = session.get(ExecutionRun, request.execution_run_id)
                    if run is not None:
                        fail_execution_run(run, error_detail=str(exc))
                running_rows = session.scalars(
                    select(BulkFillRowExecution).where(
                        BulkFillRowExecution.bulk_fill_request_id == request.id,
                        BulkFillRowExecution.status == BulkFillRowStatus.RUNNING,
                    )
                ).all()
                for row_execution in running_rows:
                    row_execution.status = BulkFillRowStatus.FAILED
                    row_execution.error_detail = str(exc)
                _record_event(
                    session,
                    request=request,
                    event_type=BulkFillEventType.FAILED,
                    runner_id=request.runner_id,
                    message="Bulk-fill execution failed.",
                    metadata={"error_detail": str(exc)},
                )
                _refresh_request_summary(session, request)
                session.commit()


def run_bulk_fill_worker_once(
    container: ServiceContainer,
    *,
    runner_id: str,
    execution_mode: str = "worker",
) -> UUID | None:
    _ensure_bulk_fill_generation_available(container)
    with container.session_factory() as session:
        request = claim_next_bulk_fill_request(
            session,
            runner_id=runner_id,
            execution_mode=execution_mode,
        )
        if request is None:
            session.commit()
            return None
        claim_id = request.claim_id
        request_id = request.id
        if claim_id is None:
            raise ValidationFailure(f"Bulk-fill request {request.id} was claimed without a claim ID.")
        session.commit()
    _execute_claimed_request(
        container,
        request_id=request_id,
        runner_id=runner_id,
        claim_id=claim_id,
    )
    return request_id
