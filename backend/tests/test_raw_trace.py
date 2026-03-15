from __future__ import annotations

import pytest
from sqlalchemy import select

from app.exceptions import ValidationFailure
from app.models.entities import Questionnaire, QuestionnaireRow
from app.models.enums import ExecutionRunKind
from app.services.ai import StubAIService
from app.services.answers import (
    draft_answer_for_row,
    raw_trace_for_latest_attempt,
    raw_trace_for_selected_answer_version,
)
from app.services.bulk_fill import create_initial_bulk_fill_request, run_bulk_fill_worker_once
from app.services.identity import ensure_local_identity
from app.services.seed import import_historical_corpus
from tests.seed_paths import seed_data_root
from tests.test_invariants import create_case_with_questionnaire


class RenderingFailureAIService(StubAIService):
    def render_answer(self, **kwargs):  # type: ignore[override]
        raise ValidationFailure("Synthetic render failure for raw trace coverage.")


def _prepare_case_with_row(session, *, container, repo_root, settings):
    context = ensure_local_identity(session, settings)
    import_historical_corpus(
        session,
        ai_service=StubAIService(),
        storage=container.storage,
        tenant_id=context.tenant.id,
        base_path=seed_data_root(repo_root),
    )
    case = create_case_with_questionnaire(
        session,
        repo_root=repo_root,
        tenant_id=context.tenant.id,
        user_id=context.user.id,
        storage=container.storage,
        ai_service=StubAIService(),
    )
    row = session.scalar(
        select(QuestionnaireRow)
        .where(QuestionnaireRow.case_id == case.id)
        .order_by(QuestionnaireRow.source_row_number.asc())
    )
    assert row is not None
    return context, case, row


def test_raw_trace_selected_version_returns_planning_and_rendering(
    session,
    container,
    repo_root,
    settings,
) -> None:
    context, case, row = _prepare_case_with_row(
        session,
        container=container,
        repo_root=repo_root,
        settings=settings,
    )
    result = draft_answer_for_row(
        session,
        ai_service=StubAIService(),
        case=case,
        row=row,
        user_id=context.user.id,
        user_message="Draft the answer.",
        thread=None,
    )

    trace = raw_trace_for_selected_answer_version(
        session,
        row=row,
        answer_version=result.answer_version,
    )

    assert trace.scope == "selected_answer_version"
    assert trace.answer_version == result.answer_version
    assert trace.execution_run is not None
    assert trace.execution_run.id == result.answer_version.execution_run_id
    assert trace.generation_path == "two_stage_plan_render"
    assert trace.planning_stage.availability == "available"
    assert trace.planning_stage.source_type == "current_run"
    assert trace.planning_stage.source_answer_version == result.answer_version
    assert trace.rendering_stage.availability == "available"
    assert trace.rendering_stage.source_type == "current_run"
    assert trace.rendering_stage.source_answer_version == result.answer_version
    assert trace.rendering_stage.model_invocation is not None
    assert trace.rendering_stage.model_invocation.id == result.answer_version.model_invocation_id


def test_raw_trace_selected_version_marks_style_only_plan_reuse(
    session,
    container,
    repo_root,
    settings,
) -> None:
    context, case, row = _prepare_case_with_row(
        session,
        container=container,
        repo_root=repo_root,
        settings=settings,
    )
    first = draft_answer_for_row(
        session,
        ai_service=StubAIService(),
        case=case,
        row=row,
        user_id=context.user.id,
        user_message="Draft the answer.",
        thread=None,
    )
    second = draft_answer_for_row(
        session,
        ai_service=StubAIService(),
        case=case,
        row=row,
        user_id=context.user.id,
        user_message="Make it shorter.",
        thread=first.thread,
        revision_mode_override="style_only",
    )

    trace = raw_trace_for_selected_answer_version(
        session,
        row=row,
        answer_version=second.answer_version,
    )

    assert trace.generation_path == "render_only_reuse_plan"
    assert trace.planning_stage.availability == "available"
    assert trace.planning_stage.source_type == "reused_prior_plan"
    assert trace.planning_stage.source_answer_version == first.answer_version
    assert trace.planning_stage.source_execution_run is not None
    assert trace.planning_stage.source_execution_run.id == first.answer_version.execution_run_id
    assert trace.rendering_stage.availability == "available"
    assert trace.rendering_stage.source_answer_version == second.answer_version


def test_raw_trace_latest_attempt_returns_partial_trace_for_failed_render(
    session,
    container,
    repo_root,
    settings,
) -> None:
    context, case, row = _prepare_case_with_row(
        session,
        container=container,
        repo_root=repo_root,
        settings=settings,
    )

    with pytest.raises(ValidationFailure, match="Synthetic render failure"):
        draft_answer_for_row(
            session,
            ai_service=RenderingFailureAIService(),
            case=case,
            row=row,
            user_id=context.user.id,
            user_message="Draft the answer.",
            thread=None,
        )

    trace = raw_trace_for_latest_attempt(session, row=row)

    assert trace.scope == "latest_attempt"
    assert trace.execution_run is not None
    assert trace.execution_run.kind == ExecutionRunKind.ROW_DRAFT
    assert trace.answer_version is None
    assert trace.latest_attempt_state == "failed_no_answer"
    assert "Synthetic render failure" in (trace.failure_detail or "")
    assert trace.planning_stage.availability == "available"
    assert trace.rendering_stage.availability == "missing"


def test_raw_trace_latest_attempt_uses_bulk_fill_row_run(
    session,
    container,
    repo_root,
    settings,
) -> None:
    context, case, row = _prepare_case_with_row(
        session,
        container=container,
        repo_root=repo_root,
        settings=settings,
    )
    questionnaire = session.scalar(
        select(Questionnaire).where(Questionnaire.case_id == case.id)
    )
    assert questionnaire is not None

    request = create_initial_bulk_fill_request(
        session,
        case=case,
        questionnaire=questionnaire,
        user_id=context.user.id,
        note="raw-trace coverage",
        settings=settings,
    )
    session.commit()

    claimed_request_id = run_bulk_fill_worker_once(
        container,
        runner_id="raw-trace-worker",
        execution_mode="test_worker",
    )
    assert claimed_request_id == request.id

    session.expire_all()
    refreshed_row = session.get(QuestionnaireRow, row.id)
    assert refreshed_row is not None

    trace = raw_trace_for_latest_attempt(session, row=refreshed_row)

    assert trace.execution_run is not None
    assert trace.execution_run.kind == ExecutionRunKind.BULK_FILL_ROW_ATTEMPT
    assert trace.latest_attempt_state == "answer_available"
    assert trace.answer_version is not None
    assert trace.planning_stage.availability == "available"
    assert trace.rendering_stage.availability == "available"
