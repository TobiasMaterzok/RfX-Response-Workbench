from __future__ import annotations

import json
import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal, cast
from uuid import UUID

from pydantic import ValidationError as PydanticValidationError
from sqlalchemy import case, func, select
from sqlalchemy.orm import Session

from app.config import Settings, get_settings
from app.exceptions import ScopeViolation, ValidationFailure
from app.models.entities import (
    AnswerVersion,
    ChatMessage,
    ChatThread,
    EvidenceLink,
    ExecutionRun,
    ModelInvocation,
    QuestionnaireRow,
    RetrievalRun,
    RetrievalSnapshotItem,
    RfxCase,
)
from app.models.enums import (
    AnswerStatus,
    ExecutionRunKind,
    LLMCaptureStatus,
    MessageRole,
    ModelInvocationKind,
    QuestionnaireRowStatus,
    ReproducibilityMode,
)
from app.pipeline.config import PipelineSelection, resolve_pipeline_selection
from app.prompts.answer_planning import ANSWER_PLANNING_PROMPT_VERSION
from app.prompts.answer_rendering import ANSWER_RENDERING_PROMPT_VERSION
from app.schemas.answer_plan import AnswerPlan, NormalizedEvidenceItem
from app.services.ai import AIService, OpenAIAIService, openai_sdk_version
from app.services.answer_prompting import (
    normalize_evidence_pack,
    normalize_target_language,
    validate_answer_plan,
    validate_rendered_answer,
)
from app.services.hashing import sha256_text
from app.services.packing import PackedEvidence, pack_normalized_evidence
from app.services.reproducibility import (
    assert_execution_run_consistency,
    fail_execution_run,
    finish_execution_run,
    record_model_invocation,
    start_repro_run,
)
from app.services.retrieval import build_retrieval_request, build_retrieval_run

STYLE_ONLY_PATTERNS = (
    re.compile(r"\bmake (it )?(shorter|longer|more formal|more concise|crisper|clearer)\b"),
    re.compile(r"\btranslate (it )?to (german|english|deutsch|englisch)\b"),
    re.compile(r"\b(rewrite|rephrase|polish|tighten|simplify|proofread|clean up)\b"),
    re.compile(r"\b(change|adjust|improve) (the )?(tone|style|wording|phrasing|format)\b"),
)
CONTENT_CHANGE_PATTERNS = (
    re.compile(r"\b(add|mention|include|address|cover|emphas(?:i|e)ze|highlight|focus on)\b"),
    re.compile(r"\b(gdpr|hosting|governance|integrations?|timeline|risks?|rollout|security|privacy|residency)\b"),
)
REVISION_CLASSIFIER_VERSION = "revision_classifier.v2"
TWO_STAGE_PLAN_RENDER_PATH = "two_stage_plan_render"
RENDER_ONLY_REUSE_PLAN_PATH = "render_only_reuse_plan"


@dataclass(frozen=True)
class DraftResult:
    thread: ChatThread
    answer_version: AnswerVersion
    messages: list[ChatMessage]
    evidence_items: list[RetrievalSnapshotItem]


@dataclass(frozen=True)
class RevisionDecision:
    mode: str
    reason: str


RevisionModeOverride = Literal["style_only", "content_change"]


def _title_for_row(row: QuestionnaireRow) -> str:
    return f"Row {row.source_row_number}: {row.question_raw[:80]}"


def _chat_message_ordering():
    role_order = case(
        (ChatMessage.role == MessageRole.SYSTEM, 0),
        (ChatMessage.role == MessageRole.USER, 1),
        else_=2,
    )
    return (
        ChatMessage.created_at.asc(),
        role_order.asc(),
        ChatMessage.id.asc(),
    )


def list_thread_messages(session: Session, *, thread_id: UUID) -> list[ChatMessage]:
    return list(
        session.scalars(
            select(ChatMessage)
            .where(ChatMessage.thread_id == thread_id)
            .order_by(*_chat_message_ordering())
        ).all()
    )


def _classify_revision_mode(
    *,
    user_message: str,
    has_previous_answer: bool,
) -> RevisionDecision:
    if not has_previous_answer:
        return RevisionDecision(
            mode="initial_draft",
            reason="no_previous_answer_version",
        )
    normalized = " ".join(user_message.lower().split())
    if any(pattern.search(normalized) for pattern in CONTENT_CHANGE_PATTERNS):
        return RevisionDecision(
            mode="content_change",
            reason="matched_content_change_pattern",
        )
    if any(pattern.search(normalized) for pattern in STYLE_ONLY_PATTERNS):
        return RevisionDecision(
            mode="style_only",
            reason="matched_style_only_pattern",
        )
    return RevisionDecision(
        mode="content_change",
        reason="default_content_change_fallback",
    )


def _explicit_revision_mode(
    *,
    user_message: str,
    has_previous_answer: bool,
    override: RevisionModeOverride,
) -> RevisionDecision:
    if override == "style_only":
        if not has_previous_answer:
            raise ValidationFailure("Style-only revision requires a previous answer version.")
        return RevisionDecision(
            mode="style_only",
            reason="explicit_style_only_override",
        )
    if override == "content_change" and not has_previous_answer:
        return RevisionDecision(
            mode="initial_draft",
            reason="no_previous_answer_version",
        )
    return RevisionDecision(
        mode="content_change",
        reason="explicit_content_change_override",
    )


def _load_latest_answer_version(session: Session, *, thread_id) -> AnswerVersion | None:
    return session.scalar(
        select(AnswerVersion)
        .where(AnswerVersion.chat_thread_id == thread_id)
        .order_by(AnswerVersion.version_number.desc())
    )


def _render_thread_history(messages: Sequence[ChatMessage]) -> list[tuple[str, str]]:
    history: list[tuple[str, str]] = []
    for message in messages:
        role = message.role.value
        if role not in {"user", "assistant"}:
            raise ValidationFailure(
                f"Chat message {message.id} has unsupported rendering role {role!r}."
            )
        history.append((role, message.content))
    return history


def _style_only_render_thread_history(
    session: Session,
    *,
    row: QuestionnaireRow,
    messages: Sequence[ChatMessage],
) -> list[tuple[str, str]]:
    del session, row
    return _render_thread_history(messages)


def _evidence_citations(item: RetrievalSnapshotItem) -> list[str] | None:
    metadata = item.metadata_json
    citations = metadata.get("citations")
    if isinstance(citations, list) and citations and all(isinstance(entry, str) for entry in citations):
        return [str(entry) for entry in citations]
    provenance = metadata.get("provenance")
    if isinstance(provenance, dict):
        page_number = provenance.get("page_number")
        if item.source_label == "raw_current_pdf" and isinstance(page_number, int):
            return [f"Page {page_number}"]
        source_row_id = provenance.get("source_row_id")
        if item.source_label == "historical_exemplar" and isinstance(source_row_id, str):
            return [source_row_id]
    return None


def _normalized_evidence_item(item: RetrievalSnapshotItem, *, normalized_id: str) -> NormalizedEvidenceItem:
    usage_policy = {
        "current_case_facts": "scope_context",
        "raw_current_pdf": "scope_context",
        "product_truth": "factual_support",
        "historical_exemplar": "pattern_only",
    }.get(item.source_label)
    if usage_policy is None:
        raise ValidationFailure(
            f"Retrieval snapshot item {item.id} has unsupported source label {item.source_label!r}."
        )
    citations = _evidence_citations(item)
    return NormalizedEvidenceItem(
        id=normalized_id,
        layer=cast(
            Literal[
                "current_case_facts",
                "raw_current_pdf",
                "product_truth",
                "historical_exemplar",
            ],
            item.source_label,
        ),
        title=item.source_title,
        text=item.excerpt,
        usage_policy=cast(
            Literal[
                "factual_support",
                "scope_context",
                "style_only",
                "pattern_only",
                "factual_allowed",
            ],
            usage_policy,
        ),
        source_kind=item.source_kind.value,
        product_name=item.source_title if item.source_label == "product_truth" else None,
        citations=citations,
    )


def _normalized_evidence_items(
    evidence_items: Sequence[RetrievalSnapshotItem],
) -> list[NormalizedEvidenceItem]:
    prefixes = {
        "current_case_facts": "CF",
        "raw_current_pdf": "PDF",
        "product_truth": "PT",
        "historical_exemplar": "HX",
    }
    counts: dict[str, int] = {
        "current_case_facts": 0,
        "raw_current_pdf": 0,
        "product_truth": 0,
        "historical_exemplar": 0,
    }
    normalized: list[NormalizedEvidenceItem] = []
    for item in evidence_items:
        if item.source_label not in prefixes:
            raise ValidationFailure(
                f"Retrieval snapshot item {item.id} has unsupported source label {item.source_label!r}."
            )
        counts[item.source_label] += 1
        normalized.append(
            _normalized_evidence_item(
                item,
                normalized_id=f"{prefixes[item.source_label]}{counts[item.source_label]}",
            )
        )
    return normalized


def _evidence_pack_hash(evidence_items: Sequence[RetrievalSnapshotItem]) -> str:
    return sha256_text(
        json.dumps(
            [
                {
                    "source_label": item.source_label,
                    "source_id": str(item.source_id),
                    "rank": item.rank,
                }
                for item in evidence_items
            ],
            sort_keys=True,
            separators=(",", ":"),
        )
    )


def _load_retrieval_snapshot_items(
    session: Session,
    *,
    retrieval_run_id,
) -> list[RetrievalSnapshotItem]:
    return list(
        session.scalars(
            select(RetrievalSnapshotItem)
            .where(RetrievalSnapshotItem.retrieval_run_id == retrieval_run_id)
            .order_by(RetrievalSnapshotItem.rank.asc())
        ).all()
    )


def _pipeline_selection_for_retrieval_run(
    *,
    retrieval_run: RetrievalRun,
    settings: Settings,
) -> PipelineSelection:
    pipeline = retrieval_run.request_context.get("pipeline")
    if not isinstance(pipeline, dict):
        raise ValidationFailure(
            f"Retrieval run {retrieval_run.id} is missing pipeline provenance."
        )
    resolved_config = pipeline.get("resolved_config")
    if not isinstance(resolved_config, dict):
        raise ValidationFailure(
            f"Retrieval run {retrieval_run.id} is missing resolved pipeline config provenance."
        )
    profile_name = pipeline.get("profile_name")
    if profile_name is not None and not isinstance(profile_name, str):
        raise ValidationFailure(
            f"Retrieval run {retrieval_run.id} has malformed pipeline profile provenance."
        )
    return resolve_pipeline_selection(
        settings,
        pinned_config=cast(dict[str, object], resolved_config),
        pinned_profile_name=cast(str | None, profile_name),
    )


def _load_execution_run_for_answer(
    session: Session,
    *,
    answer_version: AnswerVersion,
) -> ExecutionRun:
    if answer_version.execution_run_id is None:
        raise ValidationFailure(
            f"Answer version {answer_version.id} is missing execution-run provenance."
        )
    execution_run = session.get(ExecutionRun, answer_version.execution_run_id)
    if execution_run is None:
        raise ValidationFailure(
            f"Answer version {answer_version.id} references missing execution run {answer_version.execution_run_id}."
        )
    return execution_run


def _planning_invocation_on_run(
    session: Session,
    *,
    execution_run_id,
) -> ModelInvocation | None:
    invocations = session.scalars(
        select(ModelInvocation)
        .where(ModelInvocation.execution_run_id == execution_run_id)
        .order_by(ModelInvocation.created_at.asc())
    ).all()
    return next(
        (
            invocation
            for invocation in invocations
            if invocation.kind == ModelInvocationKind.ANSWER_GENERATION
            and invocation.metadata_json.get("prompt_family") == "answer_planning"
        ),
        None,
    )


def _source_planning_invocation_for_answer(
    session: Session,
    *,
    answer_version: AnswerVersion,
) -> ModelInvocation:
    execution_run = _load_execution_run_for_answer(session, answer_version=answer_version)
    for key in ("source_planning_model_invocation_id", "planning_model_invocation_id"):
        invocation_id = execution_run.outputs_json.get(key)
        if not isinstance(invocation_id, str):
            continue
        invocation = session.get(ModelInvocation, UUID(invocation_id))
        if (
            invocation is not None
            and invocation.kind == ModelInvocationKind.ANSWER_GENERATION
            and invocation.metadata_json.get("prompt_family") == "answer_planning"
        ):
            return invocation
    invocation = _planning_invocation_on_run(session, execution_run_id=execution_run.id)
    if invocation is None:
        raise ValidationFailure(
            "Style-only revision cannot reuse the prior answer plan safely. Use Regenerate to create a new answer."
        )
    return invocation


def _answer_plan_from_invocation(invocation: ModelInvocation) -> AnswerPlan:
    if invocation.response_payload_text is None:
        raise ValidationFailure(
            f"Planning invocation {invocation.id} is missing its stored AnswerPlan payload."
        )
    try:
        payload = json.loads(invocation.response_payload_text)
    except json.JSONDecodeError as exc:
        raise ValidationFailure(
            f"Planning invocation {invocation.id} stored invalid AnswerPlan JSON."
        ) from exc
    try:
        return AnswerPlan.model_validate(payload)
    except PydanticValidationError as exc:
        raise ValidationFailure(
            f"Planning invocation {invocation.id} stored an invalid AnswerPlan payload: {exc}"
        ) from exc


def draft_answer_for_row(
    session: Session,
    *,
    ai_service: AIService,
    case: RfxCase,
    row: QuestionnaireRow,
    user_id,
    user_message: str,
    thread: ChatThread | None,
    settings: Settings | None = None,
    pipeline_profile_name: str | None = None,
    pipeline_override: dict[str, object] | None = None,
    pipeline_selection: PipelineSelection | None = None,
    reproducibility_mode: ReproducibilityMode = ReproducibilityMode.BEST_EFFORT,
    revision_mode_override: RevisionModeOverride | None = None,
    execution_run_kind: ExecutionRunKind | None = None,
    parent_run_id=None,
    existing_execution_run=None,
    render_with_thread_history: bool = True,
) -> DraftResult:
    if row.case_id != case.id or row.tenant_id != case.tenant_id:
        raise ScopeViolation("Questionnaire row does not belong to the selected case.")
    if thread is None:
        thread = ChatThread(
            tenant_id=case.tenant_id,
            case_id=case.id,
            questionnaire_row_id=row.id,
            created_by_user_id=user_id,
            title=_title_for_row(row),
        )
        session.add(thread)
        session.flush()
    elif thread.case_id != case.id or thread.questionnaire_row_id != row.id:
        raise ScopeViolation("Chat thread cannot mix cases or questionnaire rows.")

    prior_messages = list_thread_messages(session, thread_id=thread.id)
    latest_answer_version = _load_latest_answer_version(session, thread_id=thread.id)
    has_previous_answer = latest_answer_version is not None
    revision_decision = (
        _explicit_revision_mode(
            user_message=user_message,
            has_previous_answer=has_previous_answer,
            override=revision_mode_override,
        )
        if revision_mode_override is not None
        else _classify_revision_mode(
            user_message=user_message,
            has_previous_answer=has_previous_answer,
        )
    )
    revision_mode = revision_decision.mode
    retrieval_action = "reuse_previous_snapshot" if revision_mode == "style_only" else "refresh_retrieval"
    retrieval_action_reason = (
        "style_only_revision_reuses_previous_snapshot"
        if revision_mode == "style_only"
        else "new_or_content_change_requires_refresh"
    )

    session.add(
        ChatMessage(
            tenant_id=case.tenant_id,
            case_id=case.id,
            questionnaire_row_id=row.id,
            thread_id=thread.id,
            role=MessageRole.USER,
            content=user_message,
        )
    )
    thread.updated_at = datetime.now(UTC)
    session.flush()

    effective_settings = settings or get_settings()
    pipeline: PipelineSelection
    retrieval_run: RetrievalRun
    evidence_items: list[RetrievalSnapshotItem]
    normalized_evidence_json: str
    packed_evidence: PackedEvidence
    target_language: Literal["de", "en"]
    output_mode: Literal["customer_facing", "grounded_review"]
    answer_plan: AnswerPlan
    evidence_pack_hash: str
    planning_invocation = None
    source_planning_model_invocation_id: str | None = None
    reused_answer_version_id: str | None = None
    generation_path = TWO_STAGE_PLAN_RENDER_PATH
    if revision_mode == "style_only":
        if latest_answer_version is None:
            raise ValidationFailure("Style-only revision requires a previous answer version.")
        retrieval_run_candidate = session.get(RetrievalRun, latest_answer_version.retrieval_run_id)
        if retrieval_run_candidate is None:
            raise ValidationFailure(
                f"Answer version {latest_answer_version.id} is missing its retrieval run."
            )
        retrieval_run = retrieval_run_candidate
        pipeline = _pipeline_selection_for_retrieval_run(
            retrieval_run=retrieval_run,
            settings=effective_settings,
        )
        if existing_execution_run is None:
            row_run = start_repro_run(
                session,
                storage=None,
                settings=effective_settings,
                kind=execution_run_kind or ExecutionRunKind.ROW_REVISION,
                mode=reproducibility_mode,
                tenant_id=case.tenant_id,
                case_id=case.id,
                user_id=user_id,
                parent_run_id=parent_run_id,
                pipeline_profile_name=pipeline.profile_name,
                pipeline_config_hash=pipeline.config_hash,
                index_config_hash=pipeline.index_config_hash,
                runtime_config_hash=pipeline.runtime_config_hash,
                inputs_json={
                    "row_id": str(row.id),
                    "thread_id": str(thread.id),
                    "user_message": user_message,
                    "revision_mode": revision_mode,
                },
            ).execution_run
        else:
            row_run = existing_execution_run
        evidence_items = _load_retrieval_snapshot_items(session, retrieval_run_id=retrieval_run.id)
        normalized_evidence, normalized_evidence_json = normalize_evidence_pack(
            _normalized_evidence_items(evidence_items)
        )
        if not normalized_evidence:
            raise ValidationFailure(
                f"Retrieval run {retrieval_run.id} produced no normalized evidence for answer rendering."
            )
        packed_evidence = pack_normalized_evidence(normalized_evidence, selection=pipeline)
        evidence_pack_hash = _evidence_pack_hash(evidence_items)
        try:
            source_planning_invocation = _source_planning_invocation_for_answer(
                session,
                answer_version=latest_answer_version,
            )
            answer_plan = validate_answer_plan(
                _answer_plan_from_invocation(source_planning_invocation),
                normalized_evidence=packed_evidence.items,
                row_question=row.question_raw,
                row_context=row.context_raw,
                user_request=user_message,
            )
            source_planning_model_invocation_id = str(source_planning_invocation.id)
            reused_answer_version_id = str(latest_answer_version.id)
        except Exception as exc:
            fail_execution_run(
                row_run,
                error_detail=str(exc),
                diagnostics_json={"phase": "answer_plan_reuse"},
            )
            raise
        target_language = cast(Literal["de", "en"], answer_plan.target_language)
        output_mode = cast(Literal["customer_facing", "grounded_review"], answer_plan.output_mode)
        render_thread_history = (
            _style_only_render_thread_history(
                session,
                row=row,
                messages=prior_messages,
            )
            if render_with_thread_history
            else []
        )
        generation_path = RENDER_ONLY_REUSE_PLAN_PATH
    else:
        reuse_from_run: RetrievalRun | None = None
        retrieval_request = build_retrieval_request(
            session,
            case=case,
            row=row,
            user_message=user_message,
            revision_mode=revision_mode,
            retrieval_action=retrieval_action,
            previous_answer_text=latest_answer_version.answer_text if latest_answer_version else None,
            reused_from_retrieval_run_id=None,
            revision_classifier_version=(
                None if revision_mode_override is not None else REVISION_CLASSIFIER_VERSION
            ),
            revision_reason=revision_decision.reason,
            retrieval_action_reason=retrieval_action_reason,
            settings=settings,
            pipeline_selection=pipeline_selection,
            pipeline_profile_name=pipeline_profile_name,
            pipeline_override=pipeline_override,
        )
        if existing_execution_run is None:
            row_run = start_repro_run(
                session,
                storage=None,
                settings=effective_settings,
                kind=(
                    execution_run_kind
                    or (
                        ExecutionRunKind.ROW_DRAFT
                        if revision_mode == "initial_draft"
                        else ExecutionRunKind.ROW_REVISION
                    )
                ),
                mode=reproducibility_mode,
                tenant_id=case.tenant_id,
                case_id=case.id,
                user_id=user_id,
                parent_run_id=parent_run_id,
                pipeline_profile_name=retrieval_request.pipeline.profile_name,
                pipeline_config_hash=retrieval_request.pipeline.config_hash,
                index_config_hash=retrieval_request.pipeline.index_config_hash,
                runtime_config_hash=retrieval_request.pipeline.runtime_config_hash,
                inputs_json={
                    "row_id": str(row.id),
                    "thread_id": str(thread.id),
                    "user_message": user_message,
                    "revision_mode": revision_mode,
                },
            ).execution_run
        else:
            row_run = existing_execution_run
        retrieval_run, _ = build_retrieval_run(
            session,
            ai_service=ai_service,
            tenant_id=case.tenant_id,
            case_id=case.id,
            row=row,
            thread_id=thread.id,
            request=retrieval_request,
            reuse_from_run=reuse_from_run,
            reproducibility_mode=reproducibility_mode,
            parent_run_id=row_run.id,
        )
        pipeline = retrieval_request.pipeline
        evidence_items = _load_retrieval_snapshot_items(session, retrieval_run_id=retrieval_run.id)
        normalized_evidence, normalized_evidence_json = normalize_evidence_pack(
            _normalized_evidence_items(evidence_items)
        )
        if not normalized_evidence:
            raise ValidationFailure(
                f"Retrieval run {retrieval_run.id} produced no normalized evidence for answer planning."
            )
        packed_evidence = pack_normalized_evidence(
            normalized_evidence,
            selection=pipeline,
        )
        target_language = cast(
            Literal["de", "en"],
            normalize_target_language(retrieval_request.row_language),
        )
        output_mode = cast(Literal["customer_facing", "grounded_review"], "customer_facing")
        evidence_pack_hash = _evidence_pack_hash(evidence_items)

        try:
            planning_result = ai_service.plan_answer(
                row_question=row.question_raw,
                row_context=row.context_raw,
                user_request=user_message,
                target_language=target_language,
                output_mode=output_mode,
                normalized_evidence=packed_evidence.items,
                pipeline=pipeline,
            )
            try:
                answer_plan = validate_answer_plan(
                    planning_result.answer_plan,
                    normalized_evidence=packed_evidence.items,
                    row_question=row.question_raw,
                    row_context=row.context_raw,
                    user_request=user_message,
                )
                if answer_plan.target_language != target_language:
                    raise ValidationFailure(
                        f"AnswerPlan target_language {answer_plan.target_language!r} does not match expected {target_language!r}."
                    )
                if answer_plan.output_mode != output_mode:
                    raise ValidationFailure(
                        f"AnswerPlan output_mode {answer_plan.output_mode!r} does not match expected {output_mode!r}."
                    )
                plan_validation_outcome: dict[str, object] = {"valid": True}
                plan_validation_error: str | None = None
            except ValidationFailure as exc:
                answer_plan = planning_result.answer_plan
                plan_validation_outcome = {"valid": False, "error": str(exc)}
                plan_validation_error = str(exc)
            planning_invocation = record_model_invocation(
                session,
                storage=None,
                execution_run=row_run,
                provider_name="openai" if isinstance(ai_service, OpenAIAIService) else "stub",
                endpoint_kind="responses.parse",
                kind=ModelInvocationKind.ANSWER_GENERATION,
                requested_model_id=planning_result.requested_model_id,
                actual_model_id=planning_result.actual_model_id,
                reasoning_effort=pipeline.resolved_pipeline.models.answer_planning.reasoning_effort,
                temperature=None,
                embedding_model_id=None,
                tokenizer_identity=None,
                tokenizer_version=None,
                request_payload=planning_result.request_payload,
                response_payload=planning_result.response_payload,
                provider_response_id=planning_result.provider_response_id,
                sdk_version=openai_sdk_version() if isinstance(ai_service, OpenAIAIService) else None,
                service_tier=planning_result.service_tier,
                usage_json=planning_result.usage_json,
                metadata_json={
                    "prompt_family": "answer_planning",
                    "prompt_version": ANSWER_PLANNING_PROMPT_VERSION,
                    "resolved_prompt_hash": sha256_text(
                        json.dumps(planning_result.request_payload, sort_keys=True, separators=(",", ":"))
                    ),
                    "answer_plan_schema_version": answer_plan.schema_version,
                    "answer_plan_hash": sha256_text(
                        json.dumps(answer_plan.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
                    ),
                    "primary_intent": answer_plan.primary_intent,
                    "secondary_intents": list(answer_plan.secondary_intents),
                    "output_mode": answer_plan.output_mode,
                    "target_language": answer_plan.target_language,
                    "normalized_evidence_hash": packed_evidence.packed_hash,
                    "evidence_pack_hash": evidence_pack_hash,
                    "packing": packed_evidence.metadata,
                    "pre_packing_normalized_evidence_hash": sha256_text(normalized_evidence_json),
                    "validation_outcome": plan_validation_outcome,
                },
            )
            source_planning_model_invocation_id = str(planning_invocation.id)
            if plan_validation_error is not None:
                raise ValidationFailure(plan_validation_error)
        except Exception as exc:
            fail_execution_run(
                row_run,
                error_detail=str(exc),
                diagnostics_json={"phase": "answer_planning"},
            )
            raise
        render_thread_history = []

    rendering_invocation = None
    try:
        render_result = ai_service.render_answer(
            row_question=row.question_raw,
            row_context=row.context_raw,
            user_request=user_message,
            thread_history=render_thread_history,
            answer_plan=answer_plan,
            output_mode=output_mode,
            target_language=target_language,
            pipeline=pipeline,
        )
        try:
            validation_outcome = validate_rendered_answer(
                answer_text=render_result.response_text,
                plan=answer_plan,
            )
            validation_error: str | None = None
        except ValidationFailure as exc:
            validation_outcome = cast(dict[str, object], {"valid": False, "error": str(exc)})
            validation_error = str(exc)
        rendering_invocation = record_model_invocation(
            session,
            storage=None,
            execution_run=row_run,
            provider_name="openai" if isinstance(ai_service, OpenAIAIService) else "stub",
            endpoint_kind="responses.create",
            kind=ModelInvocationKind.ANSWER_GENERATION,
            requested_model_id=render_result.requested_model_id,
            actual_model_id=render_result.actual_model_id,
            reasoning_effort=pipeline.resolved_pipeline.models.answer_rendering.reasoning_effort,
            temperature=pipeline.resolved_pipeline.generation.temperature,
            embedding_model_id=None,
            tokenizer_identity=None,
            tokenizer_version=None,
            request_payload=render_result.request_payload,
            response_payload=render_result.response_text,
            provider_response_id=render_result.provider_response_id,
            sdk_version=openai_sdk_version() if isinstance(ai_service, OpenAIAIService) else None,
            service_tier=render_result.service_tier,
            usage_json=render_result.usage_json,
            metadata_json={
                "prompt_family": "answer_rendering",
                "prompt_version": ANSWER_RENDERING_PROMPT_VERSION,
                "resolved_prompt_hash": sha256_text(render_result.request_text),
                "answer_plan_hash": sha256_text(
                    json.dumps(answer_plan.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
                ),
                "primary_intent": answer_plan.primary_intent,
                "secondary_intents": list(answer_plan.secondary_intents),
                "output_mode": answer_plan.output_mode,
                "target_language": answer_plan.target_language,
                "validation_outcome": validation_outcome,
                "evidence_pack_hash": evidence_pack_hash,
                "thread_history_enabled": render_with_thread_history,
                "thread_history_message_count": len(render_thread_history),
            },
        )
        if validation_error is not None:
            raise ValidationFailure(validation_error)
    except Exception as exc:
        if rendering_invocation is not None:
            rendering_invocation.metadata_json = {
                **rendering_invocation.metadata_json,
                "validation_outcome": {"valid": False, "error": str(exc)},
            }
        fail_execution_run(
            row_run,
            error_detail=str(exc),
            diagnostics_json={"phase": "answer_rendering"},
        )
        raise

    answer_text = render_result.response_text.strip()
    version_number = (
        session.scalar(
            select(func.coalesce(func.max(AnswerVersion.version_number), 0) + 1).where(
                AnswerVersion.questionnaire_row_id == row.id
            )
        )
        or 1
    )
    model_name = (
        render_result.actual_model_id
        or render_result.requested_model_id
        or (
            ai_service._settings.openai_response_model
            if isinstance(ai_service, OpenAIAIService)
            else "stub-ai-service"
        )
    )
    if rendering_invocation is None:
        raise ValidationFailure("Answer generation did not persist the required rendering invocation.")
    answer_version = AnswerVersion(
        tenant_id=case.tenant_id,
        case_id=case.id,
        questionnaire_row_id=row.id,
        chat_thread_id=thread.id,
        retrieval_run_id=retrieval_run.id,
        execution_run_id=row_run.id,
        model_invocation_id=rendering_invocation.id,
        version_number=version_number,
        answer_text=answer_text,
        status=AnswerStatus.DRAFT,
        model=model_name,
        prompt_version=ANSWER_RENDERING_PROMPT_VERSION,
        llm_capture_status=LLMCaptureStatus.CAPTURED,
        llm_request_text=render_result.request_text,
        llm_response_text=render_result.response_text,
    )
    session.add(answer_version)
    row.review_status = QuestionnaireRowStatus.NEEDS_REVIEW
    row.last_error_detail = None
    session.flush()
    for item in evidence_items:
        session.add(
            EvidenceLink(
                answer_version_id=answer_version.id,
                retrieval_run_id=retrieval_run.id,
                snapshot_item_id=item.id,
            )
        )
    session.add(
        ChatMessage(
            tenant_id=case.tenant_id,
            case_id=case.id,
            questionnaire_row_id=row.id,
            thread_id=thread.id,
            role=MessageRole.ASSISTANT,
            content=answer_text,
            answer_version_id=answer_version.id,
            retrieval_run_id=retrieval_run.id,
        )
    )
    thread.updated_at = datetime.now(UTC)
    session.flush()
    execution_outputs: dict[str, object] = {
        "answer_version_id": str(answer_version.id),
        "retrieval_run_id": str(retrieval_run.id),
        "model_invocation_id": str(rendering_invocation.id),
        "rendering_model_invocation_id": str(rendering_invocation.id),
        "generation_path": generation_path,
    }
    if planning_invocation is not None:
        execution_outputs["planning_model_invocation_id"] = str(planning_invocation.id)
    if source_planning_model_invocation_id is not None:
        execution_outputs["source_planning_model_invocation_id"] = source_planning_model_invocation_id
    if reused_answer_version_id is not None:
        execution_outputs["reused_answer_version_id"] = reused_answer_version_id
    finish_execution_run(
        row_run,
        outputs_json=execution_outputs,
    )
    if reproducibility_mode == ReproducibilityMode.STRICT_EVAL:
        assert_execution_run_consistency(session, run=row_run)
    messages = list_thread_messages(session, thread_id=thread.id)
    return DraftResult(
        thread=thread,
        answer_version=answer_version,
        messages=list(messages),
        evidence_items=list(evidence_items),
    )
