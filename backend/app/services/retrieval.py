from __future__ import annotations

import json
import math
import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Any
from uuid import UUID

from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from app.config import Settings, get_settings
from app.exceptions import ScopeViolation, ValidationFailure
from app.models.entities import (
    CaseProfile,
    CaseProfileItem,
    ExecutionRun,
    HistoricalCaseProfile,
    HistoricalClientPackage,
    HistoricalDataset,
    HistoricalQARow,
    HistoricalWorkbook,
    PdfChunk,
    ProductTruthChunk,
    ProductTruthRecord,
    Questionnaire,
    QuestionnaireRow,
    RetrievalRun,
    RetrievalSnapshotItem,
    RfxCase,
)
from app.models.enums import (
    ApprovalStatus,
    EvidenceSourceKind,
    ExecutionRunKind,
    ReproducibilityMode,
)
from app.pipeline.config import (
    PipelineSelection,
    RetrievalConfig,
    assert_pipeline_runtime_compatibility,
    resolve_pipeline_selection,
)
from app.prompts.case_profile import CASE_PROFILE_PROMPT_SET_VERSION, CASE_PROFILE_SCHEMA_VERSION
from app.services.ai import AIService
from app.services.case_profiles import CASE_SIGNATURE_VERSION, build_case_profile_signature_text
from app.services.hashing import sha256_text
from app.services.pdf_chunks import current_pdf_chunking_version
from app.services.reproducibility import (
    assert_execution_run_consistency,
    embed_text_with_invocation_recorded,
    fail_execution_run,
    finish_execution_run,
    start_repro_run,
)
from app.services.workbooks import infer_language

AUTHORITY_ORDER = ["current_case_facts", "product_truth", "historical_exemplars"]
MINIMUM_SOURCE_SCORES = {
    "current_case_facts": 0.0,
    "raw_current_pdf": 0.05,
    "product_truth": 0.05,
    "historical_exemplar": 0.05,
}
RETRIEVAL_STRATEGY_VERSION = "retrieval.v2.hardened.v1"
_CONFIG_KEY_BY_SOURCE_LABEL = {
    "current_case_facts": "current_case_facts",
    "raw_current_pdf": "current_pdf_evidence",
    "product_truth": "product_truth",
    "historical_exemplar": "historical_exemplars",
}
_TOKEN_PATTERN = re.compile(r"[A-Za-z0-9_]+")
_STOPWORDS = {
    "the",
    "and",
    "for",
    "with",
    "that",
    "this",
    "from",
    "into",
    "eine",
    "einer",
    "eines",
    "und",
    "der",
    "die",
    "das",
    "mit",
    "für",
    "von",
    "auf",
}


@dataclass(frozen=True)
class RetrievalRequest:
    pipeline: PipelineSelection
    tenant_id: UUID
    case_id: UUID
    questionnaire_id: UUID
    questionnaire_source_row_id: str
    questionnaire_file_name: str
    questionnaire_file_hash: str
    case_profile_id: UUID
    current_case_pdf_upload_id: UUID
    current_case_pdf_file_name: str
    current_case_pdf_file_hash: str
    current_case_signature_version: str
    row_language: str
    row_question: str
    row_context: str
    row_question_context: str
    user_message: str
    revision_mode: str
    revision_classifier_version: str | None
    revision_reason: str | None
    retrieval_action: str
    retrieval_action_reason: str | None
    current_case_signature: str
    previous_answer_text: str | None
    revision_intent: str
    reused_from_retrieval_run_id: UUID | None = None

    @property
    def query_text(self) -> str:
        parts = [self.row_question_context]
        if self.revision_intent.strip():
            parts.append(self.revision_intent)
        return "\n\n".join(part for part in parts if part.strip())

    def to_request_context(self) -> dict[str, object]:
        return {
            "strategy_version": RETRIEVAL_STRATEGY_VERSION,
            "pipeline": {
                "profile_name": self.pipeline.profile_name,
                "used_default_profile": self.pipeline.used_default_profile,
                "override_supplied": self.pipeline.override_supplied,
                "config_hash": self.pipeline.config_hash,
                "index_config_hash": self.pipeline.index_config_hash,
                "runtime_config_hash": self.pipeline.runtime_config_hash,
                "resolved_config": self.pipeline.resolved_config,
            },
            "retrieval_action": self.retrieval_action,
            "retrieval_action_reason": self.retrieval_action_reason,
            "revision_mode": self.revision_mode,
            "reused_from_retrieval_run_id": (
                str(self.reused_from_retrieval_run_id)
                if self.reused_from_retrieval_run_id is not None
                else None
            ),
            "revision_classifier": {
                "version": self.revision_classifier_version,
                "reason": self.revision_reason,
            },
            "scope": {
                "tenant_id": str(self.tenant_id),
                "case_id": str(self.case_id),
                "questionnaire_id": str(self.questionnaire_id),
                "questionnaire_source_row_id": self.questionnaire_source_row_id,
                "questionnaire_file_name": self.questionnaire_file_name,
                "questionnaire_file_hash": self.questionnaire_file_hash,
                "case_profile_id": str(self.case_profile_id),
                "current_case_pdf_upload_id": str(self.current_case_pdf_upload_id),
                "current_case_pdf_file_name": self.current_case_pdf_file_name,
                "current_case_pdf_file_hash": self.current_case_pdf_file_hash,
                "current_case_signature_version": self.current_case_signature_version,
            },
            "language": {
                "row_language": self.row_language,
            },
            "feature_texts": {
                "row_question": self.row_question,
                "row_context": self.row_context,
                "row_question_context": self.row_question_context,
                "revision_intent": self.revision_intent,
                "current_case_signature": self.current_case_signature,
                "user_message": self.user_message,
                "previous_answer_text": self.previous_answer_text,
            },
        }


@dataclass(frozen=True)
class RetrievedEvidence:
    source_kind: EvidenceSourceKind
    source_id: UUID
    source_label: str
    source_title: str
    excerpt: str
    metadata_json: dict[str, object]
    score: float


@dataclass(frozen=True)
class CandidateStageTrace:
    corpus: str
    stage: str
    mode: str
    candidate_count: int
    broadened: bool = False
    skipped: bool = False
    reason: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "corpus": self.corpus,
            "stage": self.stage,
            "mode": self.mode,
            "candidate_count": self.candidate_count,
            "broadened": self.broadened,
            "skipped": self.skipped,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class CandidatePool:
    candidates: list[Any]
    stages: list[CandidateStageTrace]


@dataclass(frozen=True)
class RetrievalAssessment:
    status: str
    degraded: bool
    notes: list[str]

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "degraded": self.degraded,
            "notes": self.notes,
        }


def _to_float_list(values: Iterable[float] | None) -> list[float]:
    if values is None:
        return []
    return [float(value) for value in values]


def _cosine_similarity(
    left: Iterable[float] | None,
    right: Iterable[float] | None,
) -> float:
    left_values = _to_float_list(left)
    right_values = _to_float_list(right)
    if len(left_values) == 0 or len(right_values) == 0:
        return 0.0
    numerator = sum(a * b for a, b in zip(left_values, right_values, strict=False))
    left_norm = math.sqrt(sum(a * a for a in left_values))
    right_norm = math.sqrt(sum(b * b for b in right_values))
    if left_norm == 0 or right_norm == 0:
        return 0.0
    return numerator / (left_norm * right_norm)


def _tokenize(text: str) -> set[str]:
    return {
        token
        for token in _TOKEN_PATTERN.findall(text.lower())
        if len(token) > 2 and token not in _STOPWORDS
    }


def _keyword_overlap(query: str, candidate: str) -> float:
    query_tokens = _tokenize(query)
    candidate_tokens = _tokenize(candidate)
    if not query_tokens:
        return 0.0
    return len(query_tokens & candidate_tokens) / len(query_tokens)


def _keyword_match_clause(columns: Sequence[Any], query_text: str) -> Any | None:
    tokens = list(_tokenize(query_text))[:10]
    if not tokens:
        return None
    return or_(
        *[
            func.lower(column).contains(token)
            for column in columns
            for token in tokens
        ]
    )


def _matched_features(component_scores: dict[str, float]) -> list[str]:
    return sorted(
        key
        for key, value in component_scores.items()
        if key != "language_adjustment" and value >= 0.05
    )


def _language_adjustment(active_language: str, candidate_language: str) -> tuple[float, dict[str, object]]:
    if active_language not in {"de", "en"} or candidate_language not in {"de", "en"}:
        return 0.0, {
            "language_match": False,
            "cross_lingual_fallback": False,
            "active_language": active_language,
            "candidate_language": candidate_language,
        }
    same_language = active_language == candidate_language
    return (
        0.12 if same_language else -0.03,
        {
            "language_match": same_language,
            "cross_lingual_fallback": not same_language,
            "active_language": active_language,
            "candidate_language": candidate_language,
        },
    )


def _retrieval_config(request: RetrievalRequest) -> RetrievalConfig:
    return request.pipeline.resolved_pipeline.retrieval


def _query_weight(request: RetrievalRequest, key: str) -> float:
    return float(getattr(_retrieval_config(request).query_weights, key))


def _scoring_multiplier(request: RetrievalRequest, key: str) -> float:
    return float(getattr(_retrieval_config(request).scoring, key))


def _candidate_pool_field_name(key: str) -> str:
    return _CONFIG_KEY_BY_SOURCE_LABEL.get(key, key)


def _optional_candidate_pool_limit(request: RetrievalRequest, key: str) -> int | None:
    value = getattr(_retrieval_config(request).candidate_pool, _candidate_pool_field_name(key))
    return int(value) if isinstance(value, int) else None


def _required_candidate_pool_limit(request: RetrievalRequest, key: str) -> int:
    field_name = _candidate_pool_field_name(key)
    # Non-optional candidate-pool defaults are single-sourced from resolved pipeline config.
    value = getattr(_retrieval_config(request).candidate_pool, field_name)
    if not isinstance(value, int):
        raise ValidationFailure(
            f"Resolved pipeline config is missing required retrieval.candidate_pool.{field_name}."
        )
    return int(value)


def _final_quota(request: RetrievalRequest, key: str) -> int:
    return int(
        getattr(
            _retrieval_config(request).final_quota,
            _candidate_pool_field_name(key),
        )
    )


def _broadening_enabled(request: RetrievalRequest) -> bool:
    return bool(_retrieval_config(request).broadening.enabled)


def _broadening_max_stages(request: RetrievalRequest) -> int:
    return int(_retrieval_config(request).broadening.max_stages)


def _dedup_mode(request: RetrievalRequest) -> str:
    return str(_retrieval_config(request).dedup.mode)


def _language_bonus(request: RetrievalRequest) -> float:
    return _scoring_multiplier(request, "same_language_bonus")


def _build_query_embeddings(
    session: Session,
    ai_service: AIService,
    request: RetrievalRequest,
    *,
    execution_run,
) -> tuple[dict[str, list[float]], dict[str, dict[str, object]]]:
    texts = {
        "row_question": request.row_question,
        "row_context": request.row_context,
        "row_question_context": request.row_question_context,
        "current_case_signature": request.current_case_signature,
        "current_pdf_query": f"{request.row_question_context}\n{request.revision_intent}".strip(),
        "product_truth_query": (
            f"{request.row_question_context}\n{request.current_case_signature}\n{request.revision_intent}".strip()
        ),
        "historical_row_query": f"{request.row_question_context}\n{request.revision_intent}".strip(),
    }
    if request.revision_intent.strip():
        texts["revision_intent"] = request.revision_intent
    embeddings: dict[str, list[float]] = {}
    lineage: dict[str, dict[str, object]] = {}
    for key, text in texts.items():
        if not text.strip():
            continue
        vector, invocation = embed_text_with_invocation_recorded(
            session,
            storage=None,
            execution_run=execution_run,
            ai_service=ai_service,
            text=text,
            model_id=request.pipeline.resolved_pipeline.indexing.embedding_model,
            dimensions=request.pipeline.resolved_pipeline.indexing.embedding_dimensions,
            metadata_json={"retrieval_query_feature": key},
        )
        embeddings[key] = vector
        lineage[key] = {
            "model_invocation_id": str(invocation.id),
            "request_payload_hash": invocation.request_payload_hash,
            "response_payload_hash": invocation.response_payload_hash,
            "embedding_model_id": invocation.embedding_model_id,
        }
    return embeddings, lineage


def _require_embedding(name: str, value: Iterable[float] | None) -> list[float]:
    vector = _to_float_list(value)
    if not vector:
        raise ValidationFailure(f"Retrieval candidate is missing required embedding for {name}.")
    return vector


def _dialect_name(session: Session) -> str:
    bind = session.get_bind()
    return bind.dialect.name if bind is not None else "unknown"


def _pgvector_candidates(session: Session, statement, order_expression, limit: int):  # type: ignore[no-untyped-def]
    return session.execute(statement.order_by(order_expression).limit(limit)).all()


def _merge_unique(items: Sequence[Any], *, key_fn) -> list[Any]:  # type: ignore[no-untyped-def]
    merged: list[Any] = []
    seen: set[object] = set()
    for item in items:
        key = key_fn(item)
        if key in seen:
            continue
        seen.add(key)
        merged.append(item)
    return merged


def _build_execution_context(
    *,
    session: Session,
    request: RetrievalRequest,
    stages: Sequence[CandidateStageTrace],
    sufficiency: RetrievalAssessment,
    selected_counts: dict[str, int],
    dedup: list[dict[str, object]],
) -> dict[str, object]:
    broadened = any(stage.broadened for stage in stages)
    candidate_generation_mode = (
        "postgres_keyword_pgvector_python_rerank"
        if _dialect_name(session) == "postgresql"
        else "sql_keyword_scope_python_rerank"
    )
    return {
        "candidate_generation_mode": candidate_generation_mode,
        "broadened": broadened,
        "stages": [stage.to_dict() for stage in stages],
        "selected_counts": selected_counts,
        "dedup": dedup,
        "sufficiency": sufficiency.to_dict(),
        "reused_snapshot": request.retrieval_action == "reuse_previous_snapshot",
    }


def _serialize_candidate_pool(corpus: str, candidates: Sequence[Any]) -> list[dict[str, object]]:
    serialized: list[dict[str, object]] = []
    for candidate in candidates:
        if corpus == "current_case_facts":
            serialized.append(
                {
                    "id": str(candidate.id),
                    "analysis_item_id": candidate.analysis_item_id,
                    "case_id": str(candidate.case_id),
                }
            )
            continue
        if corpus == "raw_current_pdf":
            serialized.append(
                {
                    "id": str(candidate.id),
                    "page_number": candidate.page_number,
                    "chunk_index": candidate.chunk_index,
                    "chunk_hash": candidate.chunk_hash,
                }
            )
            continue
        if corpus == "product_truth":
            chunk, record = candidate
            serialized.append(
                {
                    "chunk_id": str(chunk.id),
                    "truth_record_id": str(record.id),
                    "version": record.version,
                    "artifact_build_id": str(record.artifact_build_id) if record.artifact_build_id else None,
                }
            )
            continue
        if corpus == "historical_exemplar":
            row, package, _profile = candidate
            serialized.append(
                {
                    "row_id": str(row.id),
                    "dataset_id": str(row.dataset_id),
                    "source_row_id": row.source_row_id,
                    "client_package_id": str(package.id),
                }
            )
    return serialized


def _fetch_case_profile_signature(
    session: Session,
    *,
    case_id: UUID,
) -> tuple[CaseProfile, list[CaseProfileItem], str]:
    profile = session.scalar(
        select(CaseProfile)
        .where(CaseProfile.case_id == case_id)
        .order_by(CaseProfile.created_at.desc())
    )
    if profile is None:
        raise ValidationFailure(f"Case {case_id} has no case profile for retrieval.")
    if profile.prompt_set_version != CASE_PROFILE_PROMPT_SET_VERSION:
        raise ValidationFailure(
            f"Case profile {profile.id} has unexpected prompt set version {profile.prompt_set_version!r}. Rebuild case index artifacts."
        )
    if profile.schema_version != CASE_PROFILE_SCHEMA_VERSION:
        raise ValidationFailure(
            f"Case profile {profile.id} has unexpected schema version {profile.schema_version!r}. Rebuild case index artifacts."
        )
    items = session.scalars(
        select(CaseProfileItem)
        .where(CaseProfileItem.case_profile_id == profile.id)
        .order_by(CaseProfileItem.position.asc())
    ).all()
    if not items:
        raise ValidationFailure(f"Case profile {profile.id} has no persisted analysis items.")
    signature_text = build_case_profile_signature_text(summary=profile.summary, analysis_items=items)
    if not signature_text.strip():
        raise ValidationFailure(f"Case profile {profile.id} produced an empty retrieval signature.")
    return profile, list(items), signature_text


def build_retrieval_request(
    session: Session,
    *,
    case: RfxCase,
    row: QuestionnaireRow,
    user_message: str,
    revision_mode: str,
    retrieval_action: str,
    previous_answer_text: str | None,
    reused_from_retrieval_run_id: UUID | None = None,
    revision_classifier_version: str | None = None,
    revision_reason: str | None = None,
    retrieval_action_reason: str | None = None,
    settings: Settings | None = None,
    pipeline_selection: PipelineSelection | None = None,
    pipeline_profile_name: str | None = None,
    pipeline_override: dict[str, object] | None = None,
) -> RetrievalRequest:
    questionnaire = session.get(Questionnaire, row.questionnaire_id)
    if questionnaire is None:
        raise ValidationFailure(
            f"Questionnaire row {row.id} is missing questionnaire provenance for retrieval."
        )
    selection = pipeline_selection or resolve_pipeline_selection(
        settings or get_settings(),
        profile_name=pipeline_profile_name,
        override=pipeline_override,
        pinned_config=case.pipeline_config_json,
        pinned_profile_name=case.pipeline_profile_name,
    )
    profile, items, _ = _fetch_case_profile_signature(session, case_id=case.id)
    signature_mode = selection.resolved_pipeline.indexing.historical.signature_mode
    signature_text = build_case_profile_signature_text(
        summary=profile.summary,
        signature_mode=signature_mode,
        analysis_items=items,
    )
    row_language, _ = infer_language(f"{row.context_raw} {row.question_raw}")
    revision_intent = user_message if revision_mode == "content_change" else ""
    return RetrievalRequest(
        pipeline=selection,
        tenant_id=case.tenant_id,
        case_id=case.id,
        questionnaire_id=questionnaire.id,
        questionnaire_source_row_id=row.source_row_id,
        questionnaire_file_name=questionnaire.source_file_name,
        questionnaire_file_hash=questionnaire.file_hash,
        case_profile_id=profile.id,
        current_case_pdf_upload_id=profile.source_pdf_upload_id,
        current_case_pdf_file_name=profile.source_file_name,
        current_case_pdf_file_hash=profile.source_file_hash,
        current_case_signature_version=CASE_SIGNATURE_VERSION,
        row_language=row_language,
        row_question=row.question_raw,
        row_context=row.context_raw,
        row_question_context=f"{row.context_raw}\n{row.question_raw}",
        user_message=user_message,
        revision_mode=revision_mode,
        revision_classifier_version=revision_classifier_version,
        revision_reason=revision_reason,
        retrieval_action=retrieval_action,
        retrieval_action_reason=retrieval_action_reason,
        current_case_signature=signature_text,
        previous_answer_text=previous_answer_text,
        revision_intent=revision_intent,
        reused_from_retrieval_run_id=reused_from_retrieval_run_id,
    )


def _validate_historical_corpus_integrity(session: Session, *, tenant_id: UUID) -> None:
    invalid = session.execute(
        select(HistoricalQARow, HistoricalClientPackage, HistoricalCaseProfile)
        .outerjoin(HistoricalClientPackage, HistoricalClientPackage.id == HistoricalQARow.client_package_id)
        .outerjoin(HistoricalCaseProfile, HistoricalCaseProfile.id == HistoricalQARow.historical_case_profile_id)
        .where(
            HistoricalQARow.tenant_id == tenant_id,
            HistoricalQARow.approval_status == ApprovalStatus.APPROVED,
            or_(
                HistoricalQARow.client_package_id.is_(None),
                HistoricalQARow.historical_case_profile_id.is_(None),
                HistoricalClientPackage.id.is_(None),
                HistoricalCaseProfile.id.is_(None),
                HistoricalCaseProfile.client_package_id != HistoricalQARow.client_package_id,
                HistoricalCaseProfile.prompt_set_version != CASE_PROFILE_PROMPT_SET_VERSION,
                HistoricalCaseProfile.schema_version != CASE_PROFILE_SCHEMA_VERSION,
                HistoricalCaseProfile.source_file_hash != HistoricalClientPackage.source_pdf_file_hash,
                HistoricalCaseProfile.source_file_name != HistoricalClientPackage.source_pdf_file_name,
                HistoricalCaseProfile.signature_version != CASE_SIGNATURE_VERSION,
                HistoricalCaseProfile.signature_embedding_model == "",
                HistoricalCaseProfile.signature_fields_json.is_(None),
                HistoricalCaseProfile.signature_text == "",
                HistoricalCaseProfile.signature_embedding.is_(None),
            ),
        )
        .limit(1)
    ).first()
    if invalid is None:
        return
    row, package, profile = invalid
    if row is not None and row.client_package_id is None:
        raise ValidationFailure(
            f"Historical row {row.source_row_id} is missing historical client-package provenance."
        )
    if row is not None and row.historical_case_profile_id is None:
        raise ValidationFailure(
            f"Historical row {row.source_row_id} is missing historical case-signature provenance."
        )
    if row is not None and package is None:
        raise ValidationFailure(
            f"Historical row {row.source_row_id} links to a missing historical client package."
        )
    if row is not None and profile is None:
        raise ValidationFailure(
            f"Historical row {row.source_row_id} is missing its historical case profile."
        )
    if (
        row is not None
        and profile is not None
        and row.client_package_id is not None
        and profile.client_package_id != row.client_package_id
    ):
        raise ValidationFailure(
            f"Historical row {row.source_row_id} points to a historical case profile from another package."
        )
    if profile is not None and profile.prompt_set_version != CASE_PROFILE_PROMPT_SET_VERSION:
        raise ValidationFailure(
            f"Historical case profile {profile.id} has unexpected prompt set version {profile.prompt_set_version!r}."
        )
    if profile is not None and profile.schema_version != CASE_PROFILE_SCHEMA_VERSION:
        raise ValidationFailure(
            f"Historical case profile {profile.id} has unexpected schema version {profile.schema_version!r}."
        )
    if profile is not None and profile.signature_version != CASE_SIGNATURE_VERSION:
        raise ValidationFailure(
            f"Historical case profile {profile.id} has unexpected signature version {profile.signature_version!r}."
        )
    raise ValidationFailure("Historical case-signature provenance is inconsistent.")


def _validate_pdf_chunks(
    chunks: Sequence[PdfChunk],
    *,
    case_id: UUID,
    request: RetrievalRequest,
) -> None:
    if not chunks:
        raise ValidationFailure(f"Case {case_id} has no persisted PDF chunks for retrieval.")
    expected_chunking_version = current_pdf_chunking_version(request.pipeline)
    expected_embedding_model = request.pipeline.resolved_pipeline.indexing.embedding_model
    if expected_embedding_model is None:
        raise ValidationFailure("Resolved pipeline is missing expected embedding model for PDF chunks.")
    for chunk in chunks:
        if chunk.chunking_version != expected_chunking_version:
            raise ValidationFailure(
                f"PDF chunk {chunk.id} has unexpected chunking version {chunk.chunking_version!r}; expected {expected_chunking_version!r}."
            )
        if chunk.embedding_model != expected_embedding_model:
            raise ValidationFailure(
                f"PDF chunk {chunk.id} has embedding model {chunk.embedding_model!r}; expected {expected_embedding_model!r}."
            )
        if chunk.start_offset < 0 or chunk.end_offset <= chunk.start_offset:
            raise ValidationFailure(f"PDF chunk {chunk.id} has inconsistent offsets.")
        if chunk.chunk_hash != sha256_text(chunk.content):
            raise ValidationFailure(f"PDF chunk {chunk.id} has mismatched chunk hash provenance.")
        _require_embedding(f"pdf_chunk {chunk.id}", chunk.embedding)


def _load_current_case_fact_candidates(
    session: Session,
    *,
    request: RetrievalRequest,
    tenant_id: UUID,
    case_id: UUID,
) -> CandidatePool:
    statement = select(CaseProfileItem).where(
            CaseProfileItem.tenant_id == tenant_id,
            CaseProfileItem.case_id == case_id,
        )
    limit = _optional_candidate_pool_limit(request, "current_case_facts")
    if limit is not None:
        statement = statement.order_by(CaseProfileItem.position.asc()).limit(limit)
    case_profile_items = session.scalars(statement).all()
    if not case_profile_items:
        raise ValidationFailure(f"Case {case_id} has no case-profile facts for retrieval.")
    return CandidatePool(
        candidates=list(case_profile_items),
        stages=[
            CandidateStageTrace(
                corpus="current_case_facts",
                stage="case_scoped_load",
                mode="case_scoped_small_corpus",
                candidate_count=len(case_profile_items),
            )
        ],
    )


def _load_raw_pdf_candidates(
    session: Session,
    *,
    request: RetrievalRequest,
    embeddings: dict[str, list[float]],
) -> CandidatePool:
    pool_limit = _required_candidate_pool_limit(request, "current_pdf_evidence")
    base_statement = (
        select(PdfChunk)
        .where(
            PdfChunk.tenant_id == request.tenant_id,
            PdfChunk.case_id == request.case_id,
            PdfChunk.upload_id == request.current_case_pdf_upload_id,
        )
    )
    chunks = session.scalars(
        base_statement.order_by(PdfChunk.page_number.asc(), PdfChunk.chunk_index.asc())
    ).all()
    _validate_pdf_chunks(chunks, case_id=request.case_id, request=request)
    stages: list[CandidateStageTrace] = []
    # reuse the original query text for keyword matching
    keyword_query_text = f"{request.row_question_context}\n{request.revision_intent}".strip()
    keyword_rows: list[PdfChunk] = []
    keyword_clause = _keyword_match_clause((PdfChunk.content,), keyword_query_text)
    if keyword_clause is None:
        stages.append(
            CandidateStageTrace(
                corpus="raw_current_pdf",
                stage="keyword_prefilter",
                mode="sql_contains",
                candidate_count=0,
                skipped=True,
                reason="no_keyword_tokens",
            )
        )
    else:
        keyword_rows = list(session.scalars(base_statement.where(keyword_clause).limit(pool_limit)).all())
        stages.append(
            CandidateStageTrace(
                corpus="raw_current_pdf",
                stage="keyword_prefilter",
                mode="sql_contains",
                candidate_count=len(keyword_rows),
            )
        )
    vector_rows: list[PdfChunk] = []
    if _dialect_name(session) == "postgresql":
        vector_rows = list(
            session.scalars(
            select(PdfChunk)
            .where(
                PdfChunk.tenant_id == request.tenant_id,
                PdfChunk.case_id == request.case_id,
                PdfChunk.upload_id == request.current_case_pdf_upload_id,
            )
            .order_by(PdfChunk.embedding.cosine_distance(embeddings["current_pdf_query"]))  # type: ignore[attr-defined]
            .limit(pool_limit)
            ).all()
        )
        stages.append(
            CandidateStageTrace(
                corpus="raw_current_pdf",
                stage="pgvector_prefilter",
                mode="pgvector_cosine_distance",
                candidate_count=len(vector_rows),
            )
        )
    else:
        stages.append(
            CandidateStageTrace(
                corpus="raw_current_pdf",
                stage="pgvector_prefilter",
                mode="pgvector_cosine_distance",
                candidate_count=0,
                skipped=True,
                reason="non_postgresql_backend",
            )
        )
    merged = _merge_unique([*keyword_rows, *vector_rows], key_fn=lambda item: item.id)
    if merged:
        return CandidatePool(candidates=merged, stages=stages)
    if not _broadening_enabled(request) or _broadening_max_stages(request) == 0:
        stages.append(
            CandidateStageTrace(
                corpus="raw_current_pdf",
                stage="broadening_disabled",
                mode="case_scoped_broadening",
                candidate_count=0,
                skipped=True,
                reason="broadening_disabled_by_pipeline_config",
            )
        )
        return CandidatePool(candidates=[], stages=stages)
    broadened = chunks[:pool_limit]
    stages.append(
        CandidateStageTrace(
            corpus="raw_current_pdf",
            stage="broadened_scope",
            mode="case_scoped_broadening",
            candidate_count=len(broadened),
            broadened=True,
            reason="keyword/vector stages produced zero candidates",
        )
    )
    return CandidatePool(candidates=list(broadened), stages=stages)


def _load_product_truth_candidates(
    session: Session,
    *,
    request: RetrievalRequest,
    embeddings: dict[str, list[float]],
    effective_date: date,
) -> CandidatePool:
    pool_limit = _required_candidate_pool_limit(request, "product_truth")
    base_statement = (
        select(ProductTruthChunk, ProductTruthRecord)
        .join(ProductTruthRecord, ProductTruthRecord.id == ProductTruthChunk.truth_record_id)
        .where(
            ProductTruthChunk.tenant_id == request.tenant_id,
            ProductTruthChunk.approval_status == ApprovalStatus.APPROVED,
            ProductTruthRecord.tenant_id == request.tenant_id,
            ProductTruthRecord.approval_status == ApprovalStatus.APPROVED,
            or_(
                ProductTruthRecord.effective_to.is_(None),
                ProductTruthRecord.effective_to >= effective_date,
            ),
        )
    )
    stages: list[CandidateStageTrace] = []
    keyword_query_text = (
        f"{request.row_question_context}\n{request.current_case_signature}\n{request.revision_intent}".strip()
    )
    keyword_clause = _keyword_match_clause(
        (
            ProductTruthChunk.content,
            ProductTruthRecord.product_area,
            ProductTruthRecord.title,
            ProductTruthRecord.source_section,
        ),
        keyword_query_text,
    )
    keyword_rows: list[tuple[ProductTruthChunk, ProductTruthRecord]] = []
    if keyword_clause is None:
        stages.append(
            CandidateStageTrace(
                corpus="product_truth",
                stage="keyword_prefilter",
                mode="sql_contains",
                candidate_count=0,
                skipped=True,
                reason="no_keyword_tokens",
            )
        )
    else:
        keyword_rows = [
            (chunk, record)
            for chunk, record in session.execute(base_statement.where(keyword_clause).limit(pool_limit)).all()
        ]
        stages.append(
            CandidateStageTrace(
                corpus="product_truth",
                stage="keyword_prefilter",
                mode="sql_contains",
                candidate_count=len(keyword_rows),
            )
        )
    vector_rows: list[tuple[ProductTruthChunk, ProductTruthRecord]] = []
    if _dialect_name(session) == "postgresql":
        vector_rows = [
            (chunk, record)
            for chunk, record in session.execute(
                base_statement.order_by(
                    ProductTruthChunk.embedding.cosine_distance(embeddings["product_truth_query"])  # type: ignore[attr-defined]
                ).limit(pool_limit)
            ).all()
        ]
        stages.append(
            CandidateStageTrace(
                corpus="product_truth",
                stage="pgvector_prefilter",
                mode="pgvector_cosine_distance",
                candidate_count=len(vector_rows),
            )
        )
    else:
        stages.append(
            CandidateStageTrace(
                corpus="product_truth",
                stage="pgvector_prefilter",
                mode="pgvector_cosine_distance",
                candidate_count=0,
                skipped=True,
                reason="non_postgresql_backend",
            )
        )
    merged = _merge_unique([*keyword_rows, *vector_rows], key_fn=lambda item: item[0].id)
    if merged:
        return CandidatePool(candidates=merged, stages=stages)
    if not _broadening_enabled(request) or _broadening_max_stages(request) == 0:
        stages.append(
            CandidateStageTrace(
                corpus="product_truth",
                stage="broadening_disabled",
                mode="tenant_scoped_broadening",
                candidate_count=0,
                skipped=True,
                reason="broadening_disabled_by_pipeline_config",
            )
        )
        return CandidatePool(candidates=[], stages=stages)
    broadened = [
        (chunk, record)
        for chunk, record in session.execute(
            base_statement.limit(pool_limit)
        ).all()
    ]
    stages.append(
        CandidateStageTrace(
            corpus="product_truth",
            stage="broadened_scope",
            mode="tenant_scoped_broadening",
            candidate_count=len(broadened),
            broadened=True,
            reason="keyword/vector stages produced zero candidates",
        )
    )
    return CandidatePool(candidates=broadened, stages=stages)


def _load_historical_candidates(
    session: Session,
    *,
    request: RetrievalRequest,
    embeddings: dict[str, list[float]],
) -> CandidatePool:
    pool_limit = _required_candidate_pool_limit(request, "historical_exemplars")
    _validate_historical_corpus_integrity(session, tenant_id=request.tenant_id)
    base_statement = (
        select(HistoricalQARow, HistoricalClientPackage, HistoricalCaseProfile)
        .join(HistoricalWorkbook, HistoricalWorkbook.id == HistoricalQARow.workbook_id)
        .join(HistoricalClientPackage, HistoricalClientPackage.id == HistoricalQARow.client_package_id)
        .join(HistoricalCaseProfile, HistoricalCaseProfile.id == HistoricalQARow.historical_case_profile_id)
        .where(
            HistoricalQARow.tenant_id == request.tenant_id,
            HistoricalQARow.approval_status == ApprovalStatus.APPROVED,
            HistoricalWorkbook.tenant_id == request.tenant_id,
            HistoricalWorkbook.approval_status == ApprovalStatus.APPROVED,
            HistoricalClientPackage.tenant_id == request.tenant_id,
            HistoricalCaseProfile.tenant_id == request.tenant_id,
            HistoricalQARow.source_row_id != request.questionnaire_source_row_id,
            HistoricalWorkbook.file_hash != request.questionnaire_file_hash,
            HistoricalWorkbook.source_file_name != request.questionnaire_file_name,
            HistoricalClientPackage.source_pdf_file_hash != request.current_case_pdf_file_hash,
            HistoricalClientPackage.source_pdf_file_name != request.current_case_pdf_file_name,
        )
    )
    stages: list[CandidateStageTrace] = []
    keyword_query_text = (
        f"{request.row_question_context}\n{request.current_case_signature}\n{request.revision_intent}".strip()
    )
    keyword_clause = _keyword_match_clause(
        (
            HistoricalQARow.normalized_text,
            HistoricalCaseProfile.signature_text,
        ),
        keyword_query_text,
    )
    keyword_rows: list[tuple[HistoricalQARow, HistoricalClientPackage, HistoricalCaseProfile]] = []
    if keyword_clause is None:
        stages.append(
            CandidateStageTrace(
                corpus="historical_exemplar",
                stage="keyword_prefilter",
                mode="sql_contains",
                candidate_count=0,
                skipped=True,
                reason="no_keyword_tokens",
            )
        )
    else:
        keyword_rows = [
            (row, package, profile)
            for row, package, profile in session.execute(base_statement.where(keyword_clause).limit(pool_limit)).all()
        ]
        stages.append(
            CandidateStageTrace(
                corpus="historical_exemplar",
                stage="keyword_prefilter",
                mode="sql_contains",
                candidate_count=len(keyword_rows),
            )
        )
    row_vector_rows: list[tuple[HistoricalQARow, HistoricalClientPackage, HistoricalCaseProfile]] = []
    signature_vector_rows: list[tuple[HistoricalQARow, HistoricalClientPackage, HistoricalCaseProfile]] = []
    if _dialect_name(session) == "postgresql":
        row_vector_rows = [
            (row, package, profile)
            for row, package, profile in session.execute(
                base_statement.order_by(
                    HistoricalQARow.embedding.cosine_distance(embeddings["historical_row_query"])  # type: ignore[attr-defined]
                ).limit(pool_limit)
            ).all()
        ]
        signature_vector_rows = [
            (row, package, profile)
            for row, package, profile in session.execute(
                base_statement.order_by(
                    HistoricalCaseProfile.signature_embedding.cosine_distance(embeddings["current_case_signature"])  # type: ignore[attr-defined]
                ).limit(pool_limit)
            ).all()
        ]
        stages.extend(
            [
                CandidateStageTrace(
                    corpus="historical_exemplar",
                    stage="row_pgvector_prefilter",
                    mode="pgvector_cosine_distance",
                    candidate_count=len(row_vector_rows),
                ),
                CandidateStageTrace(
                    corpus="historical_exemplar",
                    stage="case_signature_pgvector_prefilter",
                    mode="pgvector_cosine_distance",
                    candidate_count=len(signature_vector_rows),
                ),
            ]
        )
    else:
        stages.extend(
            [
                CandidateStageTrace(
                    corpus="historical_exemplar",
                    stage="row_pgvector_prefilter",
                    mode="pgvector_cosine_distance",
                    candidate_count=0,
                    skipped=True,
                    reason="non_postgresql_backend",
                ),
                CandidateStageTrace(
                    corpus="historical_exemplar",
                    stage="case_signature_pgvector_prefilter",
                    mode="pgvector_cosine_distance",
                    candidate_count=0,
                    skipped=True,
                    reason="non_postgresql_backend",
                ),
            ]
        )
    merged = _merge_unique(
        [*keyword_rows, *row_vector_rows, *signature_vector_rows],
        key_fn=lambda item: item[0].id,
    )
    if merged:
        return CandidatePool(candidates=merged, stages=stages)
    if not _broadening_enabled(request) or _broadening_max_stages(request) == 0:
        stages.append(
            CandidateStageTrace(
                corpus="historical_exemplar",
                stage="broadening_disabled",
                mode="tenant_scoped_broadening",
                candidate_count=0,
                skipped=True,
                reason="broadening_disabled_by_pipeline_config",
            )
        )
        return CandidatePool(candidates=[], stages=stages)
    broadened = [
        (row, package, profile)
        for row, package, profile in session.execute(
            base_statement.limit(pool_limit)
        ).all()
    ]
    stages.append(
        CandidateStageTrace(
            corpus="historical_exemplar",
            stage="broadened_scope",
            mode="tenant_scoped_broadening",
            candidate_count=len(broadened),
            broadened=True,
            reason="keyword/vector stages produced zero candidates",
        )
    )
    return CandidatePool(candidates=broadened, stages=stages)


def _score_case_profile_item(
    request: RetrievalRequest,
    embeddings: dict[str, list[float]],
    item: CaseProfileItem,
) -> RetrievedEvidence:
    candidate_embedding = _require_embedding(f"case_profile_item {item.id}", item.embedding)
    component_scores = {
        "row_question_semantic": _cosine_similarity(embeddings["row_question"], candidate_embedding),
        "row_context_semantic": _cosine_similarity(embeddings["row_context"], candidate_embedding),
        "row_question_context_semantic": _cosine_similarity(
            embeddings["row_question_context"],
            candidate_embedding,
        ),
        "row_question_keyword": _keyword_overlap(request.row_question, item.normalized_text),
        "row_context_keyword": _keyword_overlap(request.row_context, item.normalized_text),
        "revision_intent_semantic": _cosine_similarity(
            embeddings.get("revision_intent"),
            candidate_embedding,
        ),
        "revision_intent_keyword": _keyword_overlap(request.revision_intent, item.normalized_text),
    }
    score = (
        (_scoring_multiplier(request, "semantic_weight") * _query_weight(request, "row_question_context") * component_scores["row_question_context_semantic"] * 0.32)
        + (_scoring_multiplier(request, "semantic_weight") * _query_weight(request, "row_context") * component_scores["row_context_semantic"] * 0.20)
        + (_scoring_multiplier(request, "semantic_weight") * _query_weight(request, "row_question") * component_scores["row_question_semantic"] * 0.14)
        + (_scoring_multiplier(request, "keyword_weight") * _query_weight(request, "row_context") * component_scores["row_context_keyword"] * 0.12)
        + (_scoring_multiplier(request, "keyword_weight") * _query_weight(request, "row_question") * component_scores["row_question_keyword"] * 0.10)
        + (_scoring_multiplier(request, "semantic_weight") * _query_weight(request, "user_message") * component_scores["revision_intent_semantic"] * 0.07)
        + (_scoring_multiplier(request, "keyword_weight") * _query_weight(request, "user_message") * component_scores["revision_intent_keyword"] * 0.05)
    )
    metadata: dict[str, object] = {
        "confidence": item.confidence,
        "citations": item.citations,
        "component_scores": component_scores,
        "matched_features": _matched_features(component_scores),
        "provenance": {
            "case_profile_id": str(item.case_profile_id),
            "analysis_item_id": item.analysis_item_id,
            "case_id": str(item.case_id),
        },
    }
    return RetrievedEvidence(
        source_kind=EvidenceSourceKind.CASE_PROFILE_ITEM,
        source_id=item.id,
        source_label="current_case_facts",
        source_title=item.analysis_item_id,
        excerpt=item.answer,
        metadata_json=metadata,
        score=score,
    )


def _score_pdf_chunk(
    request: RetrievalRequest,
    embeddings: dict[str, list[float]],
    chunk: PdfChunk,
) -> RetrievedEvidence:
    candidate_embedding = _require_embedding(f"pdf_chunk {chunk.id}", chunk.embedding)
    component_scores = {
        "row_question_semantic": _cosine_similarity(embeddings["row_question"], candidate_embedding),
        "row_context_semantic": _cosine_similarity(embeddings["row_context"], candidate_embedding),
        "row_question_context_semantic": _cosine_similarity(
            embeddings["row_question_context"],
            candidate_embedding,
        ),
        "row_question_keyword": _keyword_overlap(request.row_question, chunk.content),
        "row_context_keyword": _keyword_overlap(request.row_context, chunk.content),
        "revision_intent_semantic": _cosine_similarity(
            embeddings.get("revision_intent"),
            candidate_embedding,
        ),
        "revision_intent_keyword": _keyword_overlap(request.revision_intent, chunk.content),
    }
    score = (
        (_scoring_multiplier(request, "semantic_weight") * _query_weight(request, "row_question_context") * component_scores["row_question_context_semantic"] * 0.30)
        + (_scoring_multiplier(request, "semantic_weight") * _query_weight(request, "row_context") * component_scores["row_context_semantic"] * 0.18)
        + (_scoring_multiplier(request, "semantic_weight") * _query_weight(request, "row_question") * component_scores["row_question_semantic"] * 0.12)
        + (_scoring_multiplier(request, "keyword_weight") * _query_weight(request, "row_context") * component_scores["row_context_keyword"] * 0.16)
        + (_scoring_multiplier(request, "keyword_weight") * _query_weight(request, "row_question") * component_scores["row_question_keyword"] * 0.12)
        + (_scoring_multiplier(request, "semantic_weight") * _query_weight(request, "user_message") * component_scores["revision_intent_semantic"] * 0.07)
        + (_scoring_multiplier(request, "keyword_weight") * _query_weight(request, "user_message") * component_scores["revision_intent_keyword"] * 0.05)
    )
    metadata: dict[str, object] = {
        "component_scores": component_scores,
        "matched_features": _matched_features(component_scores),
        "provenance": {
            "case_id": str(chunk.case_id),
            "upload_id": str(chunk.upload_id),
            "page_number": chunk.page_number,
            "chunk_index": chunk.chunk_index,
            "start_offset": chunk.start_offset,
            "end_offset": chunk.end_offset,
            "chunk_hash": chunk.chunk_hash,
            "chunking_version": chunk.chunking_version,
            "embedding_model": chunk.embedding_model,
        },
    }
    return RetrievedEvidence(
        source_kind=EvidenceSourceKind.PDF_CHUNK,
        source_id=chunk.id,
        source_label="raw_current_pdf",
        source_title=f"page {chunk.page_number} chunk {chunk.chunk_index}",
        excerpt=chunk.content,
        metadata_json=metadata,
        score=score,
    )


def _score_product_truth(
    request: RetrievalRequest,
    embeddings: dict[str, list[float]],
    chunk: ProductTruthChunk,
    record: ProductTruthRecord,
) -> RetrievedEvidence:
    chunk_embedding = _require_embedding(f"product_truth_chunk {chunk.id}", chunk.embedding)
    language_adjustment, language_metadata = _language_adjustment(request.row_language, chunk.language)
    language_adjustment = _language_bonus(request) if language_metadata["language_match"] else -(_language_bonus(request) / 4.0)
    component_scores = {
        "row_question_context_semantic": _cosine_similarity(
            embeddings["row_question_context"],
            chunk_embedding,
        ),
        "current_case_signature_semantic": _cosine_similarity(
            embeddings["current_case_signature"],
            chunk_embedding,
        ),
        "row_question_context_keyword": _keyword_overlap(
            request.row_question_context,
            chunk.content,
        ),
        "current_case_signature_keyword": _keyword_overlap(
            request.current_case_signature,
            chunk.content,
        ),
        "revision_intent_semantic": _cosine_similarity(
            embeddings.get("revision_intent"),
            chunk_embedding,
        ),
        "revision_intent_keyword": _keyword_overlap(request.revision_intent, chunk.content),
        "language_adjustment": language_adjustment,
    }
    score = (
        (_scoring_multiplier(request, "semantic_weight") * _query_weight(request, "row_question_context") * component_scores["row_question_context_semantic"] * 0.34)
        + (_scoring_multiplier(request, "semantic_weight") * _query_weight(request, "current_case_signature") * component_scores["current_case_signature_semantic"] * 0.20)
        + (_scoring_multiplier(request, "keyword_weight") * _query_weight(request, "row_question_context") * component_scores["row_question_context_keyword"] * 0.18)
        + (_scoring_multiplier(request, "keyword_weight") * _query_weight(request, "current_case_signature") * component_scores["current_case_signature_keyword"] * 0.12)
        + (_scoring_multiplier(request, "semantic_weight") * _query_weight(request, "user_message") * component_scores["revision_intent_semantic"] * 0.10)
        + (_scoring_multiplier(request, "keyword_weight") * _query_weight(request, "user_message") * component_scores["revision_intent_keyword"] * 0.06)
        + component_scores["language_adjustment"]
    )
    metadata: dict[str, object] = {
        "language": chunk.language,
        **language_metadata,
        "component_scores": component_scores,
        "matched_features": _matched_features(component_scores),
        "provenance": {
            "truth_record_id": str(record.id),
            "source_file_name": record.source_file_name,
            "source_section": record.source_section,
            "effective_from": record.effective_from.isoformat(),
            "effective_to": record.effective_to.isoformat() if record.effective_to else None,
            "version": record.version,
        },
    }
    return RetrievedEvidence(
        source_kind=EvidenceSourceKind.PRODUCT_TRUTH_CHUNK,
        source_id=chunk.id,
        source_label="product_truth",
        source_title=record.title,
        excerpt=chunk.content,
        metadata_json=metadata,
        score=score,
    )


def _historical_exemplar_excerpt(row: HistoricalQARow) -> str:
    return (
        f"Historical client context: {row.context_raw}\n"
        f"Historical question: {row.question_raw}\n"
        f"Historical answer exemplar: {row.answer_raw}"
    )


def _score_historical_exemplar(
    request: RetrievalRequest,
    embeddings: dict[str, list[float]],
    row: HistoricalQARow,
    package: HistoricalClientPackage,
    profile: HistoricalCaseProfile,
) -> RetrievedEvidence:
    row_embedding = _require_embedding(f"historical_qa_row {row.id}", row.embedding)
    case_signature_embedding = _require_embedding(
        f"historical_case_profile {profile.id}",
        profile.signature_embedding,
    )
    language_adjustment, language_metadata = _language_adjustment(request.row_language, row.language)
    language_adjustment = _language_bonus(request) if language_metadata["language_match"] else -(_language_bonus(request) / 4.0)
    component_scores = {
        "row_question_semantic": _cosine_similarity(embeddings["row_question"], row_embedding),
        "row_context_semantic": _cosine_similarity(embeddings["row_context"], row_embedding),
        "row_question_context_semantic": _cosine_similarity(
            embeddings["row_question_context"],
            row_embedding,
        ),
        "row_question_context_keyword": _keyword_overlap(
            request.row_question_context,
            row.normalized_text,
        ),
        "revision_intent_semantic": _cosine_similarity(
            embeddings.get("revision_intent"),
            row_embedding,
        ),
        "revision_intent_keyword": _keyword_overlap(request.revision_intent, row.normalized_text),
        "current_case_signature_semantic": _cosine_similarity(
            embeddings["current_case_signature"],
            case_signature_embedding,
        ),
        "current_case_signature_keyword": _keyword_overlap(
            request.current_case_signature,
            profile.signature_text,
        ),
        "language_adjustment": language_adjustment,
    }
    score = (
        (_scoring_multiplier(request, "semantic_weight") * _scoring_multiplier(request, "historical_row_weight") * _query_weight(request, "row_question_context") * component_scores["row_question_context_semantic"] * 0.18)
        + (_scoring_multiplier(request, "semantic_weight") * _scoring_multiplier(request, "historical_row_weight") * _query_weight(request, "row_context") * component_scores["row_context_semantic"] * 0.10)
        + (_scoring_multiplier(request, "semantic_weight") * _scoring_multiplier(request, "historical_row_weight") * _query_weight(request, "row_question") * component_scores["row_question_semantic"] * 0.06)
        + (_scoring_multiplier(request, "keyword_weight") * _scoring_multiplier(request, "historical_row_weight") * _query_weight(request, "row_question_context") * component_scores["row_question_context_keyword"] * 0.18)
        + (_scoring_multiplier(request, "semantic_weight") * _scoring_multiplier(request, "historical_case_weight") * _query_weight(request, "current_case_signature") * component_scores["current_case_signature_semantic"] * 0.14)
        + (_scoring_multiplier(request, "keyword_weight") * _scoring_multiplier(request, "historical_case_weight") * _query_weight(request, "current_case_signature") * component_scores["current_case_signature_keyword"] * 0.18)
        + (_scoring_multiplier(request, "semantic_weight") * _scoring_multiplier(request, "historical_row_weight") * _query_weight(request, "user_message") * component_scores["revision_intent_semantic"] * 0.04)
        + (_scoring_multiplier(request, "keyword_weight") * _scoring_multiplier(request, "historical_row_weight") * _query_weight(request, "user_message") * component_scores["revision_intent_keyword"] * 0.02)
        + component_scores["language_adjustment"]
    )
    metadata: dict[str, object] = {
        "client_name": row.client_name,
        "language": row.language,
        "question": row.question_raw,
        "context": row.context_raw,
        **language_metadata,
        "component_scores": component_scores,
        "matched_features": _matched_features(component_scores),
        "provenance": {
            "dataset_id": str(row.dataset_id),
            "workbook_id": str(row.workbook_id),
            "client_package_id": str(package.id),
            "historical_case_profile_id": str(profile.id),
            "client_slug": package.client_slug,
            "source_file_name": row.source_file_name,
            "source_sheet_name": row.source_sheet_name,
            "source_row_number": row.source_row_number,
            "source_row_id": row.source_row_id,
            "source_pdf_file_name": package.source_pdf_file_name,
            "source_pdf_file_hash": package.source_pdf_file_hash,
            "source_workbook_file_hash": row.file_hash,
            "prompt_set_version": profile.prompt_set_version,
            "schema_version": profile.schema_version,
            "signature_version": profile.signature_version,
            "signature_embedding_model": profile.signature_embedding_model,
        },
    }
    return RetrievedEvidence(
        source_kind=EvidenceSourceKind.HISTORICAL_QA_ROW,
        source_id=row.id,
        source_label="historical_exemplar",
        source_title=row.source_row_id,
        excerpt=_historical_exemplar_excerpt(row),
        metadata_json=metadata,
        score=score,
    )


def _dedup_key(item: RetrievedEvidence) -> tuple[str, str]:
    provenance = item.metadata_json.get("provenance", {})
    if not isinstance(provenance, dict):
        return (item.source_label, str(item.source_id))
    if item.source_label == "current_case_facts":
        return (item.source_label, str(provenance.get("analysis_item_id", item.source_id)))
    if item.source_label == "raw_current_pdf":
        return (item.source_label, str(provenance.get("chunk_hash", item.source_id)))
    if item.source_label == "product_truth":
        truth_record_id = provenance.get("truth_record_id", item.source_id)
        title = item.source_title
        return (item.source_label, f"{truth_record_id}:{title}")
    if item.source_label == "historical_exemplar":
        return (
            item.source_label,
            f"{provenance.get('source_row_id', item.source_id)}:{provenance.get('source_pdf_file_hash', '')}",
        )
    return (item.source_label, str(item.source_id))


def _deduplicate_evidence(
    evidence: Sequence[RetrievedEvidence],
) -> tuple[list[RetrievedEvidence], list[dict[str, object]]]:
    kept: dict[tuple[str, str], RetrievedEvidence] = {}
    dropped: dict[tuple[str, str], list[str]] = {}
    for item in evidence:
        key = _dedup_key(item)
        existing = kept.get(key)
        if existing is None or item.score > existing.score:
            if existing is not None:
                dropped.setdefault(key, []).append(str(existing.source_id))
            kept[key] = item
            continue
        dropped.setdefault(key, []).append(str(item.source_id))
    decisions: list[dict[str, object]] = [
        {
            "source_label": label,
            "dedup_key": dedup_key,
            "kept_source_id": str(kept[(label, dedup_key)].source_id),
            "dropped_source_ids": dropped_source_ids,
        }
        for (label, dedup_key), dropped_source_ids in dropped.items()
    ]
    return list(kept.values()), decisions


def _assess_retrieval_sufficiency(
    *,
    selected: Sequence[RetrievedEvidence],
    stages: Sequence[CandidateStageTrace],
    request: RetrievalRequest,
) -> RetrievalAssessment:
    counts = {
        label: len([item for item in selected if item.source_label == label])
        for label in ("current_case_facts", "raw_current_pdf", "product_truth", "historical_exemplar")
    }
    broadened = any(stage.broadened for stage in stages)
    notes: list[str] = []
    if counts["current_case_facts"] + counts["raw_current_pdf"] == 0:
        notes.append("No current-case evidence was selected.")
        return RetrievalAssessment(status="insufficient", degraded=True, notes=notes)
    average_score = sum(item.score for item in selected) / len(selected) if selected else 0.0
    threshold = float(_retrieval_config(request).sufficiency.threshold)
    if average_score < threshold:
        notes.append(
            f"Average selected evidence score {average_score:.3f} fell below configured sufficiency threshold {threshold:.3f}."
        )
        return RetrievalAssessment(status="degraded", degraded=True, notes=notes)
    if broadened:
        notes.append("Candidate generation broadened beyond the primary prefilter path.")
    if counts["product_truth"] == 0:
        notes.append("No product-truth evidence was selected.")
    if counts["historical_exemplar"] == 0:
        notes.append("No historical exemplar evidence was selected.")
    if not broadened and counts["current_case_facts"] >= 2 and counts["product_truth"] >= 1 and counts["historical_exemplar"] >= 1:
        return RetrievalAssessment(status="sufficient", degraded=False, notes=notes)
    if not broadened and counts["current_case_facts"] + counts["raw_current_pdf"] >= 1 and (
        counts["product_truth"] >= 1 or counts["historical_exemplar"] >= 1
    ):
        return RetrievalAssessment(status="weak", degraded=False, notes=notes)
    return RetrievalAssessment(status="degraded", degraded=True, notes=notes)


def _clone_retrieval_run(
    session: Session,
    *,
    tenant_id: UUID,
    case_id: UUID,
    row: QuestionnaireRow,
    thread_id: UUID,
    request: RetrievalRequest,
    source_run: RetrievalRun,
    execution_run_id: UUID,
) -> tuple[RetrievalRun, list[RetrievedEvidence]]:
    source_execution = source_run.request_context.get("retrieval_execution")
    if not isinstance(source_execution, dict):
        raise ValidationFailure(
            f"Retrieval run {source_run.id} cannot be reused because execution metadata is missing."
        )
    source_items = session.scalars(
        select(RetrievalSnapshotItem)
        .where(RetrievalSnapshotItem.retrieval_run_id == source_run.id)
        .order_by(RetrievalSnapshotItem.rank.asc())
    ).all()
    if not source_items:
        raise ValidationFailure(
            f"Retrieval run {source_run.id} cannot be reused because its snapshot is missing."
        )
    request_context = request.to_request_context()
    request_context["retrieval_execution"] = {
        **source_execution,
        "stages": [
            {
                "corpus": "all",
                "stage": "reuse_snapshot",
                "mode": "reused_snapshot",
                "candidate_count": len(source_items),
                "broadened": False,
                "skipped": False,
                "reason": "style_only_revision_reused_previous_snapshot",
            }
        ],
        "reused_snapshot": True,
    }
    retrieval_run = RetrievalRun(
        tenant_id=tenant_id,
        case_id=case_id,
        questionnaire_row_id=row.id,
        chat_thread_id=thread_id,
        execution_run_id=execution_run_id,
        query_text=request.query_text,
        request_context=request_context,
        prompt_authority_order=AUTHORITY_ORDER,
    )
    session.add(retrieval_run)
    session.flush()
    evidence: list[RetrievedEvidence] = []
    for item in source_items:
        session.add(
            RetrievalSnapshotItem(
                tenant_id=tenant_id,
                case_id=case_id,
                retrieval_run_id=retrieval_run.id,
                source_kind=item.source_kind,
                source_id=item.source_id,
                source_label=item.source_label,
                source_title=item.source_title,
                excerpt=item.excerpt,
                metadata_json=item.metadata_json,
                score=item.score,
                rank=item.rank,
            )
        )
        evidence.append(
            RetrievedEvidence(
                source_kind=item.source_kind,
                source_id=item.source_id,
                source_label=item.source_label,
                source_title=item.source_title,
                excerpt=item.excerpt,
                metadata_json=item.metadata_json,
                score=item.score,
            )
        )
    session.flush()
    return retrieval_run, evidence


def build_retrieval_run(
    session: Session,
    *,
    ai_service: AIService,
    tenant_id: UUID,
    case_id: UUID,
    row: QuestionnaireRow,
    thread_id: UUID,
    request: RetrievalRequest,
    reuse_from_run: RetrievalRun | None = None,
    reproducibility_mode: ReproducibilityMode = ReproducibilityMode.BEST_EFFORT,
    parent_run_id: UUID | None = None,
) -> tuple[RetrievalRun, list[RetrievedEvidence]]:
    if row.tenant_id != tenant_id or row.case_id != case_id:
        raise ScopeViolation("Questionnaire row does not belong to the active tenant/case.")
    case = session.get(RfxCase, case_id)
    if case is None:
        raise ValidationFailure(f"Case {case_id} does not exist for retrieval.")
    repro = start_repro_run(
        session,
        storage=None,
        settings=get_settings(),
        kind=ExecutionRunKind.RETRIEVAL,
        mode=reproducibility_mode,
        tenant_id=tenant_id,
        case_id=case_id,
        user_id=None,
        parent_run_id=parent_run_id,
        pipeline_profile_name=request.pipeline.profile_name,
        pipeline_config_hash=request.pipeline.config_hash,
        index_config_hash=request.pipeline.index_config_hash,
        runtime_config_hash=request.pipeline.runtime_config_hash,
        inputs_json=request.to_request_context(),
    )
    assert_pipeline_runtime_compatibility(
        session,
        case=case,
        selection=request.pipeline,
    )
    if (
        reproducibility_mode == ReproducibilityMode.STRICT_EVAL
        and (case.case_profile_build_id is None or case.current_pdf_build_id is None)
    ):
        raise ValidationFailure(
            f"Case {case.id} is missing required current-case build lineage for strict_eval retrieval."
        )
    try:
        if request.retrieval_action == "reuse_previous_snapshot":
            request_date = date.today()
            if reuse_from_run is None:
                raise ValidationFailure("Style-only revision requested snapshot reuse without a source run.")
            source_execution_run = (
                session.get(ExecutionRun, reuse_from_run.execution_run_id)
                if reuse_from_run.execution_run_id is not None
                else None
            )
            source_replay = (
                source_execution_run.replay_json
                if source_execution_run is not None and isinstance(source_execution_run.replay_json, dict)
                else {}
            )
            cloned_run, cloned_evidence = _clone_retrieval_run(
                session,
                tenant_id=tenant_id,
                case_id=case_id,
                row=row,
                thread_id=thread_id,
                request=request,
                source_run=reuse_from_run,
                execution_run_id=repro.execution_run.id,
            )
            finish_execution_run(
                repro.execution_run,
                outputs_json={
                    "retrieval_run_id": str(cloned_run.id),
                    "selected_evidence_count": len(cloned_evidence),
                    "reused_snapshot": True,
                },
                replay_json={
                    "reused_from_retrieval_run_id": str(reuse_from_run.id),
                    "case_profile_build_id": source_replay.get("case_profile_build_id"),
                    "current_pdf_build_id": source_replay.get("current_pdf_build_id"),
                    "historical_build_ids": source_replay.get("historical_build_ids", []),
                    "product_truth_build_ids": source_replay.get("product_truth_build_ids", []),
                    "candidate_pools": source_replay.get("candidate_pools", {}),
                    "selected_evidence": source_replay.get("selected_evidence", []),
                    "query_embeddings": source_replay.get("query_embeddings", {}),
                    "stage_traces": source_replay.get("stage_traces", []),
                    "request_date": request_date.isoformat(),
                },
            )
            if reproducibility_mode == ReproducibilityMode.STRICT_EVAL:
                assert_execution_run_consistency(session, run=repro.execution_run)
            return cloned_run, cloned_evidence

        request_date = date.today()
        embeddings, query_embedding_lineage = _build_query_embeddings(
            session,
            ai_service,
            request,
            execution_run=repro.execution_run,
        )
        case_facts_pool = _load_current_case_fact_candidates(
            session,
            request=request,
            tenant_id=tenant_id,
            case_id=case_id,
        )
        raw_pdf_pool = _load_raw_pdf_candidates(
            session,
            request=request,
            embeddings=embeddings,
        )
        product_truth_pool = _load_product_truth_candidates(
            session,
            request=request,
            embeddings=embeddings,
            effective_date=request_date,
        )
        historical_pool = _load_historical_candidates(
            session,
            request=request,
            embeddings=embeddings,
        )

        evidence: list[RetrievedEvidence] = []
        evidence.extend(
            _score_case_profile_item(request, embeddings, item)
            for item in case_facts_pool.candidates
        )
        evidence.extend(
            _score_pdf_chunk(request, embeddings, item) for item in raw_pdf_pool.candidates
        )
        evidence.extend(
            _score_product_truth(request, embeddings, chunk, record)
            for chunk, record in product_truth_pool.candidates
        )
        evidence.extend(
            _score_historical_exemplar(request, embeddings, row_item, package, profile)
            for row_item, package, profile in historical_pool.candidates
        )
        if _dedup_mode(request) == "provenance":
            evidence, dedup = _deduplicate_evidence(evidence)
        else:
            dedup = []

        grouped: dict[str, list[RetrievedEvidence]] = {
            "current_case_facts": [],
            "raw_current_pdf": [],
            "product_truth": [],
            "historical_exemplar": [],
        }
        for evidence_item in evidence:
            grouped[evidence_item.source_label].append(evidence_item)

        ranked: list[RetrievedEvidence] = []
        selected_counts: dict[str, int] = {}
        for source_label in ("current_case_facts", "raw_current_pdf", "product_truth", "historical_exemplar"):
            minimum_score = MINIMUM_SOURCE_SCORES[source_label]
            selected = [
                item
                for item in sorted(grouped[source_label], key=lambda item: item.score, reverse=True)
                if item.score >= minimum_score
            ][: _final_quota(request, source_label)]
            selected_counts[source_label] = len(selected)
            ranked.extend(selected)

        all_stages = [
            *case_facts_pool.stages,
            *raw_pdf_pool.stages,
            *product_truth_pool.stages,
            *historical_pool.stages,
        ]
        sufficiency = _assess_retrieval_sufficiency(
            selected=ranked,
            stages=all_stages,
            request=request,
        )

        request_context = request.to_request_context()
        request_context["retrieval_execution"] = _build_execution_context(
            session=session,
            request=request,
            stages=all_stages,
            sufficiency=sufficiency,
            selected_counts=selected_counts,
            dedup=dedup,
        )
        retrieval_run = RetrievalRun(
            tenant_id=tenant_id,
            case_id=case_id,
            questionnaire_row_id=row.id,
            chat_thread_id=thread_id,
            execution_run_id=repro.execution_run.id,
            query_text=request.query_text,
            request_context=request_context,
            prompt_authority_order=AUTHORITY_ORDER,
        )
        session.add(retrieval_run)
        session.flush()
        for rank, entry in enumerate(ranked, start=1):
            session.add(
                RetrievalSnapshotItem(
                    tenant_id=tenant_id,
                    case_id=case_id,
                    retrieval_run_id=retrieval_run.id,
                    source_kind=entry.source_kind,
                    source_id=entry.source_id,
                    source_label=entry.source_label,
                    source_title=entry.source_title,
                    excerpt=entry.excerpt,
                    metadata_json=entry.metadata_json,
                    score=entry.score,
                    rank=rank,
                )
            )
        session.flush()
        historical_build_ids = [
            dataset_build_id
            for dataset_build_id in session.scalars(
                select(HistoricalDataset.artifact_build_id).where(
                    HistoricalDataset.id.in_({row_item.dataset_id for row_item, _package, _profile in historical_pool.candidates})
                )
            ).all()
            if dataset_build_id is not None
        ]
        evidence_pack_hash = sha256_text(
            json.dumps(
                [
                    {
                        "source_label": item.source_label,
                        "source_id": str(item.source_id),
                        "score": item.score,
                    }
                    for item in ranked
                ],
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        finish_execution_run(
            repro.execution_run,
            outputs_json={
                "retrieval_run_id": str(retrieval_run.id),
                "selected_evidence_count": len(ranked),
                "evidence_pack_hash": evidence_pack_hash,
            },
            replay_json={
                "case_profile_build_id": str(case.case_profile_build_id) if case.case_profile_build_id else None,
                "current_pdf_build_id": str(case.current_pdf_build_id) if case.current_pdf_build_id else None,
                "historical_build_ids": [str(item) for item in historical_build_ids],
                "product_truth_build_ids": [
                    str(record.artifact_build_id)
                    for _chunk, record in product_truth_pool.candidates
                    if record.artifact_build_id is not None
                ],
                "candidate_pools": {
                    "current_case_facts": _serialize_candidate_pool("current_case_facts", case_facts_pool.candidates),
                    "raw_current_pdf": _serialize_candidate_pool("raw_current_pdf", raw_pdf_pool.candidates),
                    "product_truth": _serialize_candidate_pool("product_truth", product_truth_pool.candidates),
                    "historical_exemplar": _serialize_candidate_pool("historical_exemplar", historical_pool.candidates),
                },
                "selected_evidence": [
                    {
                        "source_label": item.source_label,
                        "source_id": str(item.source_id),
                        "score": item.score,
                    }
                    for item in ranked
                ],
                "query_embeddings": query_embedding_lineage,
                "stage_traces": [stage.to_dict() for stage in all_stages],
                "request_date": request_date.isoformat(),
            },
        )
        if reproducibility_mode == ReproducibilityMode.STRICT_EVAL:
            assert_execution_run_consistency(session, run=repro.execution_run)
        return retrieval_run, ranked
    except Exception as exc:
        fail_execution_run(repro.execution_run, error_detail=str(exc))
        raise
