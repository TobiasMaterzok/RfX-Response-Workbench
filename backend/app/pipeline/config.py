from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, ValidationError, model_validator

from app.config import Settings
from app.exceptions import ValidationFailure
from app.prompts.case_profile import CASE_PROFILE_PROMPT_SET_VERSION, CASE_PROFILE_SCHEMA_VERSION
from app.services.hashing import sha256_text

PIPELINE_CONFIG_SCHEMA_VERSION = "rfx_pipeline.v1"
DEFAULT_PIPELINE_PROFILE_NAME = "default"
_PIPELINE_DIR = Path(__file__).resolve().parent
_DEFAULT_PROFILE_PATH = _PIPELINE_DIR / "default_profile.json"
_DEFAULT_CURRENT_PDF_CHUNK_SIZE = 900
_DEFAULT_CURRENT_PDF_CHUNK_OVERLAP = 150


class CurrentPdfIndexingConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    chunk_unit: Literal["legacy_char", "token"] = "legacy_char"
    chunk_size: int = Field(
        ge=1,
        validation_alias=AliasChoices("chunk_size", "chunk_size_tokens"),
    )
    chunk_overlap: int = Field(
        ge=0,
        validation_alias=AliasChoices("chunk_overlap", "chunk_overlap_tokens"),
    )
    contextualize_chunks: bool

    @model_validator(mode="after")
    def validate_supported_values(self) -> CurrentPdfIndexingConfig:
        if self.chunk_overlap >= self.chunk_size:
            raise ValueError(
                "indexing.current_pdf.chunk_overlap must be smaller than chunk_size."
            )
        if self.chunk_unit == "legacy_char":
            if self.chunk_size != _DEFAULT_CURRENT_PDF_CHUNK_SIZE:
                raise ValueError(
                    "indexing.current_pdf.chunk_size may differ from the default only when "
                    "indexing.current_pdf.chunk_unit='token'."
                )
            if self.chunk_overlap != _DEFAULT_CURRENT_PDF_CHUNK_OVERLAP:
                raise ValueError(
                    "indexing.current_pdf.chunk_overlap may differ from the default only when "
                    "indexing.current_pdf.chunk_unit='token'."
                )
        return self


class HistoricalIndexingConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    signature_mode: Literal["summary_plus_analysis_items", "summary_only"]


class IndexingConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    embedding_model: str | None
    current_pdf: CurrentPdfIndexingConfig
    historical: HistoricalIndexingConfig


class RetrievalQueryWeights(BaseModel):
    model_config = ConfigDict(extra="forbid")

    row_context: float = Field(ge=0.0)
    row_question: float = Field(ge=0.0)
    user_message: float = Field(ge=0.0)
    current_case_signature: float = Field(ge=0.0)
    row_question_context: float = Field(ge=0.0)


class RetrievalScoringConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    semantic_weight: float = Field(ge=0.0)
    keyword_weight: float = Field(ge=0.0)
    historical_row_weight: float = Field(ge=0.0)
    historical_case_weight: float = Field(ge=0.0)
    same_language_bonus: float = Field(ge=0.0)


class RetrievalCandidatePoolConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    current_case_facts: int | None = Field(default=None, ge=1)
    current_pdf_evidence: int = Field(ge=1)
    product_truth: int = Field(ge=1)
    historical_exemplars: int = Field(ge=1)


class RetrievalFinalQuotaConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    current_case_facts: int = Field(ge=0)
    current_pdf_evidence: int = Field(ge=0)
    product_truth: int = Field(ge=0)
    historical_exemplars: int = Field(ge=0)


class RetrievalSufficiencyConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    threshold: float = Field(ge=0.0)


class RetrievalBroadeningConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool
    max_stages: int = Field(ge=0)

    @model_validator(mode="after")
    def validate_supported_stages(self) -> RetrievalBroadeningConfig:
        if self.max_stages > 1:
            raise ValueError(
                "retrieval.broadening.max_stages currently supports only 0 or 1 because the repo has a single broadened scope stage."
            )
        return self


class RetrievalDedupConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: Literal["provenance", "off"]


class RetrievalConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    query_weights: RetrievalQueryWeights
    scoring: RetrievalScoringConfig
    candidate_pool: RetrievalCandidatePoolConfig
    final_quota: RetrievalFinalQuotaConfig
    sufficiency: RetrievalSufficiencyConfig
    broadening: RetrievalBroadeningConfig
    dedup: RetrievalDedupConfig


class PackingConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_context_tokens: int | None = Field(default=None, ge=1)
    order_strategy: Literal["source_block_order"]
    source_block_order: list[
        Literal["current_case_facts", "raw_current_pdf", "product_truth", "historical_exemplars"]
    ]

    @model_validator(mode="after")
    def validate_order(self) -> PackingConfig:
        expected = {"current_case_facts", "raw_current_pdf", "product_truth", "historical_exemplars"}
        if set(self.source_block_order) != expected or len(self.source_block_order) != len(expected):
            raise ValueError("packing.source_block_order must contain each source block exactly once.")
        return self


class StageModelConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    model_id: str | None = None
    reasoning_effort: Literal["low", "medium", "high"] | None = None


class ModelsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    case_profile_extraction: StageModelConfig = Field(default_factory=StageModelConfig)
    answer_planning: StageModelConfig = Field(default_factory=StageModelConfig)
    answer_rendering: StageModelConfig = Field(default_factory=StageModelConfig)


class GenerationConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    model_id: str | None = None
    reasoning_effort: Literal["low", "medium", "high"] | None = None
    temperature: float | None = Field(default=None, ge=0.0, le=2.0)
    target_answer_words_min: int | None = Field(default=None, ge=1)
    target_answer_words_max: int | None = Field(default=None, ge=1)

    @model_validator(mode="after")
    def validate_word_bounds(self) -> GenerationConfig:
        if (
            self.target_answer_words_min is not None
            and self.target_answer_words_max is not None
            and self.target_answer_words_min > self.target_answer_words_max
        ):
            raise ValueError("generation.target_answer_words_min cannot exceed target_answer_words_max.")
        return self


class RevisionConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    classifier_version: Literal["revision_classifier.v2"]
    style_only_reuses_previous_snapshot: bool = True

    @model_validator(mode="after")
    def validate_fixed_behavior(self) -> RevisionConfig:
        if not self.style_only_reuses_previous_snapshot:
            raise ValueError(
                "revision.style_only_reuses_previous_snapshot is fixed to true for the current implementation."
            )
        return self


class PipelineConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    pipeline_version: Literal["rfx_pipeline.v1"]
    indexing: IndexingConfig
    retrieval: RetrievalConfig
    packing: PackingConfig
    models: ModelsConfig = Field(default_factory=ModelsConfig)
    generation: GenerationConfig
    revision: RevisionConfig


@dataclass(frozen=True)
class PipelineSelection:
    profile_name: str | None
    used_default_profile: bool
    override_supplied: bool
    config: PipelineConfig
    resolved_pipeline: PipelineConfig
    resolved_config: dict[str, object]
    config_hash: str
    index_config_hash: str
    runtime_config_hash: str


@dataclass(frozen=True)
class ArtifactIndexHashes:
    current_pdf: str
    case_profile: str
    historical: str
    product_truth: str


def _load_default_profile_payload() -> dict[str, object]:
    return json.loads(_DEFAULT_PROFILE_PATH.read_text(encoding="utf-8"))


def pipeline_config_json_schema() -> dict[str, object]:
    return PipelineConfig.model_json_schema()


def _deep_merge(base: dict[str, object], override: dict[str, object]) -> dict[str, object]:
    result = deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result[key], value)  # type: ignore[arg-type]
        else:
            result[key] = value
    return result


def _validate_pipeline_payload(payload: dict[str, object]) -> PipelineConfig:
    try:
        return PipelineConfig.model_validate(payload)
    except ValidationError as exc:
        raise ValidationFailure(f"Pipeline config validation failed: {exc}") from exc


def _resolve_models(config: PipelineConfig, settings: Settings) -> PipelineConfig:
    resolved = config.model_copy(deep=True)
    if resolved.indexing.embedding_model is None:
        resolved.indexing.embedding_model = settings.openai_embedding_model
    legacy_model_id = resolved.generation.model_id
    legacy_reasoning = resolved.generation.reasoning_effort
    rendering_stage = resolved.models.answer_rendering
    planning_stage = resolved.models.answer_planning
    extraction_stage = resolved.models.case_profile_extraction
    if (
        legacy_model_id is not None
        and rendering_stage.model_id is not None
        and legacy_model_id != rendering_stage.model_id
    ):
        if rendering_stage.model_id != settings.openai_response_model:
            raise ValidationFailure(
                "Pipeline config validation failed: generation.model_id conflicts with models.answer_rendering.model_id."
            )
        rendering_stage.model_id = legacy_model_id
    if (
        legacy_reasoning is not None
        and rendering_stage.reasoning_effort is not None
        and legacy_reasoning != rendering_stage.reasoning_effort
    ):
        raise ValidationFailure(
            "Pipeline config validation failed: generation.reasoning_effort conflicts with models.answer_rendering.reasoning_effort."
        )
    if extraction_stage.model_id is None:
        extraction_stage.model_id = settings.openai_response_model
    if planning_stage.model_id is None:
        planning_stage.model_id = legacy_model_id or settings.openai_response_model
    elif legacy_model_id is not None and planning_stage.model_id == settings.openai_response_model:
        planning_stage.model_id = legacy_model_id
    if rendering_stage.model_id is None:
        rendering_stage.model_id = legacy_model_id or settings.openai_response_model
    if planning_stage.reasoning_effort is None:
        planning_stage.reasoning_effort = legacy_reasoning
    if rendering_stage.reasoning_effort is None:
        rendering_stage.reasoning_effort = legacy_reasoning
    resolved.generation.model_id = rendering_stage.model_id
    resolved.generation.reasoning_effort = rendering_stage.reasoning_effort
    return resolved


def _hash_payload(payload: dict[str, object]) -> str:
    return sha256_text(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True))


def indexing_payload(config: PipelineConfig) -> dict[str, object]:
    return config.model_dump(mode="json")["indexing"]


def current_pdf_index_payload(config: PipelineConfig) -> dict[str, object]:
    indexing = config.model_dump(mode="json")["indexing"]
    return {
        "embedding_model": indexing["embedding_model"],
        "current_pdf": indexing["current_pdf"],
    }


def case_profile_index_payload(config: PipelineConfig) -> dict[str, object]:
    indexing = config.model_dump(mode="json")["indexing"]
    models = config.model_dump(mode="json")["models"]
    return {
        "embedding_model": indexing["embedding_model"],
        "case_profile_extraction": models["case_profile_extraction"],
    }


def historical_index_payload(config: PipelineConfig) -> dict[str, object]:
    indexing = config.model_dump(mode="json")["indexing"]
    models = config.model_dump(mode="json")["models"]
    return {
        "embedding_model": indexing["embedding_model"],
        "historical": indexing["historical"],
        "case_profile_extraction": models["case_profile_extraction"],
    }


def product_truth_index_payload(config: PipelineConfig) -> dict[str, object]:
    indexing = config.model_dump(mode="json")["indexing"]
    return {
        "embedding_model": indexing["embedding_model"],
    }


def runtime_payload(config: PipelineConfig) -> dict[str, object]:
    dumped = config.model_dump(mode="json")
    return {
        "retrieval": dumped["retrieval"],
        "packing": dumped["packing"],
        "models": {
            "answer_planning": dumped["models"]["answer_planning"],
            "answer_rendering": dumped["models"]["answer_rendering"],
        },
        "generation": dumped["generation"],
        "revision": dumped["revision"],
    }


def resolve_pipeline_selection(
    settings: Settings,
    *,
    profile_name: str | None = None,
    override: dict[str, object] | None = None,
    pinned_config: dict[str, object] | None = None,
    pinned_profile_name: str | None = None,
) -> PipelineSelection:
    if profile_name is not None and profile_name != DEFAULT_PIPELINE_PROFILE_NAME:
        raise ValidationFailure(
            f"Unsupported pipeline profile {profile_name!r}. Only {DEFAULT_PIPELINE_PROFILE_NAME!r} is available today."
        )
    if pinned_profile_name is not None and pinned_profile_name != DEFAULT_PIPELINE_PROFILE_NAME:
        raise ValidationFailure(
            f"Pinned pipeline profile {pinned_profile_name!r} is not supported by the current repo."
        )
    if pinned_config is not None and profile_name is None:
        base_payload = deepcopy(pinned_config)
    else:
        base_payload = _load_default_profile_payload()
    if not isinstance(base_payload, dict):
        raise ValidationFailure("Pinned pipeline config payload is malformed.")
    merged_payload = _deep_merge(base_payload, override or {})
    config = _validate_pipeline_payload(merged_payload)
    resolved = _resolve_models(config, settings)
    resolved_payload = resolved.model_dump(mode="json")
    return PipelineSelection(
        profile_name=profile_name or pinned_profile_name or DEFAULT_PIPELINE_PROFILE_NAME,
        used_default_profile=pinned_config is None and profile_name is None,
        override_supplied=override is not None,
        config=config,
        resolved_pipeline=resolved,
        resolved_config=resolved_payload,
        config_hash=_hash_payload(resolved_payload),
        index_config_hash=_hash_payload(indexing_payload(resolved)),
        runtime_config_hash=_hash_payload(runtime_payload(resolved)),
    )


def artifact_index_hashes(selection: PipelineSelection) -> ArtifactIndexHashes:
    resolved = selection.resolved_pipeline
    return ArtifactIndexHashes(
        current_pdf=_hash_payload(current_pdf_index_payload(resolved)),
        case_profile=_hash_payload(case_profile_index_payload(resolved)),
        historical=_hash_payload(historical_index_payload(resolved)),
        product_truth=_hash_payload(product_truth_index_payload(resolved)),
    )


def assert_case_index_compatibility(
    session,
    *,
    case,
    selection: PipelineSelection,
) -> None:
    from sqlalchemy import select

    from app.models.entities import CaseProfile, PdfChunk

    if case.pipeline_config_json is None or case.index_config_hash is None:
        raise ValidationFailure(f"Case {case.id} is missing pipeline index config provenance.")
    expected_hashes = artifact_index_hashes(selection)
    profile = session.scalar(
        select(CaseProfile)
        .where(CaseProfile.case_id == case.id)
        .order_by(CaseProfile.created_at.desc())
    )
    if profile is None or profile.index_config_hash != expected_hashes.case_profile:
        raise ValidationFailure(
            f"Case {case.id} case_profile artifacts do not match expected case-profile index hash {expected_hashes.case_profile}."
        )
    if profile.prompt_set_version != CASE_PROFILE_PROMPT_SET_VERSION:
        raise ValidationFailure(
            f"Case {case.id} case_profile has prompt set version {profile.prompt_set_version!r}; expected {CASE_PROFILE_PROMPT_SET_VERSION!r}. Rebuild case index artifacts."
        )
    if profile.schema_version != CASE_PROFILE_SCHEMA_VERSION:
        raise ValidationFailure(
            f"Case {case.id} case_profile has schema version {profile.schema_version!r}; expected {CASE_PROFILE_SCHEMA_VERSION!r}. Rebuild case index artifacts."
        )
    mismatched_chunk = session.scalar(
        select(PdfChunk.id).where(
            PdfChunk.case_id == case.id,
            PdfChunk.index_config_hash != expected_hashes.current_pdf,
        )
    )
    if mismatched_chunk is not None:
        raise ValidationFailure(
            f"Case {case.id} raw PDF chunks do not match expected current-PDF index hash {expected_hashes.current_pdf}."
        )
    missing_chunk = session.scalar(
        select(PdfChunk.id).where(
            PdfChunk.case_id == case.id,
            PdfChunk.index_config_hash.is_(None),
        )
    )
    if missing_chunk is not None:
        raise ValidationFailure(
            f"Case {case.id} raw PDF chunks are missing index-config provenance."
        )


def assert_historical_index_compatibility(
    session,
    *,
    tenant_id,
    selection: PipelineSelection,
) -> None:
    from sqlalchemy import select

    from app.models.entities import HistoricalClientPackage, HistoricalQARow
    from app.models.enums import ApprovalStatus

    expected_hashes = artifact_index_hashes(selection)
    mismatched_package = session.scalar(
        select(HistoricalClientPackage.id).where(
            HistoricalClientPackage.tenant_id == tenant_id,
            HistoricalClientPackage.index_config_hash.is_not(None),
            HistoricalClientPackage.index_config_hash != expected_hashes.historical,
        )
    )
    if mismatched_package is not None:
        raise ValidationFailure(
            f"Historical corpus package {mismatched_package} does not match expected historical index hash {expected_hashes.historical}. Re-import historical artifacts explicitly."
        )
    missing_package = session.scalar(
        select(HistoricalQARow.id)
        .join(HistoricalClientPackage, HistoricalClientPackage.id == HistoricalQARow.client_package_id)
        .where(
            HistoricalQARow.tenant_id == tenant_id,
            HistoricalQARow.approval_status == ApprovalStatus.APPROVED,
            HistoricalClientPackage.index_config_hash.is_(None),
        )
    )
    if missing_package is not None:
        raise ValidationFailure("Historical corpus is missing index-config provenance.")


def assert_product_truth_index_compatibility(
    session,
    *,
    tenant_id,
    selection: PipelineSelection,
) -> None:
    from sqlalchemy import select

    from app.models.entities import ProductTruthRecord
    from app.models.enums import ApprovalStatus

    expected_hashes = artifact_index_hashes(selection)
    mismatched_record = session.scalar(
        select(ProductTruthRecord.id).where(
            ProductTruthRecord.tenant_id == tenant_id,
            ProductTruthRecord.approval_status == ApprovalStatus.APPROVED,
            ProductTruthRecord.index_config_hash.is_not(None),
            ProductTruthRecord.index_config_hash != expected_hashes.product_truth,
        )
    )
    if mismatched_record is not None:
        raise ValidationFailure(
            f"Product-truth record {mismatched_record} does not match expected product-truth index hash {expected_hashes.product_truth}. Re-import product truth explicitly."
        )
    missing_record = session.scalar(
        select(ProductTruthRecord.id).where(
            ProductTruthRecord.tenant_id == tenant_id,
            ProductTruthRecord.approval_status == ApprovalStatus.APPROVED,
            ProductTruthRecord.index_config_hash.is_(None),
        )
    )
    if missing_record is not None:
        raise ValidationFailure("Product-truth corpus is missing index-config provenance.")


def assert_pipeline_runtime_compatibility(
    session,
    *,
    case,
    selection: PipelineSelection,
) -> None:
    assert_case_index_compatibility(session, case=case, selection=selection)
    assert_historical_index_compatibility(
        session,
        tenant_id=case.tenant_id,
        selection=selection,
    )
    assert_product_truth_index_compatibility(
        session,
        tenant_id=case.tenant_id,
        selection=selection,
    )
