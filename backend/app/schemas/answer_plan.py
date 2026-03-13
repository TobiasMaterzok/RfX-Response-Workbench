from __future__ import annotations

import re
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

AnswerIntent = Literal[
    "product_fit",
    "rollout_approach",
    "workflow_capability",
    "integration",
    "security_compliance",
    "service_delivery",
    "reporting_analytics",
    "unknown_or_out_of_scope",
]
TargetLanguage = Literal["de", "en"]
OutputMode = Literal["customer_facing", "grounded_review"]
ClaimKind = Literal[
    "scope_fit",
    "product_mapping",
    "capability",
    "rollout_pattern",
    "integration",
    "security",
    "service_delivery",
    "reporting",
    "limitation",
]
AuthorityLayer = Literal["current_case_facts", "product_truth", "historical_exemplar"]
UnknownMateriality = Literal["material", "minor"]
PreferredFormat = Literal["paragraph", "bullets", "paragraph_plus_bullets"]
EvidenceLayer = Literal[
    "current_case_facts",
    "raw_current_pdf",
    "product_truth",
    "historical_exemplar",
]
UsagePolicy = Literal[
    "factual_support",
    "scope_context",
    "style_only",
    "pattern_only",
    "factual_allowed",
]
_INTERNAL_TOPIC_PATTERN = re.compile(r"^[a-z]+(?:_[a-z0-9]+)+$")


class NormalizedEvidenceItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    layer: EvidenceLayer
    title: str
    text: str
    usage_policy: UsagePolicy
    source_kind: str
    product_name: str | None = None
    citations: list[str] | None = None


class SupportedClaim(BaseModel):
    model_config = ConfigDict(extra="forbid")

    claim: str
    claim_kind: ClaimKind
    authority_layer: AuthorityLayer
    support_ids: list[str] = Field(min_length=1)


class UnknownItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    topic: str
    reason: str
    materiality: UnknownMateriality

    @model_validator(mode="after")
    def validate_topic_is_customer_visible(self) -> UnknownItem:
        if _INTERNAL_TOPIC_PATTERN.fullmatch(self.topic.strip()):
            raise ValueError(
                "unknowns[].topic must be customer-visible plain language, not an internal snake_case label."
            )
        return self


class AnswerShape(BaseModel):
    model_config = ConfigDict(extra="forbid")

    preferred_format: PreferredFormat
    target_word_min: int = Field(ge=1)
    target_word_max: int = Field(ge=1)

    @model_validator(mode="after")
    def validate_bounds(self) -> AnswerShape:
        if self.target_word_min > self.target_word_max:
            raise ValueError("target_word_min cannot exceed target_word_max.")
        return self


class AnswerPlan(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["rfx_answer_plan.v2"] = "rfx_answer_plan.v2"
    primary_intent: AnswerIntent
    secondary_intents: list[AnswerIntent] = Field(default_factory=list)
    target_language: TargetLanguage
    output_mode: OutputMode
    direct_answer_thesis: str
    supported_claims: list[SupportedClaim]
    historical_style_guidance: list[str]
    unknowns: list[UnknownItem]
    forbidden_claims: list[str]
    answer_shape: AnswerShape

    @model_validator(mode="after")
    def validate_intents_and_guidance(self) -> AnswerPlan:
        secondary = list(self.secondary_intents)
        if self.primary_intent in secondary:
            raise ValueError("primary_intent must not also appear in secondary_intents.")
        if len(set(secondary)) != len(secondary):
            raise ValueError("secondary_intents must not contain duplicates.")
        factual_markers = (
            "successful rollout",
            "successfully rolled out",
            "implemented at",
            "deployed at",
            "industry",
            "insurance",
            "logistics",
        )
        for entry in self.historical_style_guidance:
            lowered = entry.lower()
            if any(marker in lowered for marker in factual_markers):
                raise ValueError(
                    "historical_style_guidance must stay at pattern/style level and must not look like a factual module/outcome claim."
                )
        return self
