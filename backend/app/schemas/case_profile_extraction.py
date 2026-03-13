from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from app.prompts.case_profile import ANALYSIS_PROMPT_IDS

CaseProfileSupportLevel = Literal["explicit", "strongly_implied", "unknown"]
CaseProfileConfidence = Literal["high", "medium", "low"]
CaseProfileExtractionItemId = Literal[
    "strategic_objectives",
    "initiative_scope",
    "business_capabilities_in_scope",
    "geographies_entities_operating_model",
    "current_state_pain_points",
    "target_state_outcomes",
    "non_functional_requirements",
    "architecture_integration_data",
    "security_privacy_regulatory",
    "delivery_constraints_timeline",
    "stakeholders_governance",
    "evaluation_factors_and_risks",
]


class CaseProfileExtractionItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: CaseProfileExtractionItemId
    answer: str
    support_level: CaseProfileSupportLevel
    confidence: CaseProfileConfidence
    citations: list[str] = Field(min_length=1)
    unknowns: list[str]


class CaseProfileExtractionOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["rfx_case_profile_extraction.v2"] = (
        "rfx_case_profile_extraction.v2"
    )
    analysis_items: list[CaseProfileExtractionItem] = Field(min_length=12, max_length=12)
    summary: str

    @model_validator(mode="after")
    def validate_analysis_items(self) -> CaseProfileExtractionOutput:
        observed_ids = [item.id for item in self.analysis_items]
        if observed_ids != list(ANALYSIS_PROMPT_IDS):
            raise ValueError(
                "analysis_items must match the fixed analysis dimension order exactly: "
                + ", ".join(ANALYSIS_PROMPT_IDS)
            )
        return self
