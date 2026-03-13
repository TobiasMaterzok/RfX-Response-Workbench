from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field, model_validator

from app.prompts.case_profile import ANALYSIS_PROMPT_IDS, CASE_PROFILE_SCHEMA_VERSION

Confidence = Literal["high", "medium", "low"]
SupportLevel = Literal["explicit", "strongly_implied", "unknown"]


class CaseProfileSourcePdf(BaseModel):
    file_name: str
    file_hash: str


class CaseProfileAnalysisItem(BaseModel):
    id: str
    prompt: str
    answer: str
    support_level: SupportLevel
    confidence: Confidence
    citations: list[str] = Field(min_length=1)
    unknowns: list[str] = Field(default_factory=list)


class CaseProfileDocument(BaseModel):
    schema_version: str = CASE_PROFILE_SCHEMA_VERSION
    case_id: UUID
    source_pdf: CaseProfileSourcePdf
    language: Literal["de", "en", "unknown"]
    client_name: str
    analysis_items: list[CaseProfileAnalysisItem]
    summary: str
    generated_at: datetime
    model: str

    @model_validator(mode="after")
    def validate_analysis_items(self) -> CaseProfileDocument:
        observed_ids = [item.id for item in self.analysis_items]
        if observed_ids != list(ANALYSIS_PROMPT_IDS):
            raise ValueError(
                "analysis_items must match the fixed prompt set order exactly: "
                + ", ".join(ANALYSIS_PROMPT_IDS)
            )
        return self
