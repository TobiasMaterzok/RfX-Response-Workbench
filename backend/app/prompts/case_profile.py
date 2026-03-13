from __future__ import annotations

from dataclasses import dataclass

CASE_PROFILE_SCHEMA_VERSION = "rfx_case_profile.v3"
CASE_PROFILE_PROMPT_SET_VERSION = "rfx_case_profile_prompt_set.v3"


@dataclass(frozen=True)
class AnalysisPrompt:
    id: str
    prompt: str


ANALYSIS_PROMPTS: tuple[AnalysisPrompt, ...] = (
    AnalysisPrompt(
        id="strategic_objectives",
        prompt="What are the customer's strategic objectives, transformation drivers, and success criteria?",
    ),
    AnalysisPrompt(
        id="initiative_scope",
        prompt="What initiative scope is explicitly requested, including in-scope and out-of-scope boundaries?",
    ),
    AnalysisPrompt(
        id="business_capabilities_in_scope",
        prompt="Which business capabilities and process families are explicitly in scope?",
    ),
    AnalysisPrompt(
        id="geographies_entities_operating_model",
        prompt="What geographies, legal entities, operating model details, and rollout footprint are stated?",
    ),
    AnalysisPrompt(
        id="current_state_pain_points",
        prompt="What current-state pain points, fragmentation, and operational issues are described?",
    ),
    AnalysisPrompt(
        id="target_state_outcomes",
        prompt="What target-state outcomes and benefits are explicitly requested?",
    ),
    AnalysisPrompt(
        id="non_functional_requirements",
        prompt="Which non-functional requirements, usability expectations, language needs, and service-level expectations are stated?",
    ),
    AnalysisPrompt(
        id="architecture_integration_data",
        prompt="What architecture, integration, identity, data, and platform landscape constraints are stated?",
    ),
    AnalysisPrompt(
        id="security_privacy_regulatory",
        prompt="Which security, privacy, regulatory, auditability, or residency requirements are explicitly stated?",
    ),
    AnalysisPrompt(
        id="delivery_constraints_timeline",
        prompt="What delivery constraints, timing expectations, rollout phases, and deadlines are stated?",
    ),
    AnalysisPrompt(
        id="stakeholders_governance",
        prompt="Which stakeholders, governance structures, or organizational ownership details are explicitly stated?",
    ),
    AnalysisPrompt(
        id="evaluation_factors_and_risks",
        prompt="What evaluation factors, vendor expectations, risks, or adoption concerns are explicitly stated or strongly implied?",
    ),
)

ANALYSIS_PROMPT_IDS = tuple(prompt.id for prompt in ANALYSIS_PROMPTS)
