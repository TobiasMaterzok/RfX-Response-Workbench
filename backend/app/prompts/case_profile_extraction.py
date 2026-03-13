from __future__ import annotations

from app.prompts.case_profile import CASE_PROFILE_PROMPT_SET_VERSION

CASE_PROFILE_EXTRACTION_PROMPT_VERSION = CASE_PROFILE_PROMPT_SET_VERSION

CASE_PROFILE_EXTRACTION_SYSTEM_PROMPT = """You extract a strict case-profile analysis from one customer RfX context document.

You are not writing a summary memo.
You are filling a fixed analysis ledger.

Rules:
- Use only the text inside <document_text>.
- Do not use outside knowledge.
- Do not invent facts, integrations, governance details, rollout outcomes, security properties, vendor expectations, or organizational structures.
- For each analysis item, answer only that item.
- Keep each answer as short as possible while remaining correct. Prefer 1 to 2 sentences.
- If support_level is `explicit`, the answer should stay close to what is directly stated.
- If support_level is `strongly_implied`, state the implied conclusion narrowly.
- If support_level is `unknown`, say briefly that the point is not explicitly stated and list only material unknowns.
- Do not restate the whole document in each item.
- Confidence means extraction confidence, not business importance.
- Citations must use page references like "Page 1". If page support cannot be determined, use "citation_unavailable".
- Return exactly the analysis items listed in <analysis_dimensions>, in the exact order given.
- Return only JSON matching the schema."""

CASE_PROFILE_EXTRACTION_USER_PROMPT_TEMPLATE = """<task>
Extract a strict case-profile analysis from this RfX context document.
Return only schema-valid JSON.
</task>

<client_name>
{{client_name}}
</client_name>

<language_hint>
{{language_hint}}
</language_hint>

<analysis_dimensions>
1. strategic_objectives — What are the customer's strategic objectives, transformation drivers, and success criteria?
2. initiative_scope — What initiative scope is explicitly requested, including in-scope and out-of-scope boundaries?
3. business_capabilities_in_scope — Which business capabilities and process families are explicitly in scope?
4. geographies_entities_operating_model — What geographies, legal entities, operating model details, and rollout footprint are stated?
5. current_state_pain_points — What current-state pain points, fragmentation, and operational issues are described?
6. target_state_outcomes — What target-state outcomes and benefits are explicitly requested?
7. non_functional_requirements — Which non-functional requirements, usability expectations, language needs, and service-level expectations are stated?
8. architecture_integration_data — What architecture, integration, identity, data, and platform landscape constraints are stated?
9. security_privacy_regulatory — Which security, privacy, regulatory, auditability, or residency requirements are explicitly stated?
10. delivery_constraints_timeline — What delivery constraints, timing expectations, rollout phases, and deadlines are stated?
11. stakeholders_governance — Which stakeholders, governance structures, or organizational ownership details are explicitly stated?
12. evaluation_factors_and_risks — What evaluation factors, vendor expectations, risks, or adoption concerns are explicitly stated or strongly implied?
</analysis_dimensions>

<field_rules>
- analysis_items[].id must be one of the exact ids listed above
- analysis_items[].answer must be concise and limited to that item only
- analysis_items[].unknowns must be [] if there are no material unknowns
- summary must be one short neutral paragraph
- do not include case_id, file_name, file_hash, generated_at, or model in the model output
</field_rules>

<document_text>
{{document_text}}
</document_text>"""


def build_case_profile_extraction_request_payload(
    *,
    client_name: str,
    language_hint: str,
    document_text: str,
) -> list[dict[str, object]]:
    user_prompt = (
        CASE_PROFILE_EXTRACTION_USER_PROMPT_TEMPLATE
        .replace("{{client_name}}", client_name)
        .replace("{{language_hint}}", language_hint)
        .replace("{{document_text}}", document_text)
    )
    return [
        {
            "role": "system",
            "content": [{"type": "input_text", "text": CASE_PROFILE_EXTRACTION_SYSTEM_PROMPT}],
        },
        {
            "role": "user",
            "content": [{"type": "input_text", "text": user_prompt}],
        },
    ]
