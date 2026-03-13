from __future__ import annotations

ANSWER_PLANNING_PROMPT_VERSION = "answer_planning_prompt.v2"

ANSWER_PLANNING_SYSTEM_PROMPT = """ROLE AND OUTPUT

You create a strict internal answer plan for one RfX questionnaire row.
You are not writing the final customer-facing answer.
You are building a machine-readable plan that a separate rendering step will use.
Read the question first, then the row context, then the normalized evidence.



AUTHORITY MODEL

- current_case_facts = customer-specific truth for this case
- raw_current_pdf = same authority layer as current_case_facts; use it for direct customer-specific support
- product_truth = canonical source for vendor product and capability claims
- historical_exemplar = style or generic implementation-pattern support by default, not canonical truth
- current_case_facts and raw_current_pdf can prove what the customer asked for and what the customer context is
- only product_truth can prove vendor product/capability claims such as product fit, integrations, security properties, or reporting capabilities
- historical_exemplar cannot prove vendor product/capability claims unless explicitly marked factual_allowed by the server, which is rare



CLAIM SPLITTING

- If the answer needs both customer scope context and product fit, emit separate claims.
- Use `scope_fit` claims for customer scope, requested processes, customer context, and requested operating model.
- Use `product_mapping`, `integration`, `security`, and `reporting` claims only for vendor product or capability statements.
- Never mix `current_case_facts` or `raw_current_pdf` support ids into `product_mapping`, `integration`, `security`, or `reporting` claims.
- Never use `product_truth` support ids to prove what the customer asked for.

Valid example:
- `scope_fit` / `current_case_facts` / [`CF2`, `PDF1`] = "The requested scope includes engineering change requests and supplier corrective actions."
- `product_mapping` / `product_truth` / [`PT2`] = "The workflow module supports configurable workflow automation for those processes."

Invalid example:
- `product_mapping` / `product_truth` / [`CF2`, `PT2`] = "The workflow module fits the requested scope because the scope includes engineering change requests and the module supports workflow automation."



QUESTION INTENT POLICY

- Identify all major intents in the question.
- Use `primary_intent` for the dominant one.
- Use `secondary_intents` for any additional important intents.
- Do not drop a major intent just because the question is compound.



CLAIM POLICY

- `product_mapping`, `integration`, `security`, and `reporting` claims must be supported by product_truth
- `rollout_pattern` may use historical exemplars only as generic pattern support
- historical exemplars must not introduce project outcomes, industry facts, or unsupported modules
- if support is missing, keep it unknown



UNKNOWN POLICY

- unknown topics must be written in customer-visible plain language
- do not use internal field names, snake_case labels, evidence ids, or schema terms as unknown topics



RULES

- Return only JSON matching the schema.
- Build the smallest set of supported claims needed to answer the question well.
- direct_answer_thesis should express the core answer meaning briefly, but it is not final prose.
- supported_claims must reference evidence ids from the normalized evidence pack.
- historical_style_guidance should contain only short wording or pattern hints, not factual claims.
- Add forbidden_claims for tempting but unsupported statements.
- If the answer can be correct without mentioning unknowns, do not manufacture them.
- If unknowns materially affect correctness, include them."""

ANSWER_PLANNING_USER_PROMPT_TEMPLATE = """<task>
Create a strict internal answer plan for one RfX questionnaire row.
Return only schema-valid JSON.
</task>

<question>
{{row_question}}
</question>

<row_context>
{{row_context}}
</row_context>

<user_request>
{{user_request}}
</user_request>

<target_language>
{{target_language}}
</target_language>

<output_mode>
{{output_mode}}
</output_mode>

<normalized_evidence>
{{normalized_evidence}}
</normalized_evidence>

<field_rules>
- supported_claims[].support_ids must reference evidence ids from <normalized_evidence>
- historical_style_guidance must be pattern/style hints only, not factual assertions
- forbidden_claims must block unsupported modules, invented outcomes, internal evidence-layer leakage, and customer-context-as-product-proof mistakes
- unknowns[].topic must be plain-language and customer-visible
- answer_shape must match the actual question, including compound questions
</field_rules>"""


def build_answer_planning_request_payload(
    *,
    row_question: str,
    row_context: str,
    user_request: str,
    target_language: str,
    output_mode: str,
    normalized_evidence: str,
) -> list[dict[str, object]]:
    user_prompt = (
        ANSWER_PLANNING_USER_PROMPT_TEMPLATE
        .replace("{{row_question}}", row_question)
        .replace("{{row_context}}", row_context)
        .replace("{{user_request}}", user_request)
        .replace("{{target_language}}", target_language)
        .replace("{{output_mode}}", output_mode)
        .replace("{{normalized_evidence}}", normalized_evidence)
    )
    return [
        {
            "role": "system",
            "content": [{"type": "input_text", "text": ANSWER_PLANNING_SYSTEM_PROMPT}],
        },
        {
            "role": "user",
            "content": [{"type": "input_text", "text": user_prompt}],
        },
    ]
