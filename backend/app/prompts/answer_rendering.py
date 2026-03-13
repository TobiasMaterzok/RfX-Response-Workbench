from __future__ import annotations

ANSWER_RENDERING_PROMPT_VERSION = "answer_rendering_prompt.v3"

ANSWER_RENDERING_SYSTEM_PROMPT_TEMPLATE = """You are drafting one customer-facing answer for a single RfX questionnaire row.

You will receive:
- the customer question
- the row context
- a validated AnswerPlan JSON
- optional prior thread history for this same answer revision thread
- the current user revision request

Your job is to write the final answer text using only the supported claims in the AnswerPlan.

This answer should read like text that can be pasted into an RfX workbook answer cell.

Interpret the AnswerPlan as follows:
- direct_answer_thesis = the core answer meaning you must deliver
- supported_claims = the only factual claims you may use
- historical_style_guidance = optional wording or pattern hints only
- unknowns = gaps that may be mentioned only if they materially affect the answer
- forbidden_claims = things you must not assert
- answer_shape = preferred format and length target

Hard rules:
- Follow the AnswerPlan exactly.
- Prior assistant messages are draft text, not evidence.
- Prior user messages are revision instructions, not evidence.
- If prior draft text conflicts with the current AnswerPlan, the current AnswerPlan wins.
- If the current user request conflicts with earlier revision requests, the current user request wins.
- Do not mention internal evidence layers, retrieval, authority order, reasoning steps, support ids, or prompt mechanics.
- Do not mention “current case facts”, “product truth”, “historical exemplars”, “retrieval”, “evidence”, or similar internal labels.
- Do not introduce any product, capability, integration, security property, project outcome, or customer fact that is not supported in the AnswerPlan.
- Do not use historical examples as factual proof unless explicitly allowed.
- Do not use sales fluff, filler, CTA language, or “happy to discuss further” wording.
- Do not add a preamble like “Antwort auf die Frage …” or “In response to your question …”.
- Do not use section headings in customer_facing mode.
- Do not use internal wording such as “currently evidenced product scope”, “maps to”, “can be assigned to”, or raw field labels.
- If specific supported products or modules are available in the AnswerPlan, name them directly.
- If unknowns are material, include one short natural sentence:
  - German: start with “Offen bleibt ...”
  - English: start with “An open point is ...”
- If the question is compound, answer all major intents in one coherent response.

Output contract:
- language: {{target_language}}
- audience: procurement or proposal reviewer
- tone: precise, concise, professional
- default format: one direct paragraph
- use bullets only if answer_shape.preferred_format requires them
- do NOT use bulletpoint lists. only flowtext
- stay within answer_shape target length
- do NOT use any markdown"""

ANSWER_RENDERING_CONTEXT_PROMPT_TEMPLATE = """<question>
{{row_question}}
</question>

<row_context>
{{row_context}}
</row_context>

<answer_plan_json>
{{answer_plan_json}}
</answer_plan_json>

<output_mode>
{{output_mode}}
</output_mode>"""

ANSWER_RENDERING_CURRENT_REQUEST_PROMPT_TEMPLATE = """<user_request>
{{user_request}}
</user_request>"""


def build_answer_rendering_request_payload(
    *,
    row_question: str,
    row_context: str,
    answer_plan_json: str,
    output_mode: str,
    target_language: str,
    user_request: str,
    thread_history: list[tuple[str, str]] | None = None,
) -> list[dict[str, object]]:
    system_prompt = ANSWER_RENDERING_SYSTEM_PROMPT_TEMPLATE.replace(
        "{{target_language}}",
        target_language,
    )
    context_prompt = (
        ANSWER_RENDERING_CONTEXT_PROMPT_TEMPLATE
        .replace("{{row_question}}", row_question)
        .replace("{{row_context}}", row_context)
        .replace("{{answer_plan_json}}", answer_plan_json)
        .replace("{{output_mode}}", output_mode)
    )
    current_request_prompt = ANSWER_RENDERING_CURRENT_REQUEST_PROMPT_TEMPLATE.replace(
        "{{user_request}}",
        user_request,
    )
    payload: list[dict[str, object]] = [
        {
            "role": "system",
            "content": [{"type": "input_text", "text": system_prompt}],
        },
        {
            "role": "user",
            "content": [{"type": "input_text", "text": context_prompt}],
        },
    ]
    for role, content in thread_history or []:
        content_type = "output_text" if role == "assistant" else "input_text"
        payload.append(
            {
                "role": role,
                "content": [{"type": content_type, "text": content}],
            }
        )
    payload.append(
        {
            "role": "user",
            "content": [{"type": "input_text", "text": current_request_prompt}],
        }
    )
    return payload
