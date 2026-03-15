from __future__ import annotations

import re
from collections.abc import Sequence

from pydantic import ValidationError as PydanticValidationError

from app.exceptions import ValidationFailure
from app.schemas.answer_plan import (
    AnswerIntent,
    AnswerPlan,
    AnswerShape,
    NormalizedEvidenceItem,
    PreferredFormat,
    SupportedClaim,
)
from app.services.reproducibility import canonical_json_text

_BANNED_HEADINGS = (
    "Current case facts",
    "Product truth",
    "Historical exemplars",
    "Aktuelle Fallfakten",
    "Produktwahrheit",
    "Historische Beispiele",
)
_INTERNAL_LEAKAGE_TERMS = (
    "retrieval",
    "support id",
    "support_ids",
    "question_type",
    "primary_intent",
    "secondary_intents",
    "current_case_facts",
    "product_truth",
    "historical_exemplar",
)
_BANNED_INTERNAL_STYLE_PHRASES = (
    "currently evidenced",
    "belegten produktumfang",
    "lässt sich",
)
_CTA_FILLER_TERMS = (
    "please let us know",
    "we would be happy to",
    "happy to discuss further",
    "wir unterstützen sie gerne",
    "gerne stellen wir",
)
_NUMBERED_SECTION_PATTERN = re.compile(r"(?m)^\s*\d+[.)]\s+")
_SNAKE_CASE_PATTERN = re.compile(r"\b[a-z]+(?:_[a-z0-9]+)+\b")
_PRODUCT_NAME_STRIP_SUFFIXES = (
    " feature note",
    " capability note",
    " module note",
)
_PRODUCT_NAME_EXCLUDED_SUFFIXES = (
    " deployment note",
)


def normalize_target_language(language: str) -> str:
    return "de" if str(language).lower().startswith("de") else "en"


def classify_question_intents(
    *,
    row_question: str,
    row_context: str,
    user_request: str,
) -> tuple[AnswerIntent, list[AnswerIntent]]:
    question_text = row_question.lower()
    detected: list[AnswerIntent] = []
    if any(
        token in question_text
        for token in ("produkt", "products", "product", "module", "modul", "solution", "lösung", "suite", "plattform")
    ):
        detected.append("product_fit")
    if any(
        token in question_text
        for token in ("schrittweise", "rollout", "einführung", "phased", "phase", "depot", "wave")
    ):
        detected.append("rollout_approach")
    if any(token in question_text for token in ("integration", "integrations", "schnittstelle", "api", "interface", "sso", "azure ad", "erp", "identity")):
        detected.append("integration")
    if any(
        token in question_text
        for token in ("security", "sicherheit", "datenschutz", "privacy", "gdpr", "compliance", "audit", "eu hosting", "eu-hosting")
    ):
        detected.append("security_compliance")
    if any(
        token in question_text
        for token in ("reporting", "analytics", "bericht", "berichte", "dashboard", "kennzahl")
    ):
        detected.append("reporting_analytics")
    if any(token in question_text for token in ("service delivery", "managed service", "betriebskonzept", "sla", "liefermodell", "supportmodell")):
        detected.append("service_delivery")
    if any(
        token in question_text
        for token in ("workflow", "capability", "funktion", "funktional", "ablauf", "exception handling", "ausnahme")
    ):
        detected.append("workflow_capability")
    unique: list[AnswerIntent] = []
    for intent in detected:
        if intent not in unique:
            unique.append(intent)
    if not unique:
        return "unknown_or_out_of_scope", []
    return unique[0], unique[1:]


def expected_compound_intents(
    *,
    row_question: str,
    row_context: str,
    user_request: str,
) -> set[AnswerIntent]:
    primary, secondary = classify_question_intents(
        row_question=row_question,
        row_context=row_context,
        user_request=user_request,
    )
    expected = {primary, *secondary}
    expected.discard("unknown_or_out_of_scope")
    return expected


def default_answer_shape(
    *,
    primary_intent: AnswerIntent,
    secondary_intents: Sequence[AnswerIntent],
    target_word_min: int | None,
    target_word_max: int | None,
) -> AnswerShape:
    preferred_format: PreferredFormat = "paragraph"
    if len(secondary_intents) >= 1 or primary_intent in {
        "rollout_approach",
        "workflow_capability",
    }:
        preferred_format = "paragraph_plus_bullets"
    minimum = target_word_min or 50
    maximum = target_word_max or 140
    return AnswerShape(
        preferred_format=preferred_format,
        target_word_min=minimum,
        target_word_max=max(maximum, minimum),
    )


def normalize_evidence_text(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def normalize_evidence_pack(
    evidence_items: Sequence[NormalizedEvidenceItem],
) -> tuple[list[NormalizedEvidenceItem], str]:
    seen: set[tuple[str, str, str]] = set()
    normalized: list[NormalizedEvidenceItem] = []
    for item in evidence_items:
        text = normalize_evidence_text(item.text)
        key = (item.layer, item.title.strip(), text)
        if key in seen:
            continue
        seen.add(key)
        normalized.append(
            item.model_copy(
                update={
                    "title": item.title.strip(),
                    "text": text,
                }
            )
        )
    return normalized, canonical_json_text([item.model_dump(mode="json") for item in normalized])


def validate_answer_plan(
    plan: AnswerPlan,
    *,
    normalized_evidence: Sequence[NormalizedEvidenceItem],
    row_question: str,
    row_context: str,
    user_request: str,
) -> AnswerPlan:
    try:
        plan = AnswerPlan.model_validate(plan.model_dump(mode="python"))
    except PydanticValidationError as exc:
        raise ValidationFailure(f"AnswerPlan failed schema validation: {exc}") from exc
    evidence_by_id = {item.id: item for item in normalized_evidence}
    if not plan.direct_answer_thesis.strip():
        raise ValidationFailure("AnswerPlan direct_answer_thesis must be non-empty.")
    if (
        plan.output_mode == "customer_facing"
        and plan.answer_shape.preferred_format == "paragraph"
        and plan.answer_shape.target_word_max > 220
    ):
        raise ValidationFailure("Customer-facing paragraph answers must stay concise.")
    expected_intents = expected_compound_intents(
        row_question=row_question,
        row_context=row_context,
        user_request=user_request,
    )
    observed_intents = {plan.primary_intent, *plan.secondary_intents}
    if expected_intents and not expected_intents <= observed_intents:
        raise ValidationFailure(
            f"AnswerPlan dropped major compound-question intents. Expected at least {sorted(expected_intents)}, observed {sorted(observed_intents)}."
        )
    for claim in plan.supported_claims:
        if not claim.claim.strip():
            raise ValidationFailure("AnswerPlan supported_claims[].claim must be non-empty.")
        support_items: list[NormalizedEvidenceItem] = []
        for support_id in claim.support_ids:
            item = evidence_by_id.get(support_id)
            if item is None:
                raise ValidationFailure(
                    f"AnswerPlan references unknown support id {support_id!r}."
                )
            support_items.append(item)
        _validate_claim_authority(claim=claim, support_items=support_items)
    for guidance in plan.historical_style_guidance:
        lowered = guidance.lower()
        supported_products = {
            product_name.lower() for product_name in supported_product_names(normalized_evidence)
        }
        if any(
            phrase in lowered
            for phrase in (
                "successful rollout",
                "implemented at",
                "deployed at",
                "insurance",
                "logistics",
            )
        ) or any(product_name in lowered for product_name in supported_products):
            raise ValidationFailure(
                "historical_style_guidance must stay at pattern/style level and must not contain factual module/outcome language."
            )
    for unknown in plan.unknowns:
        if _SNAKE_CASE_PATTERN.search(unknown.topic):
            raise ValidationFailure(
                f"AnswerPlan unknown topic {unknown.topic!r} uses internal snake_case wording."
            )
    return plan


def _validate_claim_authority(
    *,
    claim: SupportedClaim,
    support_items: Sequence[NormalizedEvidenceItem],
) -> None:
    layers = {item.layer for item in support_items}
    if claim.authority_layer == "current_case_facts":
        allowed_layers = {"current_case_facts", "raw_current_pdf"}
    elif claim.authority_layer == "product_truth":
        allowed_layers = {"product_truth"}
    else:
        allowed_layers = {"historical_exemplar"}
    if not layers <= allowed_layers:
        raise ValidationFailure(
            f"AnswerPlan claim {claim.claim!r} has authority layer {claim.authority_layer!r} but references incompatible evidence layers {sorted(layers)}."
        )
    if (
        claim.claim_kind in {"product_mapping", "integration", "security", "reporting"}
        and claim.authority_layer != "product_truth"
    ):
        raise ValidationFailure(
            f"Claim kind {claim.claim_kind!r} must be supported by product_truth."
        )
    if (
        claim.claim_kind == "rollout_pattern"
        and claim.authority_layer == "historical_exemplar"
        and not all(item.usage_policy in {"pattern_only", "factual_allowed"} for item in support_items)
    ):
        raise ValidationFailure(
            f"rollout_pattern claim {claim.claim!r} references historical evidence that is not pattern-capable."
        )
    if (
        claim.authority_layer == "historical_exemplar"
        and claim.claim_kind != "rollout_pattern"
        and not all(item.usage_policy == "factual_allowed" for item in support_items)
    ):
        raise ValidationFailure(
            f"Historical exemplar claim {claim.claim!r} is not explicitly marked factual_allowed."
        )


def validate_rendered_answer(
    *,
    answer_text: str,
    plan: AnswerPlan,
) -> dict[str, object]:
    diagnostics: dict[str, object] = {
        "valid": True,
        "checks": [],
    }
    text = answer_text.strip()
    if not text:
        raise ValidationFailure("Rendered answer is empty.")
    lowered = text.lower()
    for heading in _BANNED_HEADINGS:
        if heading.lower() in lowered:
            raise ValidationFailure(f"Rendered answer leaked internal heading {heading!r}.")
    for term in _INTERNAL_LEAKAGE_TERMS:
        if term in lowered:
            raise ValidationFailure(
                f"Rendered answer leaked internal prompt scaffolding term {term!r}."
            )
    for phrase in _BANNED_INTERNAL_STYLE_PHRASES:
        if phrase in lowered:
            raise ValidationFailure(
                f"Rendered answer uses internal-style phrasing {phrase!r}."
            )
    if _SNAKE_CASE_PATTERN.search(text):
        raise ValidationFailure("Rendered answer leaked raw internal snake_case wording.")
    for forbidden in plan.forbidden_claims:
        normalized = forbidden.strip().lower()
        if normalized and normalized in lowered:
            raise ValidationFailure(
                f"Rendered answer includes forbidden claim fragment {forbidden!r}."
            )
    if plan.output_mode == "customer_facing":
        if plan.answer_shape.preferred_format == "paragraph" and _NUMBERED_SECTION_PATTERN.search(text):
            raise ValidationFailure(
                "Customer-facing paragraph answer must not use numbered debug sections."
            )
        for filler in _CTA_FILLER_TERMS:
            if filler in lowered:
                raise ValidationFailure(
                    f"Rendered answer includes CTA filler {filler!r}."
                )
    diagnostics["checks"] = [
        "non_empty",
        "internal_headings_absent",
        "internal_leakage_absent",
        "internal_style_phrasing_absent",
        "forbidden_claims_absent",
    ]
    return diagnostics


def supported_product_names(
    evidence_items: Sequence[NormalizedEvidenceItem],
) -> set[str]:
    names: set[str] = set()
    for item in evidence_items:
        if item.layer != "product_truth":
            continue
        canonical_name = canonical_product_name(item.product_name)
        if canonical_name is not None:
            names.add(canonical_name)
    return names


def canonical_product_name(value: str | None) -> str | None:
    if value is None:
        return None
    text = re.sub(r"\s+", " ", value).strip()
    lowered = text.lower()
    if any(lowered.endswith(suffix) for suffix in _PRODUCT_NAME_EXCLUDED_SUFFIXES):
        return None
    for suffix in _PRODUCT_NAME_STRIP_SUFFIXES:
        if lowered.endswith(suffix):
            return text[: -len(suffix)].strip()
    return text


def product_truth_supports_feature(
    items: Sequence[NormalizedEvidenceItem],
    *,
    keywords: Sequence[str],
) -> bool:
    negative_markers = (
        "does not",
        "not confirm",
        "not explicitly confirm",
        "not supported",
        "does not state",
        "nicht bestätigt",
        "nicht belegt",
        "nicht unterstützt",
        "does not support",
    )
    for item in items:
        if item.layer != "product_truth":
            continue
        lower = item.text.lower()
        if not any(keyword in lower for keyword in keywords):
            continue
        if any(marker in lower for marker in negative_markers):
            continue
        return True
    return False


def row_context_claim_text(item: NormalizedEvidenceItem) -> str:
    sentence = item.text.split(".")[0].strip().rstrip(".")
    return sentence if sentence else item.text
