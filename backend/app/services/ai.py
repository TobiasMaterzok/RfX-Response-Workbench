from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from importlib import metadata as importlib_metadata
from typing import Any, Literal, Protocol, cast
from urllib.parse import urlparse
from uuid import UUID

from openai import OpenAI

from app.config import Settings, get_settings
from app.exceptions import ConfigurationFailure, ValidationFailure
from app.pipeline.config import PipelineSelection
from app.prompts.answer_planning import build_answer_planning_request_payload
from app.prompts.answer_rendering import build_answer_rendering_request_payload
from app.prompts.case_profile import ANALYSIS_PROMPTS
from app.prompts.case_profile_extraction import build_case_profile_extraction_request_payload
from app.schemas.answer_plan import (
    AnswerIntent,
    AnswerPlan,
    NormalizedEvidenceItem,
    SupportedClaim,
    UnknownItem,
)
from app.schemas.case_profile import (
    CaseProfileAnalysisItem,
    CaseProfileDocument,
    CaseProfileSourcePdf,
)
from app.schemas.case_profile_extraction import (
    CaseProfileExtractionItem,
    CaseProfileExtractionItemId,
    CaseProfileExtractionOutput,
)
from app.services.answer_prompting import (
    canonical_product_name,
    classify_question_intents,
    default_answer_shape,
    product_truth_supports_feature,
    row_context_claim_text,
    supported_product_names,
)
from app.services.reproducibility import canonical_json_text


@dataclass(frozen=True)
class AnswerPlanGenerationResult:
    answer_plan: AnswerPlan
    request_payload: list[dict[str, object]]
    response_payload: dict[str, object]
    provider_response_id: str | None
    requested_model_id: str | None
    actual_model_id: str | None
    service_tier: str | None
    usage_json: dict[str, object] | None


@dataclass(frozen=True)
class AnswerRenderGenerationResult:
    request_payload: list[dict[str, object]]
    request_text: str
    response_text: str
    provider_response_id: str | None
    requested_model_id: str | None
    actual_model_id: str | None
    service_tier: str | None
    usage_json: dict[str, object] | None


@dataclass(frozen=True)
class CaseProfileGenerationResult:
    document: CaseProfileDocument
    structured_output: CaseProfileExtractionOutput
    request_payload: list[dict[str, object]]
    response_payload: dict[str, object]
    provider_response_id: str | None
    requested_model_id: str | None
    actual_model_id: str | None
    service_tier: str | None
    usage_json: dict[str, object] | None

    def __getattr__(self, name: str) -> object:
        return getattr(self.document, name)

    def model_dump(self, *, mode: Literal["json", "python"] = "python") -> dict[str, object]:
        return cast(dict[str, object], self.document.model_dump(mode=mode))


def openai_sdk_version() -> str:
    return importlib_metadata.version("openai")


def llm_provider_name_from_settings(settings: Settings) -> str:
    base_url = settings.llm_api_base_url
    if not base_url:
        return "openai"
    parsed = urlparse(base_url)
    host = (parsed.netloc or parsed.path).lower()
    if host.endswith(".openai.azure.com") or host.endswith(".services.ai.azure.com"):
        return "azure_openai"
    if host in {"api.openai.com", "api.openai.com:443"}:
        return "openai"
    return "openai_compatible"


def llm_provider_name(ai_service: AIService) -> str:
    if isinstance(ai_service, OpenAIAIService):
        return ai_service.provider_name
    return "stub"


def embedding_model_name(ai_service: AIService) -> str:
    if isinstance(ai_service, OpenAIAIService):
        return ai_service._settings.openai_embedding_model
    return "stub-ai-service"


def _usage_json(response: Any) -> dict[str, object] | None:
    usage = getattr(response, "usage", None)
    if usage is not None and hasattr(usage, "model_dump"):
        return cast(dict[str, object], usage.model_dump(mode="json"))
    return None


def _document_text_from_pages(page_text: list[str]) -> str:
    return "\n\n".join(
        f"Page {index}:\n{text}" for index, text in enumerate(page_text, start=1)
    )


def _build_case_profile_document(
    *,
    extraction_output: CaseProfileExtractionOutput,
    case_id: UUID,
    source_file_name: str,
    source_file_hash: str,
    client_name: str,
    language: str,
    model_name: str,
) -> CaseProfileDocument:
    prompt_by_id = {prompt.id: prompt.prompt for prompt in ANALYSIS_PROMPTS}
    normalized_language = (
        language if language in {"de", "en", "unknown"} else "unknown"
    )
    return CaseProfileDocument(
        case_id=case_id,
        source_pdf=CaseProfileSourcePdf(
            file_name=source_file_name,
            file_hash=source_file_hash,
        ),
        language=cast(Literal["de", "en", "unknown"], normalized_language),
        client_name=client_name,
        analysis_items=[
            CaseProfileAnalysisItem(
                id=item.id,
                prompt=prompt_by_id[item.id],
                answer=item.answer,
                support_level=item.support_level,
                confidence=item.confidence,
                citations=item.citations,
                unknowns=item.unknowns,
            )
            for item in extraction_output.analysis_items
        ],
        summary=extraction_output.summary,
        generated_at=datetime.now(UTC),
        model=model_name,
    )


def _extract_historical_product_mentions(items: list[NormalizedEvidenceItem]) -> list[str]:
    claim_pattern = re.compile(
        r"\b([A-Z][A-Za-z0-9]+(?:\s+[A-Z][A-Za-z0-9]+){0,3})\b(?=\s+(?:supports|provides|covers|enables|understützt|bietet|ermöglicht|deckt))"
    )
    mentions: set[str] = set()
    for item in items:
        if item.layer != "historical_exemplar":
            continue
        for match in claim_pattern.findall(item.text):
            normalized = canonical_product_name(match)
            if normalized is not None:
                mentions.add(normalized)
    return sorted(mentions)


class AIService(Protocol):
    def embed_text(self, text: str, *, model_id: str | None = None) -> list[float]:
        raise NotImplementedError

    def generate_case_profile(
        self,
        *,
        case_id: UUID,
        source_file_name: str,
        source_file_hash: str,
        client_name: str,
        language: str,
        page_text: list[str],
        model_id: str | None = None,
        reasoning_effort: Literal["low", "medium", "high"] | None = None,
    ) -> CaseProfileGenerationResult:
        raise NotImplementedError

    def plan_answer(
        self,
        *,
        row_question: str,
        row_context: str,
        user_request: str,
        target_language: Literal["de", "en"],
        output_mode: Literal["customer_facing", "grounded_review"],
        normalized_evidence: list[NormalizedEvidenceItem],
        pipeline: PipelineSelection,
    ) -> AnswerPlanGenerationResult:
        raise NotImplementedError

    def render_answer(
        self,
        *,
        row_question: str,
        row_context: str,
        user_request: str,
        thread_history: list[tuple[str, str]] | None,
        answer_plan: AnswerPlan,
        output_mode: Literal["customer_facing", "grounded_review"],
        target_language: Literal["de", "en"],
        pipeline: PipelineSelection,
    ) -> AnswerRenderGenerationResult:
        raise NotImplementedError


class OpenAIAIService:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client: OpenAI | None = None
        if settings.llm_api_key:
            client_kwargs: dict[str, object] = {"api_key": settings.llm_api_key}
            if settings.llm_api_base_url:
                client_kwargs["base_url"] = settings.llm_api_base_url
            self._client = OpenAI(**client_kwargs)

    @property
    def provider_name(self) -> str:
        return llm_provider_name_from_settings(self._settings)

    def _require_client(self) -> OpenAI:
        if self._client is None:
            raise ConfigurationFailure(
                "LLM_API_KEY is required for embeddings and draft generation. "
                "OPENAI_API_KEY remains accepted as a legacy alias."
            )
        return self._client

    def embed_text(self, text: str, *, model_id: str | None = None) -> list[float]:
        client = self._require_client()
        response = client.embeddings.create(
            model=model_id or self._settings.openai_embedding_model,
            input=text,
        )
        return [float(value) for value in response.data[0].embedding]

    def generate_case_profile(
        self,
        *,
        case_id: UUID,
        source_file_name: str,
        source_file_hash: str,
        client_name: str,
        language: str,
        page_text: list[str],
        model_id: str | None = None,
        reasoning_effort: Literal["low", "medium", "high"] | None = None,
    ) -> CaseProfileGenerationResult:
        client = self._require_client()
        request_payload = build_case_profile_extraction_request_payload(
            client_name=client_name,
            language_hint=language,
            document_text=_document_text_from_pages(page_text),
        )
        response_kwargs: dict[str, Any] = {}
        if reasoning_effort is not None:
            response_kwargs["reasoning"] = {"effort": reasoning_effort}
        requested_model_id = model_id or self._settings.openai_response_model
        response = client.responses.parse(
            model=requested_model_id,
            input=cast(Any, request_payload),
            text_format=CaseProfileExtractionOutput,
            **response_kwargs,
        )
        output = response.output_parsed
        if output is None:
            raise ValidationFailure(
                "The configured LLM provider returned no structured case-profile extraction output."
            )
        actual_model_id = getattr(response, "model", None)
        document = _build_case_profile_document(
            extraction_output=output,
            case_id=case_id,
            source_file_name=source_file_name,
            source_file_hash=source_file_hash,
            client_name=client_name,
            language=language,
            model_name=actual_model_id or requested_model_id,
        )
        return CaseProfileGenerationResult(
            document=document,
            structured_output=output,
            request_payload=request_payload,
            response_payload=output.model_dump(mode="json"),
            provider_response_id=getattr(response, "id", None),
            requested_model_id=requested_model_id,
            actual_model_id=actual_model_id,
            service_tier=getattr(response, "service_tier", None),
            usage_json=_usage_json(response),
        )

    def plan_answer(
        self,
        *,
        row_question: str,
        row_context: str,
        user_request: str,
        target_language: Literal["de", "en"],
        output_mode: Literal["customer_facing", "grounded_review"],
        normalized_evidence: list[NormalizedEvidenceItem],
        pipeline: PipelineSelection,
    ) -> AnswerPlanGenerationResult:
        client = self._require_client()
        request_payload = build_answer_planning_request_payload(
            row_question=row_question,
            row_context=row_context,
            user_request=user_request,
            target_language=target_language,
            output_mode=output_mode,
            normalized_evidence=canonical_json_text(
                [item.model_dump(mode="json") for item in normalized_evidence]
            ),
        )
        response_kwargs: dict[str, Any] = {}
        reasoning_effort = pipeline.resolved_pipeline.models.answer_planning.reasoning_effort
        if reasoning_effort is not None:
            response_kwargs["reasoning"] = {"effort": reasoning_effort}
        requested_model_id = pipeline.resolved_pipeline.models.answer_planning.model_id
        response = client.responses.parse(
            model=requested_model_id or self._settings.openai_response_model,
            input=cast(Any, request_payload),
            text_format=AnswerPlan,
            **response_kwargs,
        )
        answer_plan = response.output_parsed
        if answer_plan is None:
            raise ValidationFailure(
                "The configured LLM provider returned no structured AnswerPlan output."
            )
        return AnswerPlanGenerationResult(
            answer_plan=answer_plan,
            request_payload=request_payload,
            response_payload=answer_plan.model_dump(mode="json"),
            provider_response_id=getattr(response, "id", None),
            requested_model_id=requested_model_id or self._settings.openai_response_model,
            actual_model_id=getattr(response, "model", None),
            service_tier=getattr(response, "service_tier", None),
            usage_json=_usage_json(response),
        )

    def render_answer(
        self,
        *,
        row_question: str,
        row_context: str,
        user_request: str,
        thread_history: list[tuple[str, str]] | None,
        answer_plan: AnswerPlan,
        output_mode: Literal["customer_facing", "grounded_review"],
        target_language: Literal["de", "en"],
        pipeline: PipelineSelection,
    ) -> AnswerRenderGenerationResult:
        client = self._require_client()
        request_payload = build_answer_rendering_request_payload(
            row_question=row_question,
            row_context=row_context,
            answer_plan_json=canonical_json_text(answer_plan.model_dump(mode="json")),
            output_mode=output_mode,
            target_language=target_language,
            user_request=user_request,
            thread_history=thread_history,
        )
        response_kwargs: dict[str, Any] = {}
        if pipeline.resolved_pipeline.generation.temperature is not None:
            response_kwargs["temperature"] = pipeline.resolved_pipeline.generation.temperature
        reasoning_effort = pipeline.resolved_pipeline.models.answer_rendering.reasoning_effort
        if reasoning_effort is not None:
            response_kwargs["reasoning"] = {"effort": reasoning_effort}
        requested_model_id = pipeline.resolved_pipeline.models.answer_rendering.model_id
        response = cast(Any, client.responses).create(
            model=requested_model_id or self._settings.openai_response_model,
            input=cast(Any, request_payload),
            **response_kwargs,
        )
        raw_response_text = response.output_text
        answer = raw_response_text.strip()
        if not answer:
            raise ValidationFailure("The configured LLM provider returned an empty rendered answer.")
        request_text = canonical_json_text(request_payload)
        return AnswerRenderGenerationResult(
            request_payload=request_payload,
            request_text=request_text,
            response_text=raw_response_text,
            provider_response_id=getattr(response, "id", None),
            requested_model_id=requested_model_id or self._settings.openai_response_model,
            actual_model_id=getattr(response, "model", None),
            service_tier=getattr(response, "service_tier", None),
            usage_json=_usage_json(response),
        )


class StubAIService:
    def embed_text(self, text: str, *, model_id: str | None = None) -> list[float]:
        default_model_id = get_settings().openai_embedding_model
        salted_text = text
        if model_id is not None and model_id not in {default_model_id, "stub-ai-service"}:
            salted_text = f"{model_id}::{text}"
        value = sum(ord(character) for character in salted_text)
        return [float(value % 1000), float((value // 3) % 1000), float((value // 7) % 1000)]

    def generate_case_profile(
        self,
        *,
        case_id: UUID,
        source_file_name: str,
        source_file_hash: str,
        client_name: str,
        language: str,
        page_text: list[str],
        model_id: str | None = None,
        reasoning_effort: Literal["low", "medium", "high"] | None = None,
    ) -> CaseProfileGenerationResult:
        combined = " ".join(page_text)
        items: list[CaseProfileExtractionItem] = []
        for prompt in ANALYSIS_PROMPTS:
            answer = (
                "Unknown based on the available document text."
                if prompt.id == "stakeholders_governance"
                else combined[:220]
            )
            items.append(
                CaseProfileExtractionItem(
                    id=cast(CaseProfileExtractionItemId, prompt.id),
                    answer=answer,
                    support_level="unknown" if answer.startswith("Unknown") else "strongly_implied",
                    confidence="low" if answer.startswith("Unknown") else "medium",
                    citations=["citation_unavailable"],
                    unknowns=[] if not answer.startswith("Unknown") else ["Specific detail not stated"],
                )
            )
        structured_output = CaseProfileExtractionOutput(
            analysis_items=items,
            summary=f"Stub extraction summary for {client_name}.",
        )
        requested_model_id = model_id or "stub-ai-service"
        document = _build_case_profile_document(
            extraction_output=structured_output,
            case_id=case_id,
            source_file_name=source_file_name,
            source_file_hash=source_file_hash,
            client_name=client_name,
            language=language,
            model_name=requested_model_id,
        )
        request_payload = build_case_profile_extraction_request_payload(
            client_name=client_name,
            language_hint=language,
            document_text=_document_text_from_pages(page_text),
        )
        return CaseProfileGenerationResult(
            document=document,
            structured_output=structured_output,
            request_payload=request_payload,
            response_payload=structured_output.model_dump(mode="json"),
            provider_response_id=None,
            requested_model_id=requested_model_id,
            actual_model_id=requested_model_id,
            service_tier=None,
            usage_json=None,
        )

    def plan_answer(
        self,
        *,
        row_question: str,
        row_context: str,
        user_request: str,
        target_language: Literal["de", "en"],
        output_mode: Literal["customer_facing", "grounded_review"],
        normalized_evidence: list[NormalizedEvidenceItem],
        pipeline: PipelineSelection,
    ) -> AnswerPlanGenerationResult:
        primary_intent, secondary_intents = classify_question_intents(
            row_question=row_question,
            row_context=row_context,
            user_request=user_request,
        )
        current_case = [
            item for item in normalized_evidence if item.layer in {"current_case_facts", "raw_current_pdf"}
        ]
        product_truth = [item for item in normalized_evidence if item.layer == "product_truth"]
        historical = [item for item in normalized_evidence if item.layer == "historical_exemplar"]
        product_names = sorted(supported_product_names(normalized_evidence))
        historical_products = _extract_historical_product_mentions(normalized_evidence)
        question_text = " ".join([row_question, user_request]).lower()
        wants_reporting = any(
            token in question_text for token in ("reporting", "analytics", "report", "bericht", "dashboard")
        )
        wants_integration = any(
            token in question_text for token in ("integration", "integrationen", "schnittstelle", "api", "interface")
        )
        forbidden_claims = [
            "Current case facts",
            "Product truth",
            "Historical exemplars",
            "Aktuelle Fallfakten",
            "Produktwahrheit",
            "Historische Beispiele",
            "retrieval",
        ]
        forbidden_claims.extend(
            product
            for product in historical_products
            if product not in product_names
        )

        all_intents: list[AnswerIntent] = [primary_intent, *secondary_intents]
        supported_claims: list[SupportedClaim] = []
        historical_hints: list[str] = []
        unknowns: list[UnknownItem] = []

        if current_case:
            supported_claims.append(
                SupportedClaim(
                    claim=row_context_claim_text(current_case[0]),
                    claim_kind="scope_fit",
                    authority_layer="current_case_facts",
                    support_ids=[current_case[0].id],
                )
            )

        if "product_fit" in all_intents:
            if product_truth:
                top_truth = product_truth[:2]
                joined_names = " and ".join(item.title for item in top_truth) if target_language == "en" else " und ".join(item.title for item in top_truth)
                claim_text = (
                    f"{joined_names} cover the requested core process."
                    if target_language == "en"
                    else f"{joined_names} decken den angefragten Kernprozess ab."
                )
                supported_claims.append(
                    SupportedClaim(
                        claim=claim_text,
                        claim_kind="product_mapping",
                        authority_layer="product_truth",
                        support_ids=[item.id for item in top_truth],
                    )
                )
            else:
                unknowns.append(
                    UnknownItem(
                        topic=(
                            "which products or modules are included in the proposed scope"
                            if target_language == "en"
                            else "welche Produkte oder Module im vorgeschlagenen Umfang enthalten sind"
                        ),
                        reason=(
                            "The requested product mapping is not explicitly confirmed for the available scope."
                            if target_language == "en"
                            else "Die angefragte Produktzuordnung ist für den verfügbaren Scope nicht ausdrücklich bestätigt."
                        ),
                        materiality="material",
                    )
                )
            if wants_reporting and not product_truth_supports_feature(
                product_truth,
                keywords=("report", "analytics", "dashboard", "kennzahl"),
            ):
                unknowns.append(
                    UnknownItem(
                        topic=(
                            "whether a separate reporting module is part of the proposed scope"
                            if target_language == "en"
                            else "ob ein separates Reporting-Modul Teil des vorgeschlagenen Umfangs ist"
                        ),
                        reason=(
                            "Reporting support is not explicitly confirmed for the requested scope."
                            if target_language == "en"
                            else "Eine Reporting-Unterstützung ist für den angefragten Scope nicht ausdrücklich bestätigt."
                        ),
                        materiality="material",
                    )
                )
            if wants_integration and not product_truth_supports_feature(
                product_truth,
                keywords=("integration", "api", "schnittstelle", "interface"),
            ):
                unknowns.append(
                    UnknownItem(
                        topic=(
                            "the exact integration approach for the requested systems"
                            if target_language == "en"
                            else "der genaue Integrationsansatz für die angefragten Systeme"
                        ),
                        reason=(
                            "The requested integration scope is not explicitly confirmed."
                            if target_language == "en"
                            else "Der angefragte Integrationsumfang ist nicht ausdrücklich bestätigt."
                        ),
                        materiality="material",
                    )
                )
            thesis = (
                "The requested core process is covered by the supported product scope."
                if target_language == "en"
                else "Der angefragte Kernprozess wird durch den unterstützten Produktumfang abgedeckt."
            )
        else:
            thesis = (
                "The answer should stay concise and limited to supported scope."
                if target_language == "en"
                else "Die Antwort soll knapp bleiben und sich auf den belegten Scope beschränken."
            )

        if "rollout_approach" in all_intents:
            if historical:
                historical_hints.append(
                    "Use historical material only as a generic phased-rollout pattern, not as proof of a specific outcome."
                    if target_language == "en"
                    else "Historisches Material nur als generisches Muster für eine phasenweise Einführung verwenden, nicht als Beleg für ein konkretes Ergebnis."
                )
            if target_language == "en":
                thesis = f"{thesis.rstrip('.')} and a phased rollout across the requested depots is feasible."
            else:
                thesis = f"{thesis.rstrip('.')} und eine phasenweise Einführung über die angefragten Depots ist umsetzbar."

        if "reporting_analytics" in all_intents:
            reporting_truth = [
                item
                for item in product_truth
                if product_truth_supports_feature([item], keywords=("report", "analytics", "dashboard", "kennzahl"))
            ]
            if reporting_truth:
                supported_claims.append(
                    SupportedClaim(
                        claim=(
                            f"{reporting_truth[0].title} provides the supported reporting scope."
                            if target_language == "en"
                            else f"{reporting_truth[0].title} stellt den belegten Reporting-Umfang bereit."
                        ),
                        claim_kind="reporting",
                        authority_layer="product_truth",
                        support_ids=[reporting_truth[0].id],
                    )
                )
            else:
                unknowns.append(
                    UnknownItem(
                        topic=(
                            "whether additional analytics functionality is included"
                            if target_language == "en"
                            else "ob zusätzliche Analysefunktionen enthalten sind"
                        ),
                        reason=(
                            "Reporting support is not explicitly confirmed for the requested scope."
                            if target_language == "en"
                            else "Eine Reporting-Unterstützung ist für den angefragten Scope nicht ausdrücklich bestätigt."
                        ),
                        materiality="material",
                    )
                )
        if "integration" in all_intents:
            integration_truth = [
                item
                for item in product_truth
                if product_truth_supports_feature([item], keywords=("integration", "api", "schnittstelle", "interface"))
            ]
            if integration_truth:
                supported_claims.append(
                    SupportedClaim(
                        claim=(
                            f"{integration_truth[0].title} supports the confirmed integration scope."
                            if target_language == "en"
                            else f"{integration_truth[0].title} unterstützt den bestätigten Integrationsumfang."
                        ),
                        claim_kind="integration",
                        authority_layer="product_truth",
                        support_ids=[integration_truth[0].id],
                    )
                )
            else:
                unknowns.append(
                    UnknownItem(
                        topic=(
                            "the exact integration approach for SAP and identity systems"
                            if target_language == "en"
                            else "der genaue Integrationsansatz für SAP und Identitätssysteme"
                        ),
                        reason=(
                            "The requested integration scope is not explicitly confirmed."
                            if target_language == "en"
                            else "Der angefragte Integrationsumfang ist nicht ausdrücklich bestätigt."
                        ),
                        materiality="material",
                    )
                )
        if "security_compliance" in all_intents:
            security_truth = [
                item
                for item in product_truth
                if any(token in item.text.lower() for token in ("security", "privacy", "compliance", "audit"))
            ]
            if security_truth:
                supported_claims.append(
                    SupportedClaim(
                        claim=(
                            f"{security_truth[0].title} covers the confirmed security and compliance scope."
                            if target_language == "en"
                            else f"{security_truth[0].title} deckt den bestätigten Sicherheits- und Compliance-Umfang ab."
                        ),
                        claim_kind="security",
                        authority_layer="product_truth",
                        support_ids=[security_truth[0].id],
                    )
                )
            else:
                unknowns.append(
                    UnknownItem(
                        topic=(
                            "whether additional compliance functionality is included"
                            if target_language == "en"
                            else "ob zusätzliche Compliance-Funktionalität enthalten ist"
                        ),
                        reason=(
                            "The requested security or compliance scope is not explicitly confirmed."
                            if target_language == "en"
                            else "Der angefragte Sicherheits- oder Compliance-Umfang ist nicht ausdrücklich bestätigt."
                        ),
                        materiality="material",
                    )
                )
        if "workflow_capability" in all_intents and current_case:
            supported_claims.append(
                SupportedClaim(
                    claim=(
                        "The customer requires structured exception handling across depots with clear ownership and common status control."
                        if target_language == "en"
                        else "Der Kunde benötigt ein strukturiertes Ausnahmehandling über mehrere Depots mit klarer Zuständigkeit und gemeinsamer Statussteuerung."
                    ),
                    claim_kind="capability",
                    authority_layer="current_case_facts",
                    support_ids=[current_case[0].id],
                )
            )

        answer_plan = AnswerPlan(
            primary_intent=primary_intent,
            secondary_intents=list(secondary_intents),
            target_language=target_language,
            output_mode=output_mode,
            direct_answer_thesis=thesis,
            supported_claims=supported_claims,
            historical_style_guidance=historical_hints[:3],
            unknowns=unknowns,
            forbidden_claims=sorted(
                claim
                for claim in set(forbidden_claims)
                if claim.lower()
                not in " ".join(item.claim.lower() for item in supported_claims)
            ),
            answer_shape=default_answer_shape(
                primary_intent=primary_intent,
                secondary_intents=secondary_intents,
                target_word_min=pipeline.resolved_pipeline.generation.target_answer_words_min,
                target_word_max=pipeline.resolved_pipeline.generation.target_answer_words_max,
            ),
        )
        request_payload = build_answer_planning_request_payload(
            row_question=row_question,
            row_context=row_context,
            user_request=user_request,
            target_language=target_language,
            output_mode=output_mode,
            normalized_evidence=canonical_json_text(
                [item.model_dump(mode="json") for item in normalized_evidence]
            ),
        )
        return AnswerPlanGenerationResult(
            answer_plan=answer_plan,
            request_payload=request_payload,
            response_payload=answer_plan.model_dump(mode="json"),
            provider_response_id=None,
            requested_model_id=pipeline.resolved_pipeline.models.answer_planning.model_id,
            actual_model_id=pipeline.resolved_pipeline.models.answer_planning.model_id,
            service_tier=None,
            usage_json=None,
        )

    def render_answer(
        self,
        *,
        row_question: str,
        row_context: str,
        user_request: str,
        thread_history: list[tuple[str, str]] | None,
        answer_plan: AnswerPlan,
        output_mode: Literal["customer_facing", "grounded_review"],
        target_language: Literal["de", "en"],
        pipeline: PipelineSelection,
    ) -> AnswerRenderGenerationResult:
        body_parts = [answer_plan.direct_answer_thesis.rstrip(".")]
        body_parts.extend(
            claim.claim.rstrip(".")
            for claim in answer_plan.supported_claims[:2]
            if claim.claim.rstrip(".").lower() != answer_plan.direct_answer_thesis.rstrip(".").lower()
        )
        material_unknowns = [
            item for item in answer_plan.unknowns if item.materiality == "material"
        ]
        if material_unknowns:
            if target_language == "de":
                body_parts.append(
                    f"Offen bleibt {material_unknowns[0].topic}: {material_unknowns[0].reason.rstrip('.')}."
                )
            else:
                body_parts.append(
                    f"An open point is {material_unknowns[0].topic}: {material_unknowns[0].reason.rstrip('.')}."
                )
        response_text = " ".join(part.rstrip(".") + "." for part in body_parts if part.strip())
        request_payload = build_answer_rendering_request_payload(
            row_question=row_question,
            row_context=row_context,
            answer_plan_json=canonical_json_text(answer_plan.model_dump(mode="json")),
            output_mode=output_mode,
            target_language=target_language,
            user_request=user_request,
            thread_history=thread_history,
        )
        return AnswerRenderGenerationResult(
            request_payload=request_payload,
            request_text=canonical_json_text(request_payload),
            response_text=response_text,
            provider_response_id=None,
            requested_model_id=pipeline.resolved_pipeline.models.answer_rendering.model_id,
            actual_model_id=pipeline.resolved_pipeline.models.answer_rendering.model_id,
            service_tier=None,
            usage_json=None,
        )
