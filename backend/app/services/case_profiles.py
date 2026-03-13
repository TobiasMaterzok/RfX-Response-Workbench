from __future__ import annotations

from collections.abc import Sequence
from uuid import UUID

from pydantic import ValidationError as PydanticValidationError
from sqlalchemy.orm import Session

from app.exceptions import ValidationFailure
from app.models.entities import (
    CaseProfile,
    CaseProfileItem,
    HistoricalCaseProfile,
    HistoricalCaseProfileItem,
    HistoricalClientPackage,
    PdfPage,
    RfxCase,
    Upload,
)
from app.models.enums import ModelInvocationKind
from app.pipeline.config import (
    PipelineSelection,
    artifact_index_hashes,
    case_profile_index_payload,
    historical_index_payload,
)
from app.prompts.case_profile import CASE_PROFILE_PROMPT_SET_VERSION, CASE_PROFILE_SCHEMA_VERSION
from app.prompts.case_profile_extraction import CASE_PROFILE_EXTRACTION_PROMPT_VERSION
from app.schemas.case_profile import CaseProfileAnalysisItem, CaseProfileDocument
from app.schemas.case_profile_extraction import CaseProfileExtractionOutput
from app.services.ai import (
    AIService,
    CaseProfileGenerationResult,
    embedding_model_name,
    openai_sdk_version,
)
from app.services.hashing import sha256_text
from app.services.reproducibility import (
    ReproContext,
    canonical_json_text,
    embed_text_recorded,
    record_model_invocation,
)

CASE_SIGNATURE_VERSION = "case_signature.v1"


def _validate_case_profile_document(
    raw_document: CaseProfileDocument,
    *,
    expected_case_id: UUID,
    expected_file_name: str,
    expected_file_hash: str,
) -> CaseProfileDocument:
    try:
        document = CaseProfileDocument.model_validate(raw_document.model_dump(mode="python"))
    except PydanticValidationError as exc:
        raise ValidationFailure(f"Generated case_profile failed schema validation: {exc}") from exc
    if document.case_id != expected_case_id:
        raise ValidationFailure(
            f"Generated case_profile case_id mismatch: expected {expected_case_id}, observed {document.case_id}."
        )
    if document.source_pdf.file_name != expected_file_name:
        raise ValidationFailure(
            "Generated case_profile source file name mismatch: "
            f"expected {expected_file_name!r}, observed {document.source_pdf.file_name!r}."
        )
    if document.source_pdf.file_hash != expected_file_hash:
        raise ValidationFailure(
            "Generated case_profile source file hash mismatch: "
            f"expected {expected_file_hash}, observed {document.source_pdf.file_hash}."
        )
    return document


def generate_case_profile_document(
    *,
    ai_service: AIService,
    pipeline: PipelineSelection,
    case_id: UUID,
    source_file_name: str,
    source_file_hash: str,
    client_name: str,
    language: str,
    page_text: Sequence[str],
) -> CaseProfileGenerationResult:
    raw_result = ai_service.generate_case_profile(
        case_id=case_id,
        source_file_name=source_file_name,
        source_file_hash=source_file_hash,
        client_name=client_name,
        language=language,
        page_text=list(page_text),
        model_id=pipeline.resolved_pipeline.models.case_profile_extraction.model_id,
        reasoning_effort=pipeline.resolved_pipeline.models.case_profile_extraction.reasoning_effort,
    )
    result = (
        raw_result
        if isinstance(raw_result, CaseProfileGenerationResult)
        else CaseProfileGenerationResult(
            document=raw_result,
            structured_output=CaseProfileExtractionOutput.model_construct(
                analysis_items=[
                    {
                        "id": item.id,
                        "answer": item.answer,
                        "support_level": getattr(item, "support_level", "unknown"),
                        "confidence": item.confidence,
                        "citations": item.citations,
                        "unknowns": getattr(item, "unknowns", []),
                    }
                    for item in raw_result.analysis_items
                ],
                summary=raw_result.summary,
            ),
            request_payload=[],
            response_payload=raw_result.model_dump(mode="json"),
            provider_response_id=None,
            requested_model_id=None,
            actual_model_id=None,
            service_tier=None,
            usage_json=None,
        )
    )
    try:
        structured_output = CaseProfileExtractionOutput.model_validate(
            result.structured_output.model_dump(mode="python")
        )
    except PydanticValidationError as exc:
        raise ValidationFailure(
            f"Generated case_profile failed schema validation: extraction output invalid: {exc}"
        ) from exc
    document = _validate_case_profile_document(
        result.document,
        expected_case_id=case_id,
        expected_file_name=source_file_name,
        expected_file_hash=source_file_hash,
    )
    return CaseProfileGenerationResult(
        document=document,
        structured_output=structured_output,
        request_payload=result.request_payload,
        response_payload=result.response_payload,
        provider_response_id=result.provider_response_id,
        requested_model_id=result.requested_model_id,
        actual_model_id=result.actual_model_id,
        service_tier=result.service_tier,
        usage_json=result.usage_json,
    )


def build_case_profile_signature_text(
    *,
    summary: str,
    signature_mode: str = "summary_plus_analysis_items",
    analysis_items: Sequence[CaseProfileAnalysisItem] | Sequence[CaseProfileItem] | Sequence[HistoricalCaseProfileItem],
) -> str:
    lines = [summary.strip()]
    if signature_mode == "summary_only":
        return "\n".join(line for line in lines if line)
    for item in analysis_items:
        item_id = item.analysis_item_id if hasattr(item, "analysis_item_id") else item.id
        answer = item.answer
        lines.append(f"{item_id}: {answer.strip()}")
    return "\n".join(line for line in lines if line)


def persist_case_profile(
    session: Session,
    *,
    ai_service: AIService,
    pipeline: PipelineSelection,
    case: RfxCase,
    upload: Upload,
    pdf_pages: Sequence[PdfPage],
    artifact_build_id=None,
    repro_context: ReproContext | None = None,
    storage=None,
) -> CaseProfile:
    artifact_hashes = artifact_index_hashes(pipeline)
    page_text = [
        page.extracted_text for page in sorted(pdf_pages, key=lambda item: item.page_number)
    ]
    generation = generate_case_profile_document(
        ai_service=ai_service,
        pipeline=pipeline,
        case_id=case.id,
        source_file_name=upload.original_file_name,
        source_file_hash=upload.file_hash,
        client_name=case.client_name or case.name,
        language=case.language,
        page_text=page_text,
    )
    document = generation.document
    profile = CaseProfile(
        tenant_id=case.tenant_id,
        case_id=case.id,
        source_pdf_upload_id=upload.id,
        schema_version=CASE_PROFILE_SCHEMA_VERSION,
        prompt_set_version=CASE_PROFILE_PROMPT_SET_VERSION,
        model=document.model,
        summary=document.summary,
        source_file_name=document.source_pdf.file_name,
        source_file_hash=document.source_pdf.file_hash,
        language=document.language,
        pipeline_profile_name=pipeline.profile_name,
        index_config_json=case_profile_index_payload(pipeline.resolved_pipeline),
        index_config_hash=artifact_hashes.case_profile,
        artifact_build_id=artifact_build_id,
        generated_at=document.generated_at,
        document=document.model_dump(mode="json"),
    )
    session.add(profile)
    session.flush()
    if repro_context is not None and storage is not None:
        record_model_invocation(
            session,
            storage=storage,
            execution_run=repro_context.execution_run,
            provider_name="openai" if embedding_model_name(ai_service) != "stub-ai-service" else "stub",
            endpoint_kind="responses.parse",
            kind=ModelInvocationKind.CASE_PROFILE_EXTRACTION,
            requested_model_id=generation.requested_model_id,
            actual_model_id=generation.actual_model_id,
            reasoning_effort=pipeline.resolved_pipeline.models.case_profile_extraction.reasoning_effort,
            temperature=None,
            embedding_model_id=None,
            tokenizer_identity=None,
            tokenizer_version=None,
            request_payload=generation.request_payload,
            response_payload=generation.response_payload,
            provider_response_id=generation.provider_response_id,
            sdk_version=openai_sdk_version() if generation.requested_model_id != "stub-ai-service" else None,
            service_tier=generation.service_tier,
            usage_json=generation.usage_json,
            metadata_json={
                "prompt_family": "case_profile_extraction",
                "prompt_version": CASE_PROFILE_EXTRACTION_PROMPT_VERSION,
                "template_source": "backend/app/prompts/case_profile_extraction.py",
                "resolved_prompt_hash": sha256_text(
                    canonical_json_text(generation.request_payload)
                ),
                "structured_output_schema_version": generation.structured_output.schema_version,
                "structured_output_hash": sha256_text(
                    canonical_json_text(generation.response_payload)
                ),
            },
        )
    for position, item in enumerate(document.analysis_items, start=1):
        session.add(
            CaseProfileItem(
                tenant_id=case.tenant_id,
                case_profile_id=profile.id,
                case_id=case.id,
                analysis_item_id=item.id,
                position=position,
                prompt=item.prompt,
                answer=item.answer,
                confidence=item.confidence,
                citations=item.citations,
                normalized_text=f"{item.prompt}\n{item.answer}",
                embedding=(
                    embed_text_recorded(
                        session,
                        storage=storage,
                        execution_run=repro_context.execution_run,
                        ai_service=ai_service,
                        text=f"{item.prompt}\n{item.answer}",
                        model_id=pipeline.resolved_pipeline.indexing.embedding_model,
                        metadata_json={
                            "artifact_family": "case_profile_item",
                            "analysis_item_id": item.id,
                        },
                    )
                    if repro_context is not None and storage is not None
                    else ai_service.embed_text(
                        f"{item.prompt}\n{item.answer}",
                        model_id=pipeline.resolved_pipeline.indexing.embedding_model,
                    )
                ),
            )
        )
    session.flush()
    return profile


def persist_historical_case_profile(
    session: Session,
    *,
    ai_service: AIService,
    pipeline: PipelineSelection,
    client_package: HistoricalClientPackage,
    page_text: Sequence[str],
    repro_context: ReproContext | None = None,
    storage=None,
) -> HistoricalCaseProfile:
    generation = generate_case_profile_document(
        ai_service=ai_service,
        pipeline=pipeline,
        case_id=client_package.id,
        source_file_name=client_package.source_pdf_file_name,
        source_file_hash=client_package.source_pdf_file_hash,
        client_name=client_package.client_name,
        language=client_package.language,
        page_text=page_text,
    )
    document = generation.document
    signature_text = build_case_profile_signature_text(
        summary=document.summary,
        signature_mode=pipeline.resolved_pipeline.indexing.historical.signature_mode,
        analysis_items=document.analysis_items,
    )
    profile = HistoricalCaseProfile(
        tenant_id=client_package.tenant_id,
        client_package_id=client_package.id,
        schema_version=CASE_PROFILE_SCHEMA_VERSION,
        prompt_set_version=CASE_PROFILE_PROMPT_SET_VERSION,
        model=document.model,
        summary=document.summary,
        source_file_name=document.source_pdf.file_name,
        source_file_hash=document.source_pdf.file_hash,
        language=document.language,
        generated_at=document.generated_at,
        signature_version=CASE_SIGNATURE_VERSION,
        signature_embedding_model=(
            pipeline.resolved_pipeline.indexing.embedding_model
            or embedding_model_name(ai_service)
        ),
        signature_fields_json={
            "summary": document.summary,
            "analysis_item_ids": [item.id for item in document.analysis_items],
            "signature_mode": pipeline.resolved_pipeline.indexing.historical.signature_mode,
            "index_config": historical_index_payload(pipeline.resolved_pipeline),
        },
        signature_text=signature_text,
        signature_embedding=(
            embed_text_recorded(
                session,
                storage=storage,
                execution_run=repro_context.execution_run,
                ai_service=ai_service,
                text=signature_text,
                model_id=pipeline.resolved_pipeline.indexing.embedding_model,
                metadata_json={"artifact_family": "historical_case_profile_signature"},
            )
            if repro_context is not None and storage is not None
            else ai_service.embed_text(
                signature_text,
                model_id=pipeline.resolved_pipeline.indexing.embedding_model,
            )
        ),
        document=document.model_dump(mode="json"),
    )
    session.add(profile)
    session.flush()
    if repro_context is not None and storage is not None:
        record_model_invocation(
            session,
            storage=storage,
            execution_run=repro_context.execution_run,
            provider_name="openai" if embedding_model_name(ai_service) != "stub-ai-service" else "stub",
            endpoint_kind="responses.parse",
            kind=ModelInvocationKind.CASE_PROFILE_EXTRACTION,
            requested_model_id=generation.requested_model_id,
            actual_model_id=generation.actual_model_id,
            reasoning_effort=pipeline.resolved_pipeline.models.case_profile_extraction.reasoning_effort,
            temperature=None,
            embedding_model_id=None,
            tokenizer_identity=None,
            tokenizer_version=None,
            request_payload=generation.request_payload,
            response_payload=generation.response_payload,
            provider_response_id=generation.provider_response_id,
            sdk_version=openai_sdk_version() if generation.requested_model_id != "stub-ai-service" else None,
            service_tier=generation.service_tier,
            usage_json=generation.usage_json,
            metadata_json={
                "prompt_family": "case_profile_extraction",
                "prompt_version": CASE_PROFILE_EXTRACTION_PROMPT_VERSION,
                "template_source": "backend/app/prompts/case_profile_extraction.py",
                "resolved_prompt_hash": sha256_text(
                    canonical_json_text(generation.request_payload)
                ),
                "structured_output_schema_version": generation.structured_output.schema_version,
                "structured_output_hash": sha256_text(
                    canonical_json_text(generation.response_payload)
                ),
            },
        )
    for position, item in enumerate(document.analysis_items, start=1):
        session.add(
            HistoricalCaseProfileItem(
                tenant_id=client_package.tenant_id,
                historical_case_profile_id=profile.id,
                analysis_item_id=item.id,
                position=position,
                prompt=item.prompt,
                answer=item.answer,
                confidence=item.confidence,
                citations=item.citations,
                normalized_text=f"{item.prompt}\n{item.answer}",
                embedding=(
                    embed_text_recorded(
                        session,
                        storage=storage,
                        execution_run=repro_context.execution_run,
                        ai_service=ai_service,
                        text=f"{item.prompt}\n{item.answer}",
                        model_id=pipeline.resolved_pipeline.indexing.embedding_model,
                        metadata_json={
                            "artifact_family": "historical_case_profile_item",
                            "analysis_item_id": item.id,
                        },
                    )
                    if repro_context is not None and storage is not None
                    else ai_service.embed_text(
                        f"{item.prompt}\n{item.answer}",
                        model_id=pipeline.resolved_pipeline.indexing.embedding_model,
                    )
                ),
            )
        )
    session.flush()
    return profile
