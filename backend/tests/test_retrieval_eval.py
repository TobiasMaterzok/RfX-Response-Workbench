from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.config import Settings, build_settings
from app.models.entities import (
    CaseProfile,
    CaseProfileItem,
    ChatThread,
    PdfPage,
    Questionnaire,
    QuestionnaireRow,
    RetrievalRun,
    RfxCase,
    Upload,
)
from app.models.enums import UploadKind
from app.pipeline.config import (
    artifact_index_hashes,
    case_profile_index_payload,
    resolve_pipeline_selection,
)
from app.services.ai import StubAIService
from app.services.answers import draft_answer_for_row
from app.services.cases import create_case_from_uploads
from app.services.hashing import sha256_text
from app.services.identity import ensure_local_identity
from app.services.pdf_chunks import persist_pdf_chunks
from app.services.product_truth import ingest_product_truth_file
from app.services.retrieval import build_retrieval_request, build_retrieval_run
from app.services.seed import import_historical_corpus
from tests.seed_paths import historical_customer_dir, product_truth_path, seed_data_root


def sample_pdf_bytes(repo_root: Path) -> bytes:
    return (
        historical_customer_dir(repo_root, "nordtransit_logistik_ag")
        / "nordtransit_logistik_ag_context_brief.pdf"
    ).read_bytes()


def sample_xlsx_bytes(repo_root: Path) -> bytes:
    return (
        historical_customer_dir(repo_root, "nordtransit_logistik_ag")
        / "nordtransit_logistik_ag_qa.xlsx"
    ).read_bytes()


def create_manual_case(
    session: Session,
    *,
    tenant_id,
    user_id,
    ai_service: StubAIService,
    row_context: str,
    row_question: str,
) -> tuple[RfxCase, QuestionnaireRow, ChatThread]:
    pipeline = resolve_pipeline_selection(build_settings(env_file=None))
    artifact_hashes = artifact_index_hashes(pipeline)
    case = RfxCase(
        tenant_id=tenant_id,
        created_by_user_id=user_id,
        name="Eval Case",
        client_name="Eval Client",
        language="en",
        pipeline_profile_name=pipeline.profile_name,
        pipeline_config_json=pipeline.resolved_config,
        pipeline_config_hash=pipeline.config_hash,
        index_config_hash=pipeline.index_config_hash,
    )
    session.add(case)
    session.flush()
    pdf_upload = Upload(
        tenant_id=tenant_id,
        case_id=case.id,
        kind=UploadKind.CASE_PDF,
        original_file_name="eval.pdf",
        media_type="application/pdf",
        object_key=f"tests/{case.id}/eval.pdf",
        file_hash="eval-pdf-hash",
        size_bytes=1,
        payload=b"x",
    )
    questionnaire_upload = Upload(
        tenant_id=tenant_id,
        case_id=case.id,
        kind=UploadKind.QUESTIONNAIRE_XLSX,
        original_file_name="eval.xlsx",
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        object_key=f"tests/{case.id}/eval.xlsx",
        file_hash="eval-xlsx-hash",
        size_bytes=1,
        payload=b"x",
    )
    session.add_all([pdf_upload, questionnaire_upload])
    session.flush()
    profile = CaseProfile(
        tenant_id=tenant_id,
        case_id=case.id,
        source_pdf_upload_id=pdf_upload.id,
        schema_version="rfx_case_profile.v3",
        prompt_set_version="rfx_case_profile_prompt_set.v3",
        model="stub-ai-service",
        summary="Logistics workflow modernization for carriers.",
        source_file_name="eval.pdf",
        source_file_hash="eval-pdf-hash",
        language="en",
        pipeline_profile_name=pipeline.profile_name,
        index_config_json=case_profile_index_payload(pipeline.resolved_pipeline),
        index_config_hash=artifact_hashes.case_profile,
        generated_at=datetime.now(UTC),
        document={"case_id": str(case.id)},
    )
    session.add(profile)
    session.flush()
    for position, (analysis_item_id, answer) in enumerate(
        [
            ("security_privacy_regulatory", "GDPR controls and German hosting support."),
            ("architecture_integration_data", "SAP integration and API connectivity."),
        ],
        start=1,
    ):
        prompt = f"Prompt for {analysis_item_id}"
        session.add(
            CaseProfileItem(
                tenant_id=tenant_id,
                case_profile_id=profile.id,
                case_id=case.id,
                analysis_item_id=analysis_item_id,
                position=position,
                prompt=prompt,
                answer=answer,
                confidence="high",
                citations=["citation_unavailable"],
                normalized_text=f"{prompt}\n{answer}",
                embedding=ai_service.embed_text(f"{prompt}\n{answer}"),
            )
        )
    pages = [
        PdfPage(
            tenant_id=tenant_id,
            case_id=case.id,
            upload_id=pdf_upload.id,
            page_number=1,
            extracted_text="Carrier onboarding portal with multilingual supplier forms.",
            text_hash=sha256_text("Carrier onboarding portal with multilingual supplier forms."),
        )
    ]
    session.add_all(pages)
    session.flush()
    persist_pdf_chunks(
        session,
        ai_service=ai_service,
        pipeline=pipeline,
        tenant_id=tenant_id,
        case_id=case.id,
        upload_id=pdf_upload.id,
        pages=pages,
        case_name=case.name,
        client_name=case.client_name,
        language=case.language,
        source_file_name=pdf_upload.original_file_name,
    )
    questionnaire = Questionnaire(
        tenant_id=tenant_id,
        case_id=case.id,
        upload_id=questionnaire_upload.id,
        source_file_name="eval.xlsx",
        source_sheet_name="QA",
        file_hash="eval-xlsx-hash",
        schema_version="questionnaire_workbook.v1",
    )
    session.add(questionnaire)
    session.flush()
    row = QuestionnaireRow(
        tenant_id=tenant_id,
        case_id=case.id,
        questionnaire_id=questionnaire.id,
        source_sheet_name="QA",
        source_row_number=2,
        source_row_id="eval.xlsx:QA:2",
        context_raw=row_context,
        question_raw=row_question,
        answer_raw="",
        normalized_text=f"{row_context}\n{row_question}",
    )
    session.add(row)
    session.flush()
    thread = ChatThread(
        tenant_id=tenant_id,
        case_id=case.id,
        questionnaire_row_id=row.id,
        created_by_user_id=user_id,
        title="Eval thread",
    )
    session.add(thread)
    session.flush()
    return case, row, thread


def test_retrieval_eval_seed_case_smoke(
    session: Session,
    container,
    repo_root: Path,
    settings: Settings,
) -> None:
    context = ensure_local_identity(session, settings)
    import_historical_corpus(
        session,
        ai_service=StubAIService(),
        storage=container.storage,
        tenant_id=context.tenant.id,
        base_path=seed_data_root(repo_root),
    )
    ingest_product_truth_file(
        session,
        ai_service=StubAIService(),
        tenant_id=context.tenant.id,
        path=product_truth_path(repo_root),
    )
    case = create_case_from_uploads(
        session,
        storage=container.storage,
        ai_service=StubAIService(),
        tenant_id=context.tenant.id,
        user_id=context.user.id,
        case_name="Eval Seed Case",
        client_name="NordTransit Logistik AG",
        pdf_file_name="nordtransit_logistik_ag_context_brief.pdf",
        pdf_media_type="application/pdf",
        pdf_payload=sample_pdf_bytes(repo_root),
        questionnaire_file_name="nordtransit_logistik_ag_qa.xlsx",
        questionnaire_media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        questionnaire_payload=sample_xlsx_bytes(repo_root),
    )
    row = session.scalar(
        select(QuestionnaireRow)
        .where(QuestionnaireRow.case_id == case.id)
        .order_by(QuestionnaireRow.source_row_number.asc())
    )
    assert row is not None
    result = draft_answer_for_row(
        session,
        ai_service=StubAIService(),
        case=case,
        row=row,
        user_id=context.user.id,
        user_message="Draft the answer with explicit grounding.",
        thread=None,
    )
    labels = {item.source_label for item in result.evidence_items}
    assert "current_case_facts" in labels
    assert "raw_current_pdf" in labels
    assert "historical_exemplar" in labels
    assert "product_truth" in labels


def test_retrieval_eval_context_shift_smoke(session: Session, settings: Settings) -> None:
    context = ensure_local_identity(session, settings)
    ai_service = StubAIService()
    case, row, thread = create_manual_case(
        session,
        tenant_id=context.tenant.id,
        user_id=context.user.id,
        ai_service=ai_service,
        row_context="GDPR, German hosting, and data residency requirements",
        row_question="How will the requirement be addressed?",
    )
    request = build_retrieval_request(
        session,
        case=case,
        row=row,
        user_message="Draft the answer.",
        revision_mode="initial_draft",
        retrieval_action="refresh_retrieval",
        previous_answer_text=None,
    )
    _, evidence = build_retrieval_run(
        session,
        ai_service=ai_service,
        tenant_id=context.tenant.id,
        case_id=case.id,
        row=row,
        thread_id=thread.id,
        request=request,
    )
    current_case_facts = [item for item in evidence if item.source_label == "current_case_facts"]
    assert current_case_facts[0].source_title == "security_privacy_regulatory"

    row.context_raw = "SAP integration landscape and ERP connectivity requirements"
    row.normalized_text = f"{row.context_raw}\n{row.question_raw}"
    session.flush()
    request = build_retrieval_request(
        session,
        case=case,
        row=row,
        user_message="Draft the answer.",
        revision_mode="initial_draft",
        retrieval_action="refresh_retrieval",
        previous_answer_text=None,
    )
    _, evidence = build_retrieval_run(
        session,
        ai_service=ai_service,
        tenant_id=context.tenant.id,
        case_id=case.id,
        row=row,
        thread_id=thread.id,
        request=request,
    )
    current_case_facts = [item for item in evidence if item.source_label == "current_case_facts"]
    assert current_case_facts[0].source_title == "architecture_integration_data"


def test_retrieval_eval_revision_and_degraded_smoke(
    session: Session,
    settings: Settings,
) -> None:
    context = ensure_local_identity(session, settings)
    ai_service = StubAIService()
    case, row, _ = create_manual_case(
        session,
        tenant_id=context.tenant.id,
        user_id=context.user.id,
        ai_service=ai_service,
        row_context="Carrier onboarding portal and multilingual supplier forms",
        row_question="How will the portal work?",
    )
    first = draft_answer_for_row(
        session,
        ai_service=ai_service,
        case=case,
        row=row,
        user_id=context.user.id,
        user_message="Draft the answer.",
        thread=None,
    )
    first_run = session.get(RetrievalRun, first.answer_version.retrieval_run_id)
    assert first_run is not None
    assert first_run.request_context["retrieval_execution"]["sufficiency"]["status"] == "degraded"

    second = draft_answer_for_row(
        session,
        ai_service=ai_service,
        case=case,
        row=row,
        user_id=context.user.id,
        user_message="Make it shorter",
        thread=first.thread,
    )
    second_run = session.get(RetrievalRun, second.answer_version.retrieval_run_id)
    assert second_run is not None
    assert second.answer_version.retrieval_run_id == first.answer_version.retrieval_run_id

    third = draft_answer_for_row(
        session,
        ai_service=ai_service,
        case=case,
        row=row,
        user_id=context.user.id,
        user_message="Mention GDPR and German hosting.",
        thread=first.thread,
    )
    third_run = session.get(RetrievalRun, third.answer_version.retrieval_run_id)
    assert third_run is not None
    assert third_run.request_context["retrieval_action"] == "refresh_retrieval"
