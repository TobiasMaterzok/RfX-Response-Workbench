from __future__ import annotations

import importlib.metadata
import uuid

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from app.config import Settings, get_settings
from app.exceptions import ScopeViolation, ValidationFailure
from app.models.entities import (
    ArtifactBuild,
    CaseProfile,
    CaseProfileItem,
    PdfChunk,
    PdfPage,
    Questionnaire,
    QuestionnaireRow,
    RfxCase,
    Upload,
)
from app.models.enums import (
    ArtifactBuildKind,
    CaseStatus,
    ExecutionRunKind,
    ReproducibilityMode,
    SourceManifestKind,
    UploadKind,
)
from app.pipeline.config import PipelineSelection, artifact_index_hashes, resolve_pipeline_selection
from app.prompts.case_profile import CASE_PROFILE_PROMPT_SET_VERSION, CASE_PROFILE_SCHEMA_VERSION
from app.services.ai import AIService
from app.services.case_profiles import persist_case_profile
from app.services.hashing import sha256_hex
from app.services.object_keys import safe_object_key_filename
from app.services.pdf_chunks import current_pdf_chunking_version, persist_pdf_chunks
from app.services.pdfs import extract_pdf
from app.services.reproducibility import (
    assert_execution_run_consistency,
    create_artifact_build,
    finish_execution_run,
    get_or_create_source_manifest,
    live_case_input_manifest,
    start_repro_run,
)
from app.services.storage import LocalObjectStorage
from app.services.workbooks import (
    QUESTIONNAIRE_SCHEMA_VERSION,
    infer_language,
    parse_workbook_bytes,
)


def _persist_upload(
    session: Session,
    *,
    storage: LocalObjectStorage,
    tenant_id,
    case_id,
    kind: UploadKind,
    file_name: str,
    media_type: str,
    payload: bytes,
) -> Upload:
    object_key_file_name = safe_object_key_filename(file_name, fallback_stem="upload")
    stored = storage.save_bytes(
        object_key=f"cases/{case_id}/{kind.value}/{uuid.uuid4()}-{object_key_file_name}",
        payload=payload,
    )
    upload = Upload(
        tenant_id=tenant_id,
        case_id=case_id,
        kind=kind,
        original_file_name=file_name,
        media_type=media_type,
        object_key=stored.object_key,
        file_hash=stored.file_hash,
        size_bytes=stored.size_bytes,
        payload=stored.payload,
    )
    session.add(upload)
    session.flush()
    return upload


def create_case_from_uploads(
    session: Session,
    *,
    storage: LocalObjectStorage,
    ai_service: AIService,
    tenant_id,
    user_id,
    case_name: str,
    client_name: str | None,
    pdf_file_name: str,
    pdf_media_type: str,
    pdf_payload: bytes,
    questionnaire_file_name: str | None = None,
    questionnaire_media_type: str | None = None,
    questionnaire_payload: bytes | None = None,
    settings: Settings | None = None,
    pipeline_profile_name: str | None = None,
    pipeline_override: dict[str, object] | None = None,
    reproducibility_mode: ReproducibilityMode = ReproducibilityMode.BEST_EFFORT,
) -> RfxCase:
    if not case_name.strip():
        raise ValidationFailure("Case name cannot be empty.")
    pipeline = resolve_pipeline_selection(
        settings or get_settings(),
        profile_name=pipeline_profile_name,
        override=pipeline_override,
    )
    pdf_document = extract_pdf(pdf_payload)
    language, _ = infer_language(" ".join(page.text for page in pdf_document.pages))
    case = RfxCase(
        tenant_id=tenant_id,
        created_by_user_id=user_id,
        name=case_name.strip(),
        client_name=client_name.strip() if client_name else None,
        language=language,
        pipeline_profile_name=pipeline.profile_name,
        pipeline_config_json=pipeline.resolved_config,
        pipeline_config_hash=pipeline.config_hash,
        index_config_hash=pipeline.index_config_hash,
        status=CaseStatus.ACTIVE,
    )
    session.add(case)
    session.flush()
    source_manifest = get_or_create_source_manifest(
        session,
        tenant_id=tenant_id,
        case_id=case.id,
        kind=SourceManifestKind.LIVE_CASE_INPUT,
        manifest_json=live_case_input_manifest(
            case_name=case_name.strip(),
            client_name=client_name.strip() if client_name else None,
            pdf_file_name=pdf_file_name,
            pdf_media_type=pdf_media_type,
            pdf_payload=pdf_payload,
            questionnaire_file_name=questionnaire_file_name,
            questionnaire_media_type=questionnaire_media_type,
            questionnaire_payload=questionnaire_payload,
            pipeline_config_hash=pipeline.config_hash,
            index_config_hash=pipeline.index_config_hash,
        ),
    )
    repro = start_repro_run(
        session,
        storage=storage,
        settings=settings or get_settings(),
        kind=ExecutionRunKind.LIVE_CASE_CREATE,
        mode=reproducibility_mode,
        tenant_id=tenant_id,
        case_id=case.id,
        user_id=user_id,
        source_manifest=source_manifest,
        pipeline_profile_name=pipeline.profile_name,
        pipeline_config_hash=pipeline.config_hash,
        index_config_hash=pipeline.index_config_hash,
        runtime_config_hash=pipeline.runtime_config_hash,
        inputs_json={
            "case_name": case_name.strip(),
            "client_name": client_name.strip() if client_name else None,
            "pdf_file_name": pdf_file_name,
            "questionnaire_file_name": questionnaire_file_name,
        },
    )
    case.creation_run_id = repro.execution_run.id

    pdf_upload = _persist_upload(
        session,
        storage=storage,
        tenant_id=tenant_id,
        case_id=case.id,
        kind=UploadKind.CASE_PDF,
        file_name=pdf_file_name,
        media_type=pdf_media_type,
        payload=pdf_payload,
    )
    pages = [
        PdfPage(
            tenant_id=tenant_id,
            case_id=case.id,
            upload_id=pdf_upload.id,
            page_number=page.page_number,
            extracted_text=page.text,
            text_hash=page.text_hash,
        )
        for page in pdf_document.pages
    ]
    session.add_all(pages)
    session.flush()
    parser_version = importlib.metadata.version("pypdf")
    current_pdf_build = create_artifact_build(
        session,
        kind=ArtifactBuildKind.CURRENT_PDF,
        repo_snapshot=repro.repo_snapshot,
        runtime_snapshot=repro.runtime_snapshot,
        created_by_run=repro.execution_run,
        compatibility_hash=artifact_index_hashes(pipeline).current_pdf,
        index_config_hash=pipeline.index_config_hash,
        tenant_id=tenant_id,
        case_id=case.id,
        source_manifest=source_manifest,
        pipeline_profile_name=pipeline.profile_name,
        algorithm_version=current_pdf_chunking_version(pipeline),
        tokenizer_identity=(
            pipeline.resolved_pipeline.indexing.embedding_model
            if pipeline.resolved_pipeline.indexing.current_pdf.chunk_unit == "token"
            else None
        ),
        tokenizer_version=(
            importlib.metadata.version("tiktoken")
            if pipeline.resolved_pipeline.indexing.current_pdf.chunk_unit == "token"
            else None
        ),
        parser_identity="pypdf",
        parser_version=parser_version,
        embedding_model=pipeline.resolved_pipeline.indexing.embedding_model,
    )
    case_profile_build = create_artifact_build(
        session,
        kind=ArtifactBuildKind.CASE_PROFILE,
        repo_snapshot=repro.repo_snapshot,
        runtime_snapshot=repro.runtime_snapshot,
        created_by_run=repro.execution_run,
        compatibility_hash=artifact_index_hashes(pipeline).case_profile,
        index_config_hash=pipeline.index_config_hash,
        tenant_id=tenant_id,
        case_id=case.id,
        source_manifest=source_manifest,
        pipeline_profile_name=pipeline.profile_name,
        algorithm_version=CASE_PROFILE_SCHEMA_VERSION,
        parser_identity="pypdf",
        parser_version=parser_version,
        embedding_model=pipeline.resolved_pipeline.indexing.embedding_model,
        metadata_json={
            "schema_version": CASE_PROFILE_SCHEMA_VERSION,
            "prompt_set_version": CASE_PROFILE_PROMPT_SET_VERSION,
            "extraction_stage_model": pipeline.resolved_pipeline.models.case_profile_extraction.model_id,
        },
    )
    case.current_pdf_build_id = current_pdf_build.id
    case.case_profile_build_id = case_profile_build.id
    _persist_case_index_artifacts(
        session,
        storage=storage,
        ai_service=ai_service,
        pipeline=pipeline,
        case=case,
        upload=pdf_upload,
        pages=pages,
        current_pdf_build=current_pdf_build,
        case_profile_build=case_profile_build,
        repro_context=repro,
    )

    if questionnaire_payload is not None:
        if questionnaire_file_name is None or questionnaire_media_type is None:
            raise ValidationFailure("Questionnaire payload is missing file metadata.")
        parsed = parse_workbook_bytes(
            questionnaire_payload,
            source_file_name=questionnaire_file_name,
            schema_version=QUESTIONNAIRE_SCHEMA_VERSION,
            allow_empty_answer=True,
        )
        questionnaire_upload = _persist_upload(
            session,
            storage=storage,
            tenant_id=tenant_id,
            case_id=case.id,
            kind=UploadKind.QUESTIONNAIRE_XLSX,
            file_name=questionnaire_file_name,
            media_type=questionnaire_media_type,
            payload=questionnaire_payload,
        )
        questionnaire = Questionnaire(
            tenant_id=tenant_id,
            case_id=case.id,
            upload_id=questionnaire_upload.id,
            source_file_name=parsed.source_file_name,
            source_sheet_name=parsed.source_sheet_name,
            file_hash=parsed.file_hash,
            schema_version=parsed.schema_version,
        )
        session.add(questionnaire)
        session.flush()
        for row in parsed.rows:
            session.add(
                QuestionnaireRow(
                    tenant_id=tenant_id,
                    case_id=case.id,
                    questionnaire_id=questionnaire.id,
                    source_sheet_name=parsed.source_sheet_name,
                    source_row_number=row.source_row_number,
                    source_row_id=row.source_row_id,
                    context_raw=row.context,
                    question_raw=row.question,
                    answer_raw=row.answer,
                    normalized_text=f"{row.context}\n{row.question}\n{row.answer}",
                )
            )
        session.flush()

    finish_execution_run(
        repro.execution_run,
        outputs_json={
            "case_id": str(case.id),
            "current_pdf_build_id": str(case.current_pdf_build_id) if case.current_pdf_build_id else None,
            "case_profile_build_id": str(case.case_profile_build_id) if case.case_profile_build_id else None,
        },
    )
    if reproducibility_mode == ReproducibilityMode.STRICT_EVAL:
        assert_execution_run_consistency(session, run=repro.execution_run)
    return case


def _persist_case_index_artifacts(
    session: Session,
    *,
    storage: LocalObjectStorage,
    ai_service: AIService,
    pipeline: PipelineSelection,
    case: RfxCase,
    upload: Upload,
    pages: list[PdfPage],
    current_pdf_build: ArtifactBuild,
    case_profile_build: ArtifactBuild,
    repro_context,
) -> None:
    persist_pdf_chunks(
        session,
        ai_service=ai_service,
        pipeline=pipeline,
        tenant_id=case.tenant_id,
        case_id=case.id,
        upload_id=upload.id,
        pages=pages,
        case_name=case.name,
        client_name=case.client_name,
        language=case.language,
        source_file_name=upload.original_file_name,
        artifact_build_id=current_pdf_build.id,
        repro_context=repro_context,
        storage=storage,
    )
    persist_case_profile(
        session,
        ai_service=ai_service,
        pipeline=pipeline,
        case=case,
        upload=upload,
        pdf_pages=pages,
        artifact_build_id=case_profile_build.id,
        repro_context=repro_context,
        storage=storage,
    )


def _load_upload_payload(
    *,
    storage: LocalObjectStorage,
    upload: Upload,
) -> bytes:
    payload = upload.payload
    if payload is None:
        payload = storage.read_bytes(upload.object_key)
    if sha256_hex(payload) != upload.file_hash:
        raise ValidationFailure(
            f"Upload {upload.id} payload hash does not match persisted provenance."
        )
    return payload


def _require_case_pdf_upload(
    session: Session,
    *,
    case: RfxCase,
    pdf_upload_id,
) -> Upload:
    if pdf_upload_id is not None:
        upload = session.get(Upload, pdf_upload_id)
        if upload is None or upload.case_id != case.id or upload.kind != UploadKind.CASE_PDF:
            raise ValidationFailure(
                f"Upload {pdf_upload_id} is not a case PDF for case {case.id}."
            )
        return upload
    uploads = session.scalars(
        select(Upload)
        .where(Upload.case_id == case.id, Upload.kind == UploadKind.CASE_PDF)
        .order_by(Upload.created_at.asc())
    ).all()
    if not uploads:
        raise ValidationFailure(f"Case {case.id} has no source PDF upload for rebuild.")
    if len(uploads) != 1:
        raise ValidationFailure(
            f"Case {case.id} has {len(uploads)} case PDF uploads. Pass an explicit pdf_upload_id for rebuild."
        )
    return uploads[0]


def _assert_case_index_rebuild_provenance(
    session: Session,
    *,
    case: RfxCase,
    upload: Upload,
) -> None:
    mismatched_page = session.scalar(
        select(PdfPage.id).where(
            PdfPage.case_id == case.id,
            PdfPage.upload_id != upload.id,
        )
    )
    if mismatched_page is not None:
        raise ValidationFailure(
            f"Case {case.id} contains PDF pages from another upload; rebuild requires explicit repair first."
        )
    mismatched_chunk = session.scalar(
        select(PdfChunk.id).where(
            PdfChunk.case_id == case.id,
            PdfChunk.upload_id != upload.id,
        )
    )
    if mismatched_chunk is not None:
        raise ValidationFailure(
            f"Case {case.id} contains PDF chunks from another upload; rebuild requires explicit repair first."
        )
    mismatched_profile = session.scalar(
        select(CaseProfile.id).where(
            CaseProfile.case_id == case.id,
            CaseProfile.source_pdf_upload_id != upload.id,
        )
    )
    if mismatched_profile is not None:
        raise ValidationFailure(
            f"Case {case.id} contains case-profile artifacts from another upload; rebuild requires explicit repair first."
        )


def rebuild_case_index_artifacts(
    session: Session,
    *,
    storage: LocalObjectStorage,
    ai_service: AIService,
    case: RfxCase,
    settings: Settings | None = None,
    pipeline_profile_name: str | None = None,
    pipeline_override: dict[str, object] | None = None,
    pdf_upload_id=None,
    reproducibility_mode: ReproducibilityMode = ReproducibilityMode.BEST_EFFORT,
) -> RfxCase:
    if case.status != CaseStatus.ACTIVE:
        raise ValidationFailure(
            f"Case {case.id} with status {case.status.value} cannot rebuild index artifacts."
        )
    pipeline = resolve_pipeline_selection(
        settings or get_settings(),
        profile_name=pipeline_profile_name,
        override=pipeline_override,
        pinned_config=case.pipeline_config_json,
        pinned_profile_name=case.pipeline_profile_name,
    )
    upload = _require_case_pdf_upload(session, case=case, pdf_upload_id=pdf_upload_id)
    _assert_case_index_rebuild_provenance(session, case=case, upload=upload)
    payload = _load_upload_payload(storage=storage, upload=upload)
    source_manifest = get_or_create_source_manifest(
        session,
        tenant_id=case.tenant_id,
        case_id=case.id,
        kind=SourceManifestKind.LIVE_CASE_INPUT,
        manifest_json=live_case_input_manifest(
            case_name=case.name,
            client_name=case.client_name,
            pdf_file_name=upload.original_file_name,
            pdf_media_type=upload.media_type,
            pdf_payload=payload,
            questionnaire_file_name=None,
            questionnaire_media_type=None,
            questionnaire_payload=None,
            pipeline_config_hash=pipeline.config_hash,
            index_config_hash=pipeline.index_config_hash,
        ),
    )
    repro = start_repro_run(
        session,
        storage=storage,
        settings=settings or get_settings(),
        kind=ExecutionRunKind.LIVE_CASE_REBUILD,
        mode=reproducibility_mode,
        tenant_id=case.tenant_id,
        case_id=case.id,
        user_id=case.created_by_user_id,
        source_manifest=source_manifest,
        pipeline_profile_name=pipeline.profile_name,
        pipeline_config_hash=pipeline.config_hash,
        index_config_hash=pipeline.index_config_hash,
        runtime_config_hash=pipeline.runtime_config_hash,
        inputs_json={
            "pdf_upload_id": str(upload.id),
            "pdf_file_name": upload.original_file_name,
        },
    )
    pdf_document = extract_pdf(payload)
    pages = [
        PdfPage(
            tenant_id=case.tenant_id,
            case_id=case.id,
            upload_id=upload.id,
            page_number=page.page_number,
            extracted_text=page.text,
            text_hash=page.text_hash,
        )
        for page in pdf_document.pages
    ]
    language, _ = infer_language(" ".join(page.text for page in pdf_document.pages))
    case.language = language
    case.pipeline_profile_name = pipeline.profile_name
    case.pipeline_config_json = pipeline.resolved_config
    case.pipeline_config_hash = pipeline.config_hash
    case.index_config_hash = pipeline.index_config_hash
    parser_version = importlib.metadata.version("pypdf")
    replaced_current_pdf_build = (
        session.get(ArtifactBuild, case.current_pdf_build_id) if case.current_pdf_build_id else None
    )
    replaced_case_profile_build = (
        session.get(ArtifactBuild, case.case_profile_build_id) if case.case_profile_build_id else None
    )
    current_pdf_build = create_artifact_build(
        session,
        kind=ArtifactBuildKind.CURRENT_PDF,
        repo_snapshot=repro.repo_snapshot,
        runtime_snapshot=repro.runtime_snapshot,
        created_by_run=repro.execution_run,
        compatibility_hash=artifact_index_hashes(pipeline).current_pdf,
        index_config_hash=pipeline.index_config_hash,
        tenant_id=case.tenant_id,
        case_id=case.id,
        source_manifest=source_manifest,
        pipeline_profile_name=pipeline.profile_name,
        algorithm_version=current_pdf_chunking_version(pipeline),
        tokenizer_identity=(
            pipeline.resolved_pipeline.indexing.embedding_model
            if pipeline.resolved_pipeline.indexing.current_pdf.chunk_unit == "token"
            else None
        ),
        tokenizer_version=(
            importlib.metadata.version("tiktoken")
            if pipeline.resolved_pipeline.indexing.current_pdf.chunk_unit == "token"
            else None
        ),
        parser_identity="pypdf",
        parser_version=parser_version,
        embedding_model=pipeline.resolved_pipeline.indexing.embedding_model,
        replaced_build=replaced_current_pdf_build,
    )
    case_profile_build = create_artifact_build(
        session,
        kind=ArtifactBuildKind.CASE_PROFILE,
        repo_snapshot=repro.repo_snapshot,
        runtime_snapshot=repro.runtime_snapshot,
        created_by_run=repro.execution_run,
        compatibility_hash=artifact_index_hashes(pipeline).case_profile,
        index_config_hash=pipeline.index_config_hash,
        tenant_id=case.tenant_id,
        case_id=case.id,
        source_manifest=source_manifest,
        pipeline_profile_name=pipeline.profile_name,
        algorithm_version=CASE_PROFILE_SCHEMA_VERSION,
        parser_identity="pypdf",
        parser_version=parser_version,
        embedding_model=pipeline.resolved_pipeline.indexing.embedding_model,
        metadata_json={
            "schema_version": CASE_PROFILE_SCHEMA_VERSION,
            "prompt_set_version": CASE_PROFILE_PROMPT_SET_VERSION,
            "extraction_stage_model": pipeline.resolved_pipeline.models.case_profile_extraction.model_id,
        },
        replaced_build=replaced_case_profile_build,
    )
    case.current_pdf_build_id = current_pdf_build.id
    case.case_profile_build_id = case_profile_build.id

    session.execute(
        delete(CaseProfileItem).where(
            CaseProfileItem.case_profile_id.in_(
                select(CaseProfile.id).where(CaseProfile.case_id == case.id)
            )
        )
    )
    session.execute(delete(CaseProfile).where(CaseProfile.case_id == case.id))
    session.execute(delete(PdfChunk).where(PdfChunk.case_id == case.id))
    session.execute(delete(PdfPage).where(PdfPage.case_id == case.id))
    session.flush()

    session.add_all(pages)
    session.flush()
    _persist_case_index_artifacts(
        session,
        storage=storage,
        ai_service=ai_service,
        pipeline=pipeline,
        case=case,
        upload=upload,
        pages=pages,
        current_pdf_build=current_pdf_build,
        case_profile_build=case_profile_build,
        repro_context=repro,
    )
    finish_execution_run(
        repro.execution_run,
        outputs_json={
            "case_id": str(case.id),
            "current_pdf_build_id": str(current_pdf_build.id),
            "case_profile_build_id": str(case_profile_build.id),
        },
    )
    if reproducibility_mode == ReproducibilityMode.STRICT_EVAL:
        assert_execution_run_consistency(session, run=repro.execution_run)
    session.flush()
    return case


def require_case_scope(session: Session, *, case_id, tenant_id) -> RfxCase:
    case = session.get(RfxCase, case_id)
    if case is None:
        raise ValidationFailure(f"Case {case_id} does not exist.")
    if case.tenant_id != tenant_id:
        raise ScopeViolation(f"Case {case_id} does not belong to the active tenant.")
    return case


def require_row_scope(session: Session, *, row_id, case: RfxCase) -> QuestionnaireRow:
    row = session.get(QuestionnaireRow, row_id)
    if row is None:
        raise ValidationFailure(f"Questionnaire row {row_id} does not exist.")
    if row.case_id != case.id or row.tenant_id != case.tenant_id:
        raise ScopeViolation(f"Questionnaire row {row_id} is out of scope for case {case.id}.")
    return row
