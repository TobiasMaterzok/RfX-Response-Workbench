from __future__ import annotations

import importlib.metadata
import json
from pathlib import Path

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from app.config import Settings, get_settings
from app.exceptions import ValidationFailure
from app.models.entities import (
    ArtifactBuild,
    HistoricalCaseProfile,
    HistoricalCaseProfileItem,
    HistoricalClientPackage,
    HistoricalDataset,
    HistoricalQARow,
    HistoricalWorkbook,
    Upload,
)
from app.models.enums import (
    ApprovalStatus,
    ArtifactBuildKind,
    ExecutionRunKind,
    ReproducibilityMode,
    SourceManifestKind,
    UploadKind,
)
from app.pipeline.config import (
    artifact_index_hashes,
    historical_index_payload,
    resolve_pipeline_selection,
)
from app.prompts.case_profile import CASE_PROFILE_PROMPT_SET_VERSION, CASE_PROFILE_SCHEMA_VERSION
from app.services.ai import AIService
from app.services.case_profiles import persist_historical_case_profile
from app.services.object_keys import safe_object_key_filename
from app.services.pdfs import extract_pdf
from app.services.progress import (
    ProgressCallback,
    progress_interval,
    report_progress,
    should_report_progress,
)
from app.services.reproducibility import (
    assert_execution_run_consistency,
    create_artifact_build,
    embed_text_recorded,
    finish_execution_run,
    get_or_create_source_manifest,
    historical_import_manifest,
    start_repro_run,
)
from app.services.storage import LocalObjectStorage
from app.services.workbooks import HISTORICAL_SCHEMA_VERSION, infer_language, parse_workbook_bytes


def import_historical_corpus(
    session: Session,
    *,
    ai_service: AIService,
    storage: LocalObjectStorage,
    tenant_id,
    base_path: Path,
    settings: Settings | None = None,
    pipeline_profile_name: str | None = None,
    pipeline_override: dict[str, object] | None = None,
    reproducibility_mode: ReproducibilityMode = ReproducibilityMode.BEST_EFFORT,
    progress_callback: ProgressCallback | None = None,
) -> HistoricalDataset:
    pipeline = resolve_pipeline_selection(
        settings or get_settings(),
        profile_name=pipeline_profile_name,
        override=pipeline_override,
    )
    artifact_hashes = artifact_index_hashes(pipeline)
    manifest_path = base_path / "historical_corpus_manifest.json"
    if not manifest_path.exists():
        raise ValidationFailure(f"Historical corpus manifest is missing at {manifest_path}.")
    manifest_payload = manifest_path.read_bytes()
    manifest = json.loads(manifest_payload.decode("utf-8"))
    clients = list(manifest["clients"])
    total_clients = len(clients)
    response_model = pipeline.resolved_pipeline.models.case_profile_extraction.model_id
    embedding_model = pipeline.resolved_pipeline.indexing.embedding_model
    embedding_dimensions = pipeline.resolved_pipeline.indexing.embedding_dimensions
    report_progress(
        progress_callback,
        (
            "Historical corpus import started "
            f"clients={total_clients} response_model={response_model} "
            f"embedding_model={embedding_model} embedding_dimensions={embedding_dimensions}"
        ),
    )
    dataset_slug = "sample-historical-corpus"
    existing = session.scalar(
        select(HistoricalDataset).where(
            HistoricalDataset.tenant_id == tenant_id, HistoricalDataset.slug == dataset_slug
        )
    )
    replaced_build = session.get(ArtifactBuild, existing.artifact_build_id) if existing and existing.artifact_build_id else None
    source_manifest = get_or_create_source_manifest(
        session,
        tenant_id=tenant_id,
        case_id=None,
        kind=SourceManifestKind.HISTORICAL_IMPORT_SOURCE,
        manifest_json=historical_import_manifest(
            base_path=base_path,
            manifest_path=manifest_path,
            manifest_payload=manifest_payload,
            pipeline_config_hash=pipeline.config_hash,
            index_config_hash=pipeline.index_config_hash,
            clients=list(manifest["clients"]),
        ),
    )
    repro = start_repro_run(
        session,
        storage=storage,
        settings=settings or get_settings(),
        kind=(
            ExecutionRunKind.HISTORICAL_REIMPORT
            if existing is not None
            else ExecutionRunKind.HISTORICAL_IMPORT
        ),
        mode=reproducibility_mode,
        tenant_id=tenant_id,
        source_manifest=source_manifest,
        pipeline_profile_name=pipeline.profile_name,
        pipeline_config_hash=pipeline.config_hash,
        index_config_hash=pipeline.index_config_hash,
        runtime_config_hash=pipeline.runtime_config_hash,
        inputs_json={"base_path": str(base_path), "dataset_slug": dataset_slug},
    )
    if existing is not None:
        if replaced_build is not None:
            replaced_build.dataset_id = None
        session.execute(
            delete(HistoricalCaseProfileItem).where(
                HistoricalCaseProfileItem.historical_case_profile_id.in_(
                    select(HistoricalCaseProfile.id).where(
                        HistoricalCaseProfile.client_package_id.in_(
                            select(HistoricalClientPackage.id).where(
                                HistoricalClientPackage.dataset_id == existing.id
                            )
                        )
                    )
                )
            )
        )
        session.execute(delete(HistoricalQARow).where(HistoricalQARow.dataset_id == existing.id))
        session.execute(
            delete(HistoricalCaseProfile).where(
                HistoricalCaseProfile.client_package_id.in_(
                    select(HistoricalClientPackage.id).where(
                        HistoricalClientPackage.dataset_id == existing.id
                    )
                )
            )
        )
        session.execute(
            delete(HistoricalClientPackage).where(
                HistoricalClientPackage.dataset_id == existing.id
            )
        )
        workbook_upload_ids = session.scalars(
            select(HistoricalWorkbook.upload_id).where(
                HistoricalWorkbook.dataset_id == existing.id,
                HistoricalWorkbook.upload_id.is_not(None),
            )
        ).all()
        session.execute(
            delete(HistoricalWorkbook).where(HistoricalWorkbook.dataset_id == existing.id)
        )
        session.execute(
            delete(Upload).where(
                Upload.id.in_(workbook_upload_ids)
            )
        )
        session.delete(existing)
        session.flush()

    dataset = HistoricalDataset(
        tenant_id=tenant_id,
        name="Sample Historical Corpus",
        slug=dataset_slug,
        schema_version=HISTORICAL_SCHEMA_VERSION,
        approval_status=ApprovalStatus.APPROVED,
    )
    session.add(dataset)
    session.flush()
    build = create_artifact_build(
        session,
        kind=ArtifactBuildKind.HISTORICAL_CORPUS,
        repo_snapshot=repro.repo_snapshot,
        runtime_snapshot=repro.runtime_snapshot,
        created_by_run=repro.execution_run,
        compatibility_hash=artifact_hashes.historical,
        index_config_hash=pipeline.index_config_hash,
        tenant_id=tenant_id,
        dataset_id=dataset.id,
        source_manifest=source_manifest,
        pipeline_profile_name=pipeline.profile_name,
        algorithm_version="historical_corpus.v1",
        parser_identity="pypdf",
        parser_version=importlib.metadata.version("pypdf"),
        embedding_model=pipeline.resolved_pipeline.indexing.embedding_model,
        metadata_json={
            "historical_case_profile_schema_version": CASE_PROFILE_SCHEMA_VERSION,
            "historical_case_profile_prompt_set_version": CASE_PROFILE_PROMPT_SET_VERSION,
            "extraction_stage_model": pipeline.resolved_pipeline.models.case_profile_extraction.model_id,
        },
        replaced_build=replaced_build,
    )
    dataset.creation_run_id = repro.execution_run.id
    dataset.artifact_build_id = build.id

    completed_case_profile_llm_calls = 0
    completed_row_embeddings = 0
    for client_index, client in enumerate(clients, start=1):
        report_progress(
            progress_callback,
            f"Processing historical client {client_index}/{total_clients}: slug={client['slug']}",
        )
        workbook_name = str(client["deliverables"]["qa_xlsx"])
        pdf_name = str(client["deliverables"]["context_pdf"])
        workbook_path = base_path / workbook_name
        pdf_path = base_path / pdf_name
        if not workbook_path.exists():
            raise ValidationFailure(f"Expected workbook is missing: {workbook_path.name}.")
        if not pdf_path.exists():
            raise ValidationFailure(
                f"Historical client package {client['slug']} is missing its paired PDF {pdf_path.name}."
            )

        workbook_payload = workbook_path.read_bytes()
        parsed = parse_workbook_bytes(
            workbook_payload,
            source_file_name=workbook_path.name,
            schema_version=HISTORICAL_SCHEMA_VERSION,
            allow_empty_answer=False,
        )
        report_progress(
            progress_callback,
            (
                f"Parsed workbook for historical client {client_index}/{total_clients}: "
                f"slug={client['slug']} rows={len(parsed.rows)}"
            ),
        )
        workbook_stored = storage.save_bytes(
            object_key=(
                f"historical/{dataset.slug}/"
                f"{safe_object_key_filename(workbook_path.name, fallback_stem='historical-workbook')}"
            ),
            payload=workbook_payload,
        )
        workbook_upload = Upload(
            tenant_id=tenant_id,
            case_id=None,
            kind=UploadKind.HISTORICAL_WORKBOOK,
            original_file_name=workbook_path.name,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            object_key=workbook_stored.object_key,
            file_hash=workbook_stored.file_hash,
            size_bytes=workbook_stored.size_bytes,
            payload=workbook_payload,
        )
        session.add(workbook_upload)
        session.flush()
        workbook = HistoricalWorkbook(
            tenant_id=tenant_id,
            dataset_id=dataset.id,
            upload_id=workbook_upload.id,
            client_name=client["name"],
            language=client["language"],
            source_file_name=parsed.source_file_name,
            source_sheet_name=parsed.source_sheet_name,
            file_hash=parsed.file_hash,
            schema_version=parsed.schema_version,
            approval_status=ApprovalStatus.APPROVED,
        )
        session.add(workbook)
        session.flush()

        pdf_payload = pdf_path.read_bytes()
        extracted_pdf = extract_pdf(pdf_payload)
        report_progress(
            progress_callback,
            (
                f"Extracted PDF for historical client {client_index}/{total_clients}: "
                f"slug={client['slug']} pages={len(extracted_pdf.pages)}"
            ),
        )
        pdf_stored = storage.save_bytes(
            object_key=(
                f"historical/{dataset.slug}/"
                f"{safe_object_key_filename(pdf_path.name, fallback_stem='historical-pdf')}"
            ),
            payload=pdf_payload,
        )
        client_package = HistoricalClientPackage(
            tenant_id=tenant_id,
            dataset_id=dataset.id,
            workbook_id=workbook.id,
            client_slug=str(client["slug"]),
            client_name=str(client["name"]),
            language=str(client["language"]),
            source_pdf_file_name=pdf_path.name,
            source_pdf_file_hash=pdf_stored.file_hash,
            pdf_object_key=pdf_stored.object_key,
            pdf_size_bytes=pdf_stored.size_bytes,
            pipeline_profile_name=pipeline.profile_name,
            index_config_json=historical_index_payload(pipeline.resolved_pipeline),
            index_config_hash=artifact_hashes.historical,
        )
        session.add(client_package)
        session.flush()
        historical_profile = persist_historical_case_profile(
            session,
            ai_service=ai_service,
            pipeline=pipeline,
            client_package=client_package,
            page_text=[page.text for page in extracted_pdf.pages],
            repro_context=repro,
            storage=storage,
            progress_callback=progress_callback,
        )
        completed_case_profile_llm_calls += 1
        report_progress(
            progress_callback,
            (
                "Successful historical case-profile LLM calls "
                f"{completed_case_profile_llm_calls}/{total_clients}"
            ),
        )

        row_progress_every = progress_interval(len(parsed.rows))
        for row_index, row in enumerate(parsed.rows, start=1):
            language, confidence = infer_language(f"{row.context} {row.question} {row.answer}")
            retrieval_text = f"{row.context}\n{row.question}"
            session.add(
                HistoricalQARow(
                    tenant_id=tenant_id,
                    dataset_id=dataset.id,
                    workbook_id=workbook.id,
                    client_package_id=client_package.id,
                    historical_case_profile_id=historical_profile.id,
                    client_name=client["name"],
                    source_file_name=parsed.source_file_name,
                    source_sheet_name=parsed.source_sheet_name,
                    source_row_number=row.source_row_number,
                    source_row_id=row.source_row_id,
                    language=client["language"] or language,
                    language_confidence=1.0 if client.get("language") else confidence,
                    approval_status=ApprovalStatus.APPROVED,
                    context_raw=row.context,
                    question_raw=row.question,
                    answer_raw=row.answer,
                    normalized_text=retrieval_text,
                    file_hash=parsed.file_hash,
                    schema_version=parsed.schema_version,
                    embedding=embed_text_recorded(
                        session,
                        storage=storage,
                        execution_run=repro.execution_run,
                        ai_service=ai_service,
                        text=retrieval_text,
                        model_id=pipeline.resolved_pipeline.indexing.embedding_model,
                        dimensions=pipeline.resolved_pipeline.indexing.embedding_dimensions,
                        metadata_json={
                            "artifact_family": "historical_qa_row",
                            "source_row_id": row.source_row_id,
                        },
                    ),
                )
            )
            completed_row_embeddings += 1
            if should_report_progress(row_index, len(parsed.rows), every=row_progress_every):
                report_progress(
                    progress_callback,
                    (
                        f"Embedded historical questionnaire rows for client {client_index}/{total_clients} "
                        f"slug={client['slug']}: {row_index}/{len(parsed.rows)}"
                    ),
                )
        report_progress(
            progress_callback,
            (
                f"Completed historical client {client_index}/{total_clients}: "
                f"slug={client['slug']} rows={len(parsed.rows)}"
            ),
        )
    session.flush()
    report_progress(
        progress_callback,
        (
            "Historical corpus import complete "
            f"clients={total_clients} case_profile_llm_calls={completed_case_profile_llm_calls} "
            f"row_embeddings={completed_row_embeddings}"
        ),
    )
    finish_execution_run(
        repro.execution_run,
        outputs_json={"historical_dataset_id": str(dataset.id), "artifact_build_id": str(build.id)},
    )
    if reproducibility_mode == ReproducibilityMode.STRICT_EVAL:
        assert_execution_run_consistency(session, run=repro.execution_run)
    return dataset
