from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from app.config import Settings, get_settings
from app.exceptions import ValidationFailure
from app.models.entities import ArtifactBuild, ProductTruthChunk, ProductTruthRecord
from app.models.enums import (
    ApprovalStatus,
    ArtifactBuildKind,
    ArtifactBuildStatus,
    ExecutionRunKind,
    ReproducibilityMode,
    SourceManifestKind,
)
from app.pipeline.config import (
    artifact_index_hashes,
    product_truth_index_payload,
    resolve_pipeline_selection,
)
from app.services.ai import AIService
from app.services.hashing import sha256_text
from app.services.reproducibility import (
    assert_execution_run_consistency,
    create_artifact_build,
    embed_text_recorded,
    finish_execution_run,
    get_or_create_source_manifest,
    product_truth_import_manifest,
    start_repro_run,
)
from app.services.storage import LocalObjectStorage


@dataclass(frozen=True)
class ProductTruthInput:
    product_area: str
    title: str
    body: str
    language: str
    source_file_name: str
    source_section: str
    effective_from: date
    effective_to: date | None
    version: str


def load_product_truth_inputs(path: Path) -> list[ProductTruthInput]:
    if not path.exists():
        raise ValidationFailure(f"Product-truth source file is missing: {path}.")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValidationFailure("Product-truth source must be a JSON array of records.")
    records: list[ProductTruthInput] = []
    for index, item in enumerate(payload, start=1):
        if not isinstance(item, dict):
            raise ValidationFailure(f"Product-truth record {index} is not a JSON object.")
        try:
            records.append(
                ProductTruthInput(
                    product_area=str(item["product_area"]),
                    title=str(item["title"]),
                    body=str(item["body"]),
                    language=str(item["language"]),
                    source_file_name=str(item["source_file_name"]),
                    source_section=str(item["source_section"]),
                    effective_from=date.fromisoformat(str(item["effective_from"])),
                    effective_to=(
                        date.fromisoformat(str(item["effective_to"]))
                        if item.get("effective_to")
                        else None
                    ),
                    version=str(item["version"]),
                )
            )
        except KeyError as exc:
            raise ValidationFailure(
                f"Product-truth record {index} is missing required field {exc.args[0]!r}."
            ) from exc
    if not records:
        raise ValidationFailure("Product-truth source must contain at least one record.")
    return records


def ingest_product_truth_record(
    session: Session,
    *,
    ai_service: AIService,
    pipeline_profile_name: str | None,
    index_config_json: dict[str, object],
    index_config_hash: str,
    artifact_build_id,
    execution_run,
    storage: LocalObjectStorage | None,
    tenant_id,
    payload: ProductTruthInput,
) -> ProductTruthRecord:
    if not payload.body.strip():
        raise ValidationFailure("Product truth body cannot be empty.")
    record = ProductTruthRecord(
        tenant_id=tenant_id,
        product_area=payload.product_area,
        title=payload.title,
        body=payload.body,
        language=payload.language,
        source_file_name=payload.source_file_name,
        source_section=payload.source_section,
        effective_from=payload.effective_from,
        effective_to=payload.effective_to,
        version=payload.version,
        pipeline_profile_name=pipeline_profile_name,
        index_config_json=index_config_json,
        index_config_hash=index_config_hash,
        artifact_build_id=artifact_build_id,
        approval_status=ApprovalStatus.APPROVED,
        file_hash=sha256_text(payload.body),
    )
    session.add(record)
    session.flush()
    session.add(
        ProductTruthChunk(
            tenant_id=tenant_id,
            truth_record_id=record.id,
            chunk_index=1,
            content=payload.body,
            language=payload.language,
            file_hash=record.file_hash,
            approval_status=ApprovalStatus.APPROVED,
            embedding=(
                embed_text_recorded(
                    session,
                    storage=storage,
                    execution_run=execution_run,
                    ai_service=ai_service,
                    text=payload.body,
                    model_id=str(index_config_json["embedding_model"]),
                    metadata_json={
                        "artifact_family": "product_truth_chunk",
                        "truth_record_id": str(record.id),
                    },
                )
                if execution_run is not None and storage is not None
                else ai_service.embed_text(
                    payload.body,
                    model_id=str(index_config_json["embedding_model"]),
                )
            ),
        )
    )
    session.flush()
    return record


def ingest_product_truth_file(
    session: Session,
    *,
    storage: LocalObjectStorage | None = None,
    ai_service: AIService,
    tenant_id,
    path: Path,
    settings: Settings | None = None,
    pipeline_profile_name: str | None = None,
    pipeline_override: dict[str, object] | None = None,
    reproducibility_mode: ReproducibilityMode = ReproducibilityMode.BEST_EFFORT,
    run_kind: ExecutionRunKind = ExecutionRunKind.PRODUCT_TRUTH_IMPORT,
    replaced_build: ArtifactBuild | None = None,
) -> list[ProductTruthRecord]:
    payload = path.read_bytes()
    records = load_product_truth_inputs(path)
    return ingest_product_truth_inputs(
        session,
        storage=storage,
        ai_service=ai_service,
        tenant_id=tenant_id,
        records=records,
        settings=settings,
        pipeline_profile_name=pipeline_profile_name,
        pipeline_override=pipeline_override,
        reproducibility_mode=reproducibility_mode,
        run_kind=run_kind,
        replaced_build=replaced_build,
        source_manifest_json=product_truth_import_manifest(
            path=path,
            payload=payload,
            record_count=len(records),
            pipeline_config_hash=None,
            index_config_hash=None,
        ),
        run_inputs_json={"path": str(path), "record_count": len(records)},
    )


def ingest_product_truth_inputs(
    session: Session,
    *,
    storage: LocalObjectStorage | None,
    ai_service: AIService,
    tenant_id,
    records: list[ProductTruthInput],
    source_manifest_json: dict[str, object],
    run_inputs_json: dict[str, object],
    settings: Settings | None = None,
    pipeline_profile_name: str | None = None,
    pipeline_override: dict[str, object] | None = None,
    reproducibility_mode: ReproducibilityMode = ReproducibilityMode.BEST_EFFORT,
    run_kind: ExecutionRunKind = ExecutionRunKind.PRODUCT_TRUTH_IMPORT,
    replaced_build: ArtifactBuild | None = None,
) -> list[ProductTruthRecord]:
    if reproducibility_mode == ReproducibilityMode.STRICT_EVAL and run_kind == ExecutionRunKind.PRODUCT_TRUTH_IMPORT:
        existing_truth = session.scalar(
            select(ProductTruthRecord.id).where(
                ProductTruthRecord.tenant_id == tenant_id,
                ProductTruthRecord.approval_status == ApprovalStatus.APPROVED,
            )
        )
        if existing_truth is not None:
            raise ValidationFailure(
                "strict_eval forbids additive product-truth import when approved truth records already exist. Use reimport-product-truth instead."
            )
    pipeline = resolve_pipeline_selection(
        settings or get_settings(),
        profile_name=pipeline_profile_name,
        override=pipeline_override,
    )
    artifact_hashes = artifact_index_hashes(pipeline)
    source_manifest_json["pipeline_config_hash"] = pipeline.config_hash
    source_manifest_json["index_config_hash"] = pipeline.index_config_hash
    source_manifest = get_or_create_source_manifest(
        session,
        tenant_id=tenant_id,
        case_id=None,
        kind=SourceManifestKind.PRODUCT_TRUTH_IMPORT_SOURCE,
        manifest_json=source_manifest_json,
    )
    repro = start_repro_run(
        session,
        storage=storage,
        settings=settings or get_settings(),
        kind=run_kind,
        mode=reproducibility_mode,
        tenant_id=tenant_id,
        source_manifest=source_manifest,
        pipeline_profile_name=pipeline.profile_name,
        pipeline_config_hash=pipeline.config_hash,
        index_config_hash=pipeline.index_config_hash,
        runtime_config_hash=pipeline.runtime_config_hash,
        inputs_json=run_inputs_json,
    )
    build = create_artifact_build(
        session,
        kind=ArtifactBuildKind.PRODUCT_TRUTH_CORPUS,
        repo_snapshot=repro.repo_snapshot,
        runtime_snapshot=repro.runtime_snapshot,
        created_by_run=repro.execution_run,
        compatibility_hash=artifact_hashes.product_truth,
        index_config_hash=pipeline.index_config_hash,
        tenant_id=tenant_id,
        source_manifest=source_manifest,
        pipeline_profile_name=pipeline.profile_name,
        algorithm_version="product_truth_corpus.v1",
        embedding_model=pipeline.resolved_pipeline.indexing.embedding_model,
        replaced_build=replaced_build,
    )
    created_records = [
        ingest_product_truth_record(
            session,
            ai_service=ai_service,
            pipeline_profile_name=pipeline.profile_name,
            index_config_json=product_truth_index_payload(pipeline.resolved_pipeline),
            index_config_hash=artifact_hashes.product_truth,
            artifact_build_id=build.id,
            execution_run=repro.execution_run,
            storage=storage,
            tenant_id=tenant_id,
            payload=record,
        )
        for record in records
    ]
    finish_execution_run(
        repro.execution_run,
        outputs_json={"artifact_build_id": str(build.id), "record_count": len(created_records)},
    )
    if reproducibility_mode == ReproducibilityMode.STRICT_EVAL:
        assert_execution_run_consistency(session, run=repro.execution_run)
    return created_records


def reimport_product_truth_file(
    session: Session,
    *,
    storage: LocalObjectStorage | None = None,
    ai_service: AIService,
    tenant_id,
    path: Path,
    settings: Settings | None = None,
    pipeline_profile_name: str | None = None,
    pipeline_override: dict[str, object] | None = None,
    reproducibility_mode: ReproducibilityMode = ReproducibilityMode.BEST_EFFORT,
) -> list[ProductTruthRecord]:
    replaced_build = session.query(ArtifactBuild).filter(
        ArtifactBuild.kind == ArtifactBuildKind.PRODUCT_TRUTH_CORPUS,
        ArtifactBuild.status == ArtifactBuildStatus.ACTIVE,
        ArtifactBuild.tenant_id == tenant_id,
    ).order_by(ArtifactBuild.created_at.desc()).first()
    session.execute(delete(ProductTruthChunk).where(ProductTruthChunk.tenant_id == tenant_id))
    session.execute(delete(ProductTruthRecord).where(ProductTruthRecord.tenant_id == tenant_id))
    session.flush()
    return ingest_product_truth_file(
        session,
        storage=storage,
        ai_service=ai_service,
        tenant_id=tenant_id,
        path=path,
        settings=settings,
        pipeline_profile_name=pipeline_profile_name,
        pipeline_override=pipeline_override,
        reproducibility_mode=reproducibility_mode,
        run_kind=ExecutionRunKind.PRODUCT_TRUTH_REIMPORT,
        replaced_build=replaced_build,
    )
