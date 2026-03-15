from __future__ import annotations

import importlib.metadata
import json
import platform
import subprocess
from collections.abc import Sequence
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import Enum
from pathlib import Path
from typing import Any
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from app.config import Settings
from app.exceptions import ValidationFailure
from app.models.entities import (
    AnswerVersion,
    ArtifactBuild,
    BulkFillRequest,
    CaseProfile,
    ExecutionRun,
    ExportJob,
    HistoricalDataset,
    ModelInvocation,
    PdfChunk,
    ProductTruthRecord,
    RepoSnapshot,
    RetrievalRun,
    RfxCase,
    RuntimeSnapshot,
    SourceManifest,
    Upload,
)
from app.models.enums import (
    ArtifactBuildKind,
    ArtifactBuildStatus,
    ExecutionRunKind,
    ExecutionRunStatus,
    ExportMode,
    ModelInvocationKind,
    QuestionnaireRowStatus,
    ReproducibilityLevel,
    ReproducibilityMode,
    SourceManifestKind,
    UploadKind,
)
from app.services.export_selection import (
    APPROVED_ANSWER_SELECTION_KIND,
    STATUS_PLACEHOLDER_SELECTION_KIND,
    export_placeholder_text,
)
from app.services.hashing import sha256_hex, sha256_text
from app.services.storage import LocalObjectStorage

REPO_ROOT = Path(__file__).resolve().parents[3]
BACKEND_LOCKFILE = REPO_ROOT / "backend" / "requirements.lock.txt"
FRONTEND_LOCKFILE = REPO_ROOT / "frontend" / "package-lock.json"
RUNTIME_PACKAGES = [
    "alembic",
    "fastapi",
    "openai",
    "openpyxl",
    "pgvector",
    "pydantic",
    "pydantic-settings",
    "pypdf",
    "psycopg",
    "sqlalchemy",
    "tiktoken",
    "typer",
    "uvicorn",
]


def _json_safe(value: object) -> object:
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    return value


def canonical_json_bytes(payload: object) -> bytes:
    return json.dumps(
        _json_safe(payload),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")


def canonical_json_text(payload: object) -> str:
    return canonical_json_bytes(payload).decode("utf-8")


def _strict_required(mode: ReproducibilityMode, condition: bool, message: str) -> None:
    if mode == ReproducibilityMode.STRICT_EVAL and not condition:
        raise ValidationFailure(message)


def _git_command(*args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout


def _package_versions() -> dict[str, str]:
    versions: dict[str, str] = {}
    for package in RUNTIME_PACKAGES:
        with suppress(importlib.metadata.PackageNotFoundError):
            versions[package] = importlib.metadata.version(package)
    return versions


def _lock_hash(path: Path) -> tuple[str | None, str | None]:
    if not path.exists():
        return None, None
    payload = path.read_bytes()
    return sha256_hex(payload), str(path.relative_to(REPO_ROOT))


def _database_metadata(session: Session) -> tuple[str | None, str | None, str | None]:
    bind = session.get_bind()
    if bind is None:
        return None, None, None
    vendor = bind.dialect.name
    version: str | None = None
    pgvector_version: str | None = None
    if vendor == "sqlite":
        version = str(session.execute(text("select sqlite_version()")).scalar_one())
    elif vendor == "postgresql":
        version = str(session.execute(text("select version()")).scalar_one())
        pgvector_version = session.execute(
            text("select extversion from pg_extension where extname = 'vector'")
        ).scalar_one_or_none()
        if pgvector_version is not None:
            pgvector_version = str(pgvector_version)
    return vendor, version, pgvector_version


def _alembic_head(session: Session) -> str | None:
    try:
        value = session.execute(text("select version_num from alembic_version")).scalar_one()
    except Exception:
        return None
    return str(value)


def _env_fingerprint(settings: Settings) -> dict[str, object]:
    return {
        "llm_api_base_url": settings.llm_api_base_url,
        "llm_response_model": settings.openai_response_model,
        "llm_embedding_model": settings.openai_embedding_model,
        "llm_embedding_dimensions": settings.openai_embedding_dimensions,
        "openai_response_model": settings.openai_response_model,
        "openai_embedding_model": settings.openai_embedding_model,
        "openai_embedding_dimensions": settings.openai_embedding_dimensions,
        "storage_root": str(settings.storage_root.resolve()),
    }


def capture_repo_snapshot(
    session: Session,
    *,
    storage: LocalObjectStorage | None,
    mode: ReproducibilityMode,
) -> RepoSnapshot:
    try:
        git_commit_sha = _git_command("rev-parse", "HEAD").strip()
        dirty_status = _git_command("status", "--porcelain")
        git_dirty = bool(dirty_status.strip())
        git_diff_text = _git_command("diff", "--binary", "HEAD") if git_dirty else None
    except Exception as exc:
        _strict_required(
            mode,
            False,
            f"Strict-eval repo snapshot capture failed: {exc}",
        )
        git_commit_sha = "capture_failed"
        git_dirty = True
        git_diff_text = f"repo_snapshot_capture_failed:{exc}"
    git_diff_hash = sha256_text(git_diff_text) if git_diff_text else None
    snapshot_payload = {
        "git_commit_sha": git_commit_sha,
        "git_dirty": git_dirty,
        "git_diff_hash": git_diff_hash,
    }
    snapshot_hash = sha256_text(canonical_json_text(snapshot_payload))
    existing = session.query(RepoSnapshot).filter(RepoSnapshot.snapshot_hash == snapshot_hash).one_or_none()
    if existing is not None:
        return existing
    snapshot = RepoSnapshot(
        git_commit_sha=git_commit_sha,
        git_dirty=git_dirty,
        git_diff_hash=git_diff_hash,
        git_diff_text=git_diff_text,
        snapshot_hash=snapshot_hash,
    )
    session.add(snapshot)
    session.flush()
    return snapshot


def capture_runtime_snapshot(
    session: Session,
    *,
    settings: Settings,
    mode: ReproducibilityMode,
) -> RuntimeSnapshot:
    backend_lock_hash, backend_lock_file = _lock_hash(BACKEND_LOCKFILE)
    frontend_lock_hash, frontend_lock_file = _lock_hash(FRONTEND_LOCKFILE)
    _strict_required(
        mode,
        backend_lock_hash is not None,
        f"Strict-eval requires a committed backend lockfile at {BACKEND_LOCKFILE}.",
    )
    vendor, version, pgvector_version = _database_metadata(session)
    alembic_head = _alembic_head(session)
    _strict_required(mode, alembic_head is not None, "Strict-eval requires alembic head capture.")
    package_versions = _package_versions()
    env_fingerprint_json = _env_fingerprint(settings)
    env_fingerprint_hash = sha256_text(canonical_json_text(env_fingerprint_json))
    snapshot_payload = {
        "python_version": platform.python_version(),
        "backend_lock_hash": backend_lock_hash,
        "frontend_lock_hash": frontend_lock_hash,
        "alembic_head": alembic_head,
        "db_vendor": vendor,
        "db_version": version,
        "pgvector_version": pgvector_version,
        "os_name": platform.system(),
        "os_arch": platform.machine(),
        "package_versions": package_versions,
        "env_fingerprint_hash": env_fingerprint_hash,
    }
    snapshot_hash = sha256_text(canonical_json_text(snapshot_payload))
    existing = session.query(RuntimeSnapshot).filter(RuntimeSnapshot.snapshot_hash == snapshot_hash).one_or_none()
    if existing is not None:
        return existing
    snapshot = RuntimeSnapshot(
        python_version=platform.python_version(),
        backend_lock_hash=backend_lock_hash,
        backend_lock_file=backend_lock_file,
        frontend_lock_hash=frontend_lock_hash,
        frontend_lock_file=frontend_lock_file,
        alembic_head=alembic_head,
        db_vendor=vendor,
        db_version=version,
        pgvector_version=pgvector_version,
        os_name=platform.system(),
        os_arch=platform.machine(),
        package_versions_json=package_versions,
        env_fingerprint_hash=env_fingerprint_hash,
        env_fingerprint_json=env_fingerprint_json,
        snapshot_hash=snapshot_hash,
    )
    session.add(snapshot)
    session.flush()
    return snapshot


def get_or_create_source_manifest(
    session: Session,
    *,
    tenant_id,
    case_id,
    kind: SourceManifestKind,
    manifest_json: dict[str, object],
) -> SourceManifest:
    manifest_hash = sha256_text(canonical_json_text(manifest_json))
    existing = session.query(SourceManifest).filter(SourceManifest.manifest_hash == manifest_hash).one_or_none()
    if existing is not None:
        return existing
    manifest = SourceManifest(
        tenant_id=tenant_id,
        case_id=case_id,
        kind=kind,
        manifest_hash=manifest_hash,
        manifest_json=manifest_json,
    )
    session.add(manifest)
    session.flush()
    return manifest


def live_case_input_manifest(
    *,
    case_name: str,
    client_name: str | None,
    pdf_file_name: str,
    pdf_media_type: str,
    pdf_payload: bytes,
    questionnaire_file_name: str | None,
    questionnaire_media_type: str | None,
    questionnaire_payload: bytes | None,
    pipeline_config_hash: str | None,
    index_config_hash: str | None,
) -> dict[str, object]:
    files: list[dict[str, object]] = [
        {
            "role": "case_pdf",
            "logical_name": pdf_file_name,
            "media_type": pdf_media_type,
            "size_bytes": len(pdf_payload),
            "file_hash": sha256_hex(pdf_payload),
        }
    ]
    manifest: dict[str, object] = {
        "source_kind": SourceManifestKind.LIVE_CASE_INPUT.value,
        "case_name": case_name,
        "client_name": client_name,
        "files": files,
        "pipeline_config_hash": pipeline_config_hash,
        "index_config_hash": index_config_hash,
    }
    if questionnaire_payload is not None:
        files.append(
            {
                "role": "questionnaire_xlsx",
                "logical_name": questionnaire_file_name,
                "media_type": questionnaire_media_type,
                "size_bytes": len(questionnaire_payload),
                "file_hash": sha256_hex(questionnaire_payload),
            }
        )
    return manifest


def historical_import_manifest(
    *,
    base_path: Path,
    manifest_path: Path,
    manifest_payload: bytes,
    pipeline_config_hash: str | None,
    index_config_hash: str | None,
    clients: list[dict[str, Any]],
) -> dict[str, object]:
    files: list[dict[str, object]] = [
        {
            "role": "dataset_manifest",
            "logical_name": str(manifest_path.relative_to(base_path)),
            "size_bytes": len(manifest_payload),
            "file_hash": sha256_hex(manifest_payload),
        }
    ]
    for client in clients:
        workbook_path = base_path / str(client["deliverables"]["qa_xlsx"])
        pdf_path = base_path / str(client["deliverables"]["context_pdf"])
        workbook_payload = workbook_path.read_bytes()
        pdf_payload = pdf_path.read_bytes()
        files.extend(
            [
                {
                    "role": "historical_workbook",
                    "client_slug": client["slug"],
                    "logical_name": str(workbook_path.relative_to(base_path)),
                    "size_bytes": len(workbook_payload),
                    "file_hash": sha256_hex(workbook_payload),
                },
                {
                    "role": "historical_pdf",
                    "client_slug": client["slug"],
                    "logical_name": str(pdf_path.relative_to(base_path)),
                    "size_bytes": len(pdf_payload),
                    "file_hash": sha256_hex(pdf_payload),
                },
            ]
        )
    return {
        "source_kind": SourceManifestKind.HISTORICAL_IMPORT_SOURCE.value,
        "base_path": str(base_path),
        "manifest_hash": sha256_hex(manifest_payload),
        "file_count": len(files),
        "files": files,
        "pipeline_config_hash": pipeline_config_hash,
        "index_config_hash": index_config_hash,
    }


def product_truth_import_manifest(
    *,
    path: Path,
    payload: bytes,
    record_count: int,
    pipeline_config_hash: str | None,
    index_config_hash: str | None,
) -> dict[str, object]:
    return {
        "source_kind": SourceManifestKind.PRODUCT_TRUTH_IMPORT_SOURCE.value,
        "path": str(path),
        "record_count": record_count,
        "files": [
            {
                "role": "product_truth_source_json",
                "logical_name": path.name,
                "size_bytes": len(payload),
                "file_hash": sha256_hex(payload),
            }
        ],
        "pipeline_config_hash": pipeline_config_hash,
        "index_config_hash": index_config_hash,
    }


def product_truth_inline_manifest(
    *,
    records: list[dict[str, object]],
    pipeline_config_hash: str | None,
    index_config_hash: str | None,
) -> dict[str, object]:
    payload = canonical_json_bytes(records)
    return {
        "source_kind": SourceManifestKind.PRODUCT_TRUTH_IMPORT_SOURCE.value,
        "path": None,
        "record_count": len(records),
        "files": [
            {
                "role": "product_truth_inline_payload",
                "logical_name": "api_inline_product_truth",
                "size_bytes": len(payload),
                "file_hash": sha256_hex(payload),
            }
        ],
        "pipeline_config_hash": pipeline_config_hash,
        "index_config_hash": index_config_hash,
    }


def export_input_manifest(
    *,
    questionnaire_id: UUID,
    source_upload_id: UUID,
    source_upload_hash: str,
    export_mode: str,
    row_selection: list[dict[str, object]],
) -> dict[str, object]:
    return {
        "source_kind": SourceManifestKind.EXPORT_INPUT.value,
        "questionnaire_id": str(questionnaire_id),
        "source_upload_id": str(source_upload_id),
        "source_upload_hash": source_upload_hash,
        "export_mode": export_mode,
        "row_selection": row_selection,
    }


def create_execution_run(
    session: Session,
    *,
    kind: ExecutionRunKind,
    status: ExecutionRunStatus,
    mode: ReproducibilityMode,
    repo_snapshot: RepoSnapshot,
    runtime_snapshot: RuntimeSnapshot,
    tenant_id=None,
    case_id=None,
    user_id=None,
    parent_run_id=None,
    source_manifest: SourceManifest | None = None,
    pipeline_profile_name: str | None = None,
    pipeline_config_hash: str | None = None,
    index_config_hash: str | None = None,
    runtime_config_hash: str | None = None,
    inputs_json: dict[str, object] | None = None,
    external_context_json: dict[str, object] | None = None,
) -> ExecutionRun:
    if kind == ExecutionRunKind.BULK_FILL_JOB:
        level = ReproducibilityLevel.AUDIT_COMPLETE
    elif mode == ReproducibilityMode.STRICT_EVAL and kind == ExecutionRunKind.EXPORT:
        level = ReproducibilityLevel.DETERMINISTIC_NON_LLM
    elif mode == ReproducibilityMode.STRICT_EVAL:
        level = ReproducibilityLevel.OPERATIONALLY_REPLAYABLE
    else:
        level = ReproducibilityLevel.AUDIT_COMPLETE
    run = ExecutionRun(
        tenant_id=tenant_id,
        case_id=case_id,
        user_id=user_id,
        parent_run_id=parent_run_id,
        repo_snapshot_id=repo_snapshot.id,
        runtime_snapshot_id=runtime_snapshot.id,
        source_manifest_id=source_manifest.id if source_manifest else None,
        kind=kind,
        status=status,
        reproducibility_level=level,
        reproducibility_mode=mode,
        pipeline_profile_name=pipeline_profile_name,
        pipeline_config_hash=pipeline_config_hash,
        index_config_hash=index_config_hash,
        runtime_config_hash=runtime_config_hash,
        inputs_json=inputs_json or {},
        outputs_json={},
        replay_json=None,
        external_context_json=external_context_json or {},
        diagnostics_json={},
        started_at=datetime.now(UTC),
        finished_at=None,
        error_detail=None,
    )
    session.add(run)
    session.flush()
    return run


def finish_execution_run(
    run: ExecutionRun,
    *,
    outputs_json: dict[str, object] | None = None,
    replay_json: dict[str, object] | None = None,
    diagnostics_json: dict[str, object] | None = None,
) -> None:
    run.status = ExecutionRunStatus.COMPLETED
    run.finished_at = datetime.now(UTC)
    if outputs_json is not None:
        run.outputs_json = outputs_json
    if replay_json is not None:
        run.replay_json = replay_json
    if diagnostics_json is not None:
        run.diagnostics_json = diagnostics_json


def fail_execution_run(
    run: ExecutionRun,
    *,
    error_detail: str,
    diagnostics_json: dict[str, object] | None = None,
) -> None:
    run.status = ExecutionRunStatus.FAILED
    run.finished_at = datetime.now(UTC)
    run.error_detail = error_detail
    if diagnostics_json is not None:
        run.diagnostics_json = diagnostics_json


def create_artifact_build(
    session: Session,
    *,
    kind: ArtifactBuildKind,
    repo_snapshot: RepoSnapshot,
    runtime_snapshot: RuntimeSnapshot,
    created_by_run: ExecutionRun,
    compatibility_hash: str,
    index_config_hash: str | None,
    tenant_id=None,
    case_id=None,
    dataset_id=None,
    source_manifest: SourceManifest | None = None,
    pipeline_profile_name: str | None = None,
    algorithm_version: str | None = None,
    tokenizer_identity: str | None = None,
    tokenizer_version: str | None = None,
    parser_identity: str | None = None,
    parser_version: str | None = None,
    embedding_model: str | None = None,
    metadata_json: dict[str, object] | None = None,
    replaced_build: ArtifactBuild | None = None,
) -> ArtifactBuild:
    if replaced_build is not None:
        replaced_build.status = ArtifactBuildStatus.REPLACED
    build = ArtifactBuild(
        tenant_id=tenant_id,
        case_id=case_id,
        dataset_id=dataset_id,
        repo_snapshot_id=repo_snapshot.id,
        runtime_snapshot_id=runtime_snapshot.id,
        source_manifest_id=source_manifest.id if source_manifest else None,
        created_by_run_id=created_by_run.id,
        replaced_build_id=replaced_build.id if replaced_build else None,
        kind=kind,
        status=ArtifactBuildStatus.ACTIVE,
        pipeline_profile_name=pipeline_profile_name,
        index_config_hash=index_config_hash,
        compatibility_hash=compatibility_hash,
        algorithm_version=algorithm_version,
        tokenizer_identity=tokenizer_identity,
        tokenizer_version=tokenizer_version,
        parser_identity=parser_identity,
        parser_version=parser_version,
        embedding_model=embedding_model,
        metadata_json=metadata_json or {},
    )
    session.add(build)
    session.flush()
    return build


def record_model_invocation(
    session: Session,
    *,
    storage: LocalObjectStorage | None,
    execution_run: ExecutionRun,
    provider_name: str,
    endpoint_kind: str,
    kind: ModelInvocationKind,
    requested_model_id: str | None,
    actual_model_id: str | None,
    reasoning_effort: str | None,
    temperature: float | None,
    embedding_model_id: str | None,
    tokenizer_identity: str | None,
    tokenizer_version: str | None,
    request_payload: str | dict[str, object] | Sequence[object],
    response_payload: str | dict[str, object] | Sequence[object] | None,
    provider_response_id: str | None,
    sdk_version: str | None,
    service_tier: str | None = None,
    usage_json: dict[str, object] | None = None,
    metadata_json: dict[str, object] | None = None,
) -> ModelInvocation:
    request_text = (
        request_payload
        if isinstance(request_payload, str)
        else canonical_json_text(request_payload)
    )
    response_text = (
        response_payload
        if isinstance(response_payload, str)
        else canonical_json_text(response_payload)
        if response_payload is not None
        else None
    )
    request_hash = sha256_text(request_text)
    response_hash = sha256_text(response_text) if response_text is not None else None
    request_key = f"db://model-invocations/{execution_run.id}/{kind.value}/{request_hash}/request"
    if storage is not None:
        request_key = (
            f"repro/model-invocations/{execution_run.id}/{kind.value}/{request_hash}.request.json"
        )
        storage.save_bytes(request_key, request_text.encode("utf-8"))
    response_key: str | None = None
    if response_text is not None:
        response_key = (
            f"db://model-invocations/{execution_run.id}/{kind.value}/{response_hash}/response"
        )
        if storage is not None:
            response_key = (
                f"repro/model-invocations/{execution_run.id}/{kind.value}/{response_hash}.response.json"
            )
            storage.save_bytes(response_key, response_text.encode("utf-8"))
    invocation = ModelInvocation(
        tenant_id=execution_run.tenant_id,
        case_id=execution_run.case_id,
        execution_run_id=execution_run.id,
        kind=kind,
        provider_name=provider_name,
        endpoint_kind=endpoint_kind,
        requested_model_id=requested_model_id,
        actual_model_id=actual_model_id,
        reasoning_effort=reasoning_effort,
        temperature=temperature,
        embedding_model_id=embedding_model_id,
        tokenizer_identity=tokenizer_identity,
        tokenizer_version=tokenizer_version,
        sdk_version=sdk_version,
        request_payload_hash=request_hash,
        request_payload_text=request_text,
        request_artifact_key=request_key,
        response_payload_hash=response_hash,
        response_payload_text=response_text,
        response_artifact_key=response_key,
        provider_response_id=provider_response_id,
        remote_store=False,
        service_tier=service_tier,
        usage_json=usage_json,
        metadata_json=metadata_json or {},
    )
    session.add(invocation)
    session.flush()
    return invocation


def embed_text_recorded(
    session: Session,
    *,
    storage: LocalObjectStorage | None,
    execution_run: ExecutionRun,
    ai_service,
    text: str,
    model_id: str | None,
    dimensions: int | None,
    tokenizer_identity: str | None = None,
    tokenizer_version: str | None = None,
    metadata_json: dict[str, object] | None = None,
) -> list[float]:
    from app.services.ai import llm_provider_name

    vector = ai_service.embed_text(text, model_id=model_id, dimensions=dimensions)
    provider_name = llm_provider_name(ai_service)
    record_model_invocation(
        session,
        storage=storage,
        execution_run=execution_run,
        provider_name=provider_name,
        endpoint_kind="embeddings.create",
        kind=ModelInvocationKind.EMBEDDING,
        requested_model_id=model_id,
        actual_model_id=model_id,
        reasoning_effort=None,
        temperature=None,
        embedding_model_id=model_id,
        tokenizer_identity=tokenizer_identity,
        tokenizer_version=tokenizer_version,
        request_payload={"input": text, "dimensions": dimensions},
        response_payload={"embedding": vector},
        provider_response_id=None,
        sdk_version=importlib.metadata.version("openai") if provider_name != "stub" else None,
        metadata_json=metadata_json,
    )
    return vector


def embed_text_with_invocation_recorded(
    session: Session,
    *,
    storage: LocalObjectStorage | None,
    execution_run: ExecutionRun,
    ai_service,
    text: str,
    model_id: str | None,
    dimensions: int | None,
    tokenizer_identity: str | None = None,
    tokenizer_version: str | None = None,
    metadata_json: dict[str, object] | None = None,
) -> tuple[list[float], ModelInvocation]:
    from app.services.ai import llm_provider_name

    vector = ai_service.embed_text(text, model_id=model_id, dimensions=dimensions)
    provider_name = llm_provider_name(ai_service)
    invocation = record_model_invocation(
        session,
        storage=storage,
        execution_run=execution_run,
        provider_name=provider_name,
        endpoint_kind="embeddings.create",
        kind=ModelInvocationKind.EMBEDDING,
        requested_model_id=model_id,
        actual_model_id=model_id,
        reasoning_effort=None,
        temperature=None,
        embedding_model_id=model_id,
        tokenizer_identity=tokenizer_identity,
        tokenizer_version=tokenizer_version,
        request_payload={"input": text, "dimensions": dimensions},
        response_payload={"embedding": vector},
        provider_response_id=None,
        sdk_version=importlib.metadata.version("openai") if provider_name != "stub" else None,
        metadata_json=metadata_json,
    )
    return vector, invocation


@dataclass(frozen=True)
class ReproContext:
    mode: ReproducibilityMode
    repo_snapshot: RepoSnapshot
    runtime_snapshot: RuntimeSnapshot
    source_manifest: SourceManifest | None
    execution_run: ExecutionRun


def start_repro_run(
    session: Session,
    *,
    storage: LocalObjectStorage | None,
    settings: Settings,
    kind: ExecutionRunKind,
    mode: ReproducibilityMode,
    tenant_id=None,
    case_id=None,
    user_id=None,
    parent_run_id=None,
    source_manifest: SourceManifest | None = None,
    pipeline_profile_name: str | None = None,
    pipeline_config_hash: str | None = None,
    index_config_hash: str | None = None,
    runtime_config_hash: str | None = None,
    inputs_json: dict[str, object] | None = None,
    external_context_json: dict[str, object] | None = None,
) -> ReproContext:
    repo_snapshot = capture_repo_snapshot(session, storage=storage, mode=mode)
    runtime_snapshot = capture_runtime_snapshot(session, settings=settings, mode=mode)
    execution_run = create_execution_run(
        session,
        kind=kind,
        status=ExecutionRunStatus.RUNNING,
        mode=mode,
        repo_snapshot=repo_snapshot,
        runtime_snapshot=runtime_snapshot,
        tenant_id=tenant_id,
        case_id=case_id,
        user_id=user_id,
        parent_run_id=parent_run_id,
        source_manifest=source_manifest,
        pipeline_profile_name=pipeline_profile_name,
        pipeline_config_hash=pipeline_config_hash,
        index_config_hash=index_config_hash,
        runtime_config_hash=runtime_config_hash,
        inputs_json=inputs_json,
        external_context_json=external_context_json,
    )
    return ReproContext(
        mode=mode,
        repo_snapshot=repo_snapshot,
        runtime_snapshot=runtime_snapshot,
        source_manifest=source_manifest,
        execution_run=execution_run,
    )


def build_execution_run_manifest(session: Session, *, run_id: UUID) -> dict[str, object]:
    run = session.get(ExecutionRun, run_id)
    if run is None:
        raise ValidationFailure(f"Execution run {run_id} does not exist.")
    assert_execution_run_consistency(session, run=run)
    repo_snapshot = session.get(RepoSnapshot, run.repo_snapshot_id)
    runtime_snapshot = session.get(RuntimeSnapshot, run.runtime_snapshot_id)
    if repo_snapshot is None or runtime_snapshot is None:
        raise ValidationFailure(
            f"Execution run {run.id} is missing repo/runtime snapshot lineage."
        )
    source_manifest = (
        session.get(SourceManifest, run.source_manifest_id) if run.source_manifest_id else None
    )
    model_invocations = session.query(ModelInvocation).filter(
        ModelInvocation.execution_run_id == run.id
    ).order_by(ModelInvocation.created_at.asc()).all()
    artifact_builds = session.query(ArtifactBuild).filter(
        ArtifactBuild.created_by_run_id == run.id
    ).order_by(ArtifactBuild.created_at.asc()).all()
    manifest = {
        "run": {
            "id": str(run.id),
            "kind": run.kind.value,
            "status": run.status.value,
            "reproducibility_level": run.reproducibility_level.value,
            "reproducibility_mode": run.reproducibility_mode.value,
            "tenant_id": str(run.tenant_id) if run.tenant_id else None,
            "case_id": str(run.case_id) if run.case_id else None,
            "user_id": str(run.user_id) if run.user_id else None,
            "parent_run_id": str(run.parent_run_id) if run.parent_run_id else None,
            "pipeline_profile_name": run.pipeline_profile_name,
            "pipeline_config_hash": run.pipeline_config_hash,
            "index_config_hash": run.index_config_hash,
            "runtime_config_hash": run.runtime_config_hash,
            "inputs": run.inputs_json,
            "outputs": run.outputs_json,
            "replay": run.replay_json,
            "diagnostics": run.diagnostics_json,
            "started_at": run.started_at.isoformat() if run.started_at else None,
            "finished_at": run.finished_at.isoformat() if run.finished_at else None,
            "error_detail": run.error_detail,
        },
        "repo_snapshot": {
            "id": str(repo_snapshot.id),
            "git_commit_sha": repo_snapshot.git_commit_sha,
            "git_dirty": repo_snapshot.git_dirty,
            "git_diff_hash": repo_snapshot.git_diff_hash,
            "snapshot_hash": repo_snapshot.snapshot_hash,
        },
        "runtime_snapshot": {
            "id": str(runtime_snapshot.id),
            "python_version": runtime_snapshot.python_version,
            "backend_lock_hash": runtime_snapshot.backend_lock_hash,
            "frontend_lock_hash": runtime_snapshot.frontend_lock_hash,
            "alembic_head": runtime_snapshot.alembic_head,
            "db_vendor": runtime_snapshot.db_vendor,
            "db_version": runtime_snapshot.db_version,
            "pgvector_version": runtime_snapshot.pgvector_version,
            "package_versions": runtime_snapshot.package_versions_json,
            "env_fingerprint": runtime_snapshot.env_fingerprint_json,
            "env_fingerprint_hash": runtime_snapshot.env_fingerprint_hash,
        },
        "source_manifest": (
            {
                "id": str(source_manifest.id),
                "kind": source_manifest.kind.value,
                "manifest_hash": source_manifest.manifest_hash,
                "manifest": source_manifest.manifest_json,
            }
            if source_manifest is not None
            else None
        ),
        "artifact_builds": [
            {
                "id": str(build.id),
                "kind": build.kind.value,
                "status": build.status.value,
                "compatibility_hash": build.compatibility_hash,
                "index_config_hash": build.index_config_hash,
                "algorithm_version": build.algorithm_version,
                "tokenizer_identity": build.tokenizer_identity,
                "tokenizer_version": build.tokenizer_version,
                "parser_identity": build.parser_identity,
                "parser_version": build.parser_version,
                "embedding_model": build.embedding_model,
                "metadata": build.metadata_json,
            }
            for build in artifact_builds
        ],
        "model_invocations": [
            {
                "id": str(invocation.id),
                "kind": invocation.kind.value,
                "provider_name": invocation.provider_name,
                "endpoint_kind": invocation.endpoint_kind,
                "requested_model_id": invocation.requested_model_id,
                "actual_model_id": invocation.actual_model_id,
                "reasoning_effort": invocation.reasoning_effort,
                "temperature": invocation.temperature,
                "embedding_model_id": invocation.embedding_model_id,
                "tokenizer_identity": invocation.tokenizer_identity,
                "tokenizer_version": invocation.tokenizer_version,
                "sdk_version": invocation.sdk_version,
                "request_payload_hash": invocation.request_payload_hash,
                "request_artifact_key": invocation.request_artifact_key,
                "response_payload_hash": invocation.response_payload_hash,
                "response_artifact_key": invocation.response_artifact_key,
                "provider_response_id": invocation.provider_response_id,
                "service_tier": invocation.service_tier,
                "usage": invocation.usage_json,
                "metadata": invocation.metadata_json,
            }
            for invocation in model_invocations
        ],
    }
    manifest["manifest_hash"] = sha256_text(canonical_json_text(manifest))
    return manifest


def assert_execution_run_consistency(session: Session, *, run: ExecutionRun) -> None:
    if run.reproducibility_mode == ReproducibilityMode.STRICT_EVAL and run.source_manifest_id is None and run.kind in {
        ExecutionRunKind.HISTORICAL_IMPORT,
        ExecutionRunKind.HISTORICAL_REIMPORT,
        ExecutionRunKind.PRODUCT_TRUTH_IMPORT,
        ExecutionRunKind.PRODUCT_TRUTH_REIMPORT,
        ExecutionRunKind.LIVE_CASE_CREATE,
        ExecutionRunKind.LIVE_CASE_REBUILD,
        ExecutionRunKind.EXPORT,
    }:
        raise ValidationFailure(f"strict_eval run {run.id} is missing a required source manifest.")

    if run.kind == ExecutionRunKind.RETRIEVAL:
        retrieval_run_id = run.outputs_json.get("retrieval_run_id")
        if not isinstance(retrieval_run_id, str):
            raise ValidationFailure(f"Retrieval execution run {run.id} is missing retrieval_run_id output.")
        retrieval_run = session.get(RetrievalRun, UUID(retrieval_run_id))
        if retrieval_run is None or retrieval_run.execution_run_id != run.id:
            raise ValidationFailure(f"Retrieval execution run {run.id} is inconsistent with retrieval_runs.")
        replay = run.replay_json or {}
        for required_key in ("candidate_pools", "selected_evidence", "request_date"):
            if required_key not in replay:
                raise ValidationFailure(
                    f"Retrieval execution run {run.id} is missing replay field {required_key!r}."
                )
        if run.reproducibility_mode == ReproducibilityMode.STRICT_EVAL:
            for required_key in (
                "case_profile_build_id",
                "current_pdf_build_id",
                "query_embeddings",
            ):
                if required_key not in replay:
                    raise ValidationFailure(
                        f"strict_eval retrieval execution run {run.id} is missing replay field {required_key!r}."
                    )
            if not isinstance(replay["case_profile_build_id"], str) or not isinstance(
                replay["current_pdf_build_id"], str
            ):
                raise ValidationFailure(
                    f"strict_eval retrieval execution run {run.id} is missing active case build lineage."
                )
            query_embeddings = replay.get("query_embeddings")
            if not isinstance(query_embeddings, dict):
                raise ValidationFailure(
                    f"strict_eval retrieval execution run {run.id} is missing query embedding lineage."
                )
            if not query_embeddings:
                reused_from = replay.get("reused_from_retrieval_run_id")
                if not isinstance(reused_from, str):
                    raise ValidationFailure(
                        f"strict_eval retrieval execution run {run.id} is missing query embedding lineage."
                    )
                return
            for key, payload in query_embeddings.items():
                if not isinstance(payload, dict):
                    raise ValidationFailure(
                        f"strict_eval retrieval execution run {run.id} has invalid query embedding payload for {key!r}."
                    )
                invocation_id = payload.get("model_invocation_id")
                if not isinstance(invocation_id, str):
                    raise ValidationFailure(
                        f"strict_eval retrieval execution run {run.id} is missing model invocation for query embedding {key!r}."
                    )
                invocation = session.get(ModelInvocation, UUID(invocation_id))
                if (
                    invocation is None
                    or invocation.execution_run_id != run.id
                    or invocation.kind != ModelInvocationKind.EMBEDDING
                ):
                    raise ValidationFailure(
                        f"strict_eval retrieval execution run {run.id} has inconsistent query embedding model invocation for {key!r}."
                    )
        return

    if run.kind in {
        ExecutionRunKind.ROW_DRAFT,
        ExecutionRunKind.ROW_REVISION,
        ExecutionRunKind.BULK_FILL_ROW_ATTEMPT,
    }:
        answer_version_id = run.outputs_json.get("answer_version_id")
        retrieval_run_id = run.outputs_json.get("retrieval_run_id")
        model_invocation_id = run.outputs_json.get("model_invocation_id")
        planning_model_invocation_id = run.outputs_json.get("planning_model_invocation_id")
        rendering_model_invocation_id = run.outputs_json.get("rendering_model_invocation_id")
        source_planning_model_invocation_id = run.outputs_json.get("source_planning_model_invocation_id")
        generation_path = run.outputs_json.get("generation_path")
        if not isinstance(generation_path, str) or not generation_path.strip():
            generation_path = "two_stage_plan_render"
        if not all(isinstance(value, str) for value in (answer_version_id, retrieval_run_id, model_invocation_id)):
            raise ValidationFailure(
                f"Answer execution run {run.id} is missing answer/retrieval/model-invocation outputs."
            )
        answer_version_id = str(answer_version_id)
        retrieval_run_id = str(retrieval_run_id)
        model_invocation_id = str(model_invocation_id)
        answer_version = session.get(AnswerVersion, UUID(answer_version_id))
        retrieval_run = session.get(RetrievalRun, UUID(retrieval_run_id))
        model_invocation = session.get(ModelInvocation, UUID(model_invocation_id))
        planning_invocation = (
            session.get(ModelInvocation, UUID(str(planning_model_invocation_id)))
            if isinstance(planning_model_invocation_id, str)
            else None
        )
        rendering_invocation = (
            session.get(ModelInvocation, UUID(str(rendering_model_invocation_id)))
            if isinstance(rendering_model_invocation_id, str)
            else model_invocation
        )
        source_planning_invocation = (
            session.get(ModelInvocation, UUID(str(source_planning_model_invocation_id)))
            if isinstance(source_planning_model_invocation_id, str)
            else None
        )
        if (
            answer_version is None
            or retrieval_run is None
            or model_invocation is None
            or rendering_invocation is None
            or answer_version.execution_run_id != run.id
            or answer_version.model_invocation_id != rendering_invocation.id
            or answer_version.retrieval_run_id != retrieval_run.id
            or retrieval_run.execution_run_id is None
            or rendering_invocation.execution_run_id != run.id
        ):
            raise ValidationFailure(
                f"Answer execution run {run.id} is inconsistent with answer_versions/model_invocations/retrieval_runs."
            )
        if generation_path == "render_only_reuse_plan":
            planning_invocations_on_run = [
                invocation
                for invocation in session.scalars(
                    select(ModelInvocation).where(ModelInvocation.execution_run_id == run.id)
                ).all()
                if invocation.kind == ModelInvocationKind.ANSWER_GENERATION
                and invocation.metadata_json.get("prompt_family") == "answer_planning"
            ]
            if planning_invocations_on_run:
                raise ValidationFailure(
                    f"render_only_reuse_plan execution run {run.id} must not persist a planning invocation on the current run."
                )
            if source_planning_invocation is None:
                raise ValidationFailure(
                    f"render_only_reuse_plan execution run {run.id} is missing source planning lineage."
                )
            source_metadata = source_planning_invocation.metadata_json or {}
            if (
                source_planning_invocation.kind != ModelInvocationKind.ANSWER_GENERATION
                or source_metadata.get("prompt_family") != "answer_planning"
            ):
                raise ValidationFailure(
                    f"render_only_reuse_plan execution run {run.id} points to an invalid source planning invocation."
                )
        if run.reproducibility_mode == ReproducibilityMode.STRICT_EVAL:
            if generation_path != "render_only_reuse_plan" and planning_invocation is None:
                raise ValidationFailure(
                    f"strict_eval answer execution run {run.id} is missing planning model invocation lineage."
                )
            invocations_to_check: list[tuple[ModelInvocation | None, str, bool]] = [
                (rendering_invocation, "answer_rendering", True),
            ]
            if generation_path == "render_only_reuse_plan":
                invocations_to_check.append((source_planning_invocation, "answer_planning", False))
            else:
                invocations_to_check.append((planning_invocation, "answer_planning", True))
            for invocation, expected_family, require_same_run in invocations_to_check:
                if invocation is None:
                    raise ValidationFailure(
                        f"strict_eval answer execution run {run.id} is missing required {expected_family} invocation lineage."
                    )
                if require_same_run and invocation.execution_run_id != run.id:
                    raise ValidationFailure(
                        f"strict_eval answer execution run {run.id} has model invocation {invocation.id} attached to a different execution run."
                    )
                metadata = invocation.metadata_json or {}
                if not all(
                    isinstance(metadata.get(key), str) and str(metadata.get(key)).strip()
                    for key in ("prompt_family", "prompt_version", "resolved_prompt_hash")
                ):
                    raise ValidationFailure(
                        f"strict_eval answer execution run {run.id} is missing prompt lineage on model invocation {invocation.id}."
                    )
                if metadata.get("prompt_family") != expected_family:
                    raise ValidationFailure(
                        f"strict_eval answer execution run {run.id} has prompt family {metadata.get('prompt_family')!r} on invocation {invocation.id}, expected {expected_family!r}."
                    )
        return

    if run.kind in {ExecutionRunKind.LIVE_CASE_CREATE, ExecutionRunKind.LIVE_CASE_REBUILD}:
        case_id = run.outputs_json.get("case_id")
        current_pdf_build_id = run.outputs_json.get("current_pdf_build_id")
        case_profile_build_id = run.outputs_json.get("case_profile_build_id")
        if not all(isinstance(value, str) for value in (case_id, current_pdf_build_id, case_profile_build_id)):
            raise ValidationFailure(f"Case execution run {run.id} is missing case/build outputs.")
        case_id = str(case_id)
        current_pdf_build_id = str(current_pdf_build_id)
        case_profile_build_id = str(case_profile_build_id)
        case = session.get(RfxCase, UUID(case_id))
        current_pdf_build = session.get(ArtifactBuild, UUID(current_pdf_build_id))
        case_profile_build = session.get(ArtifactBuild, UUID(case_profile_build_id))
        if (
            case is None
            or str(case.current_pdf_build_id) != current_pdf_build_id
            or str(case.case_profile_build_id) != case_profile_build_id
            or current_pdf_build is None
            or current_pdf_build.created_by_run_id != run.id
            or case_profile_build is None
            or case_profile_build.created_by_run_id != run.id
        ):
            raise ValidationFailure(f"Case execution run {run.id} is inconsistent with active case builds.")
        if run.reproducibility_mode == ReproducibilityMode.STRICT_EVAL:
            profile = session.scalar(
                select(CaseProfile).where(
                    CaseProfile.case_id == case.id,
                    CaseProfile.artifact_build_id == case_profile_build.id,
                )
            )
            if profile is None:
                raise ValidationFailure(
                    f"strict_eval case execution run {run.id} is missing a case profile for build {case_profile_build.id}."
                )
            chunk_id = session.scalar(
                select(PdfChunk.id).where(PdfChunk.artifact_build_id == current_pdf_build.id)
            )
            if chunk_id is None:
                raise ValidationFailure(
                    f"strict_eval case execution run {run.id} is missing raw PDF chunks for build {current_pdf_build.id}."
                )
            case_profile_invocation = session.scalar(
                select(ModelInvocation).where(
                    ModelInvocation.execution_run_id == run.id,
                    ModelInvocation.kind == ModelInvocationKind.CASE_PROFILE_EXTRACTION,
                )
            )
            if case_profile_invocation is None:
                raise ValidationFailure(
                    f"strict_eval case execution run {run.id} is missing case-profile model invocation lineage."
                )
            metadata = case_profile_invocation.metadata_json or {}
            if not all(
                isinstance(metadata.get(key), str) and str(metadata.get(key)).strip()
                for key in ("prompt_family", "prompt_version", "resolved_prompt_hash")
            ):
                raise ValidationFailure(
                    f"strict_eval case execution run {run.id} has incomplete case-profile prompt lineage."
                )
        return

    if run.kind in {ExecutionRunKind.HISTORICAL_IMPORT, ExecutionRunKind.HISTORICAL_REIMPORT}:
        dataset_id = run.outputs_json.get("historical_dataset_id")
        build_id = run.outputs_json.get("artifact_build_id")
        if not isinstance(dataset_id, str) or not isinstance(build_id, str):
            raise ValidationFailure(f"Historical execution run {run.id} is missing dataset/build outputs.")
        dataset = session.get(HistoricalDataset, UUID(dataset_id))
        if dataset is None or str(dataset.artifact_build_id) != build_id:
            raise ValidationFailure(f"Historical execution run {run.id} is inconsistent with historical_datasets.")
        if run.reproducibility_mode == ReproducibilityMode.STRICT_EVAL:
            invocation_id = session.scalar(
                select(ModelInvocation.id).where(
                    ModelInvocation.execution_run_id == run.id,
                    ModelInvocation.kind == ModelInvocationKind.CASE_PROFILE_EXTRACTION,
                )
            )
            if invocation_id is None:
                raise ValidationFailure(
                    f"strict_eval historical execution run {run.id} is missing case-profile extraction lineage."
                )
        return

    if run.kind in {ExecutionRunKind.PRODUCT_TRUTH_IMPORT, ExecutionRunKind.PRODUCT_TRUTH_REIMPORT}:
        build_id = run.outputs_json.get("artifact_build_id")
        if not isinstance(build_id, str):
            raise ValidationFailure(f"Product-truth execution run {run.id} is missing artifact build output.")
        build = session.get(ArtifactBuild, UUID(build_id))
        record_id = session.scalar(
            select(ProductTruthRecord.id).where(ProductTruthRecord.artifact_build_id == UUID(build_id))
        )
        if build is None or build.created_by_run_id != run.id or record_id is None:
            raise ValidationFailure(f"Product-truth execution run {run.id} is inconsistent with artifact_builds.")
        return

    if run.kind == ExecutionRunKind.BULK_FILL_JOB:
        request = session.query(BulkFillRequest).filter(
            BulkFillRequest.execution_run_id == run.id
        ).one_or_none()
        if request is None:
            raise ValidationFailure(f"Bulk-fill execution run {run.id} is not linked from bulk_fill_requests.")
        return

    if run.kind == ExecutionRunKind.EXPORT:
        export_job_id = run.outputs_json.get("export_job_id")
        if not isinstance(export_job_id, str):
            raise ValidationFailure(f"Export execution run {run.id} is missing export_job_id output.")
        export_job = session.get(ExportJob, UUID(export_job_id))
        if export_job is None or export_job.execution_run_id != run.id:
            raise ValidationFailure(f"Export execution run {run.id} is inconsistent with export_jobs.")
        if export_job.output_upload_id is None:
            raise ValidationFailure(f"Export execution run {run.id} is missing output upload lineage.")
        output_upload = session.get(Upload, export_job.output_upload_id)
        if output_upload is None:
            raise ValidationFailure(f"Export execution run {run.id} references a missing output upload.")
        if output_upload.kind != UploadKind.EXPORT_XLSX:
            raise ValidationFailure(f"Export execution run {run.id} output upload is not an XLSX export.")
        csv_upload_id = export_job.metadata_json.get("csv_upload_id")
        run_csv_upload_id = run.outputs_json.get("csv_upload_id")
        zip_upload_id = export_job.metadata_json.get("zip_upload_id")
        run_zip_upload_id = run.outputs_json.get("zip_upload_id")
        if csv_upload_id is not None or run_csv_upload_id is not None:
            if not isinstance(csv_upload_id, str) or not isinstance(run_csv_upload_id, str):
                raise ValidationFailure(
                    f"Export execution run {run.id} is missing CSV upload lineage."
                )
            if csv_upload_id != run_csv_upload_id:
                raise ValidationFailure(
                    f"Export execution run {run.id} has mismatched CSV upload lineage."
                )
            csv_upload = session.get(Upload, UUID(csv_upload_id))
            if csv_upload is None:
                raise ValidationFailure(
                    f"Export execution run {run.id} references a missing CSV output upload."
                )
            if csv_upload.kind != UploadKind.EXPORT_CSV:
                raise ValidationFailure(
                    f"Export execution run {run.id} CSV upload is not a CSV export."
                )
        if zip_upload_id is not None or run_zip_upload_id is not None:
            if not isinstance(zip_upload_id, str) or not isinstance(run_zip_upload_id, str):
                raise ValidationFailure(
                    f"Export execution run {run.id} is missing ZIP upload lineage."
                )
            if zip_upload_id != run_zip_upload_id:
                raise ValidationFailure(
                    f"Export execution run {run.id} has mismatched ZIP upload lineage."
                )
            zip_upload = session.get(Upload, UUID(zip_upload_id))
            if zip_upload is None:
                raise ValidationFailure(
                    f"Export execution run {run.id} references a missing ZIP output upload."
                )
            if zip_upload.kind != UploadKind.EXPORT_ZIP:
                raise ValidationFailure(
                    f"Export execution run {run.id} ZIP upload is not a ZIP export."
                )
        if run.reproducibility_mode == ReproducibilityMode.STRICT_EVAL:
            if run.source_manifest_id is None:
                raise ValidationFailure(f"strict_eval export execution run {run.id} is missing a source manifest.")
            source_manifest = session.get(SourceManifest, run.source_manifest_id)
            export_mode_value = (
                source_manifest.manifest_json.get("export_mode")
                if source_manifest is not None
                else None
            )
            if not isinstance(export_mode_value, str):
                raise ValidationFailure(
                    f"strict_eval export execution run {run.id} is missing export mode in source manifest."
                )
            try:
                export_mode = ExportMode(export_mode_value)
            except ValueError as exc:
                raise ValidationFailure(
                    f"strict_eval export execution run {run.id} has invalid export mode in source manifest."
                ) from exc
            row_selection = (
                source_manifest.manifest_json.get("row_selection")
                if source_manifest is not None
                else None
            )
            if not isinstance(row_selection, list) or not row_selection:
                raise ValidationFailure(
                    f"strict_eval export execution run {run.id} is missing row-selection manifest lineage."
                )
            for entry in row_selection:
                if not isinstance(entry, dict):
                    raise ValidationFailure(
                        f"strict_eval export execution run {run.id} has invalid row-selection manifest data."
                    )
                selection_kind = entry.get("selection_kind")
                review_status_value = entry.get("review_status")
                answer_version_id = entry.get("answer_version_id")
                if selection_kind is None:
                    selection_kind = APPROVED_ANSWER_SELECTION_KIND
                if selection_kind not in {
                    APPROVED_ANSWER_SELECTION_KIND,
                    STATUS_PLACEHOLDER_SELECTION_KIND,
                }:
                    raise ValidationFailure(
                        f"strict_eval export execution run {run.id} has invalid selection_kind in row selection."
                    )
                if selection_kind == STATUS_PLACEHOLDER_SELECTION_KIND:
                    if not isinstance(review_status_value, str):
                        raise ValidationFailure(
                            f"strict_eval export execution run {run.id} is missing review status in row selection."
                        )
                    try:
                        review_status = QuestionnaireRowStatus(review_status_value)
                    except ValueError as exc:
                        raise ValidationFailure(
                            f"strict_eval export execution run {run.id} has invalid review status in row selection."
                        ) from exc
                    if review_status == QuestionnaireRowStatus.APPROVED:
                        raise ValidationFailure(
                            f"strict_eval export execution run {run.id} has an approved placeholder row."
                        )
                    if answer_version_id is not None or entry.get("answer_status") is not None:
                        raise ValidationFailure(
                            f"strict_eval export execution run {run.id} has placeholder row selection with answer lineage."
                        )
                    placeholder_text = entry.get("placeholder_text")
                    if (
                        not isinstance(placeholder_text, str)
                        or placeholder_text
                        != export_placeholder_text(export_mode, review_status)
                    ):
                        raise ValidationFailure(
                            f"strict_eval export execution run {run.id} has invalid placeholder row selection text."
                        )
                    continue
                if not isinstance(answer_version_id, str):
                    raise ValidationFailure(
                        f"strict_eval export execution run {run.id} is missing answer-version lineage in row selection."
                    )
                if entry.get("placeholder_text") is not None:
                    raise ValidationFailure(
                        f"strict_eval export execution run {run.id} has answer-backed row selection with placeholder text."
                    )
                answer_version = session.get(AnswerVersion, UUID(answer_version_id))
                retrieval_run = (
                    session.get(RetrievalRun, answer_version.retrieval_run_id)
                    if answer_version is not None
                    else None
                )
                if (
                    answer_version is None
                    or answer_version.execution_run_id is None
                    or answer_version.model_invocation_id is None
                    or retrieval_run is None
                    or retrieval_run.execution_run_id is None
                ):
                    raise ValidationFailure(
                        f"strict_eval export execution run {run.id} includes answer version {answer_version_id} without full lineage."
                    )
        return
