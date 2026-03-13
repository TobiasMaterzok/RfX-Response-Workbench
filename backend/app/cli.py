from __future__ import annotations

import json
import time
from pathlib import Path
from uuid import UUID

import typer
from sqlalchemy.exc import SQLAlchemyError

from app.config import get_settings
from app.db import ROOT_ALEMBIC_UPGRADE_COMMAND, WINDOWS_LOCAL_SETUP_DOC
from app.exceptions import ValidationFailure
from app.models.entities import BulkFillRequest, RfxCase
from app.models.enums import ReproducibilityMode
from app.services.bulk_fill import _ensure_bulk_fill_generation_available, run_bulk_fill_worker_once
from app.services.cases import rebuild_case_index_artifacts
from app.services.container import build_container
from app.services.identity import ensure_local_identity
from app.services.product_truth import ingest_product_truth_file, reimport_product_truth_file
from app.services.reproducibility import build_execution_run_manifest
from app.services.seed import import_historical_corpus

cli = typer.Typer()
REPO_ROOT = Path(__file__).resolve().parents[2]


def _bulk_fill_worker_status_message(container, request_id: UUID) -> str:
    with container.session_factory() as session:
        request = session.get(BulkFillRequest, request_id)
        if request is None:
            return f"Processed bulk-fill request {request_id}, but it could not be reloaded."
        return f"Processed bulk-fill request {request_id} with final status {request.status.value}"


def _resolve_repo_path(path: Path) -> Path:
    if path.exists() or path.is_absolute():
        return path
    candidate = REPO_ROOT / path
    return candidate if candidate.exists() else path


def _load_json_object(path: Path | None) -> dict[str, object] | None:
    if path is None:
        return None
    resolved = _resolve_repo_path(path)
    payload = json.loads(resolved.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValidationFailure(f"Pipeline config file {resolved} must contain a JSON object.")
    return payload


@cli.command("ensure-local-identity")
def ensure_local_identity_command() -> None:
    settings = get_settings()
    container = build_container(settings)
    try:
        with container.session_factory() as session:
            context = ensure_local_identity(session, settings)
            session.commit()
            typer.echo(f"Bootstrapped tenant={context.tenant.slug} user={context.user.email}")
    except SQLAlchemyError as exc:
        typer.echo(
            "Bootstrap failed. "
            f"Run `{ROOT_ALEMBIC_UPGRADE_COMMAND}` before initializing local identity. "
            f"On Win11, see `{WINDOWS_LOCAL_SETUP_DOC}`."
        )
        raise typer.Exit(code=1) from exc


@cli.command("import-historical-corpus")
def import_historical_corpus_command(
    base_path: Path = Path("seed_data"),
    pipeline_profile: str | None = typer.Option(None, help="Optional pipeline profile name."),
    pipeline_config_path: Path | None = typer.Option(
        None,
        help="Optional JSON file containing a pipeline override object.",
    ),
    reproducibility_mode: ReproducibilityMode = typer.Option(
        ReproducibilityMode.BEST_EFFORT,
        help="Reproducibility mode for the import run.",
    ),
) -> None:
    settings = get_settings()
    container = build_container(settings)
    resolved_base_path = _resolve_repo_path(base_path)
    try:
        with container.session_factory() as session:
            context = ensure_local_identity(session, settings)
            dataset = import_historical_corpus(
                session,
                ai_service=container.ai_service,
                storage=container.storage,
                tenant_id=context.tenant.id,
                base_path=resolved_base_path,
                settings=settings,
                pipeline_profile_name=pipeline_profile,
                pipeline_override=_load_json_object(pipeline_config_path),
                reproducibility_mode=reproducibility_mode,
            )
            session.commit()
            typer.echo(f"Imported dataset={dataset.slug}")
    except ValidationFailure as exc:
        typer.echo(f"Seed import failed: {exc}")
        raise typer.Exit(code=1) from exc
    except SQLAlchemyError as exc:
        typer.echo(
            "Seed import failed. "
            f"Run `{ROOT_ALEMBIC_UPGRADE_COMMAND}` first and ensure OPENAI_API_KEY is set for embeddings. "
            f"On Win11, see `{WINDOWS_LOCAL_SETUP_DOC}`."
        )
        raise typer.Exit(code=1) from exc


@cli.command("import-product-truth")
def import_product_truth_command(
    path: Path = Path("seed_data/product_truth/product_truth.json"),
    pipeline_profile: str | None = typer.Option(None, help="Optional pipeline profile name."),
    pipeline_config_path: Path | None = typer.Option(
        None,
        help="Optional JSON file containing a pipeline override object.",
    ),
    reproducibility_mode: ReproducibilityMode = typer.Option(
        ReproducibilityMode.BEST_EFFORT,
        help="Reproducibility mode for the import run.",
    ),
) -> None:
    settings = get_settings()
    container = build_container(settings)
    resolved_path = _resolve_repo_path(path)
    try:
        with container.session_factory() as session:
            context = ensure_local_identity(session, settings)
            records = ingest_product_truth_file(
                session,
                storage=container.storage,
                ai_service=container.ai_service,
                tenant_id=context.tenant.id,
                path=resolved_path,
                settings=settings,
                pipeline_profile_name=pipeline_profile,
                pipeline_override=_load_json_object(pipeline_config_path),
                reproducibility_mode=reproducibility_mode,
            )
            session.commit()
            typer.echo(f"Imported product truth records={len(records)}")
    except ValidationFailure as exc:
        typer.echo(f"Product-truth import failed: {exc}")
        raise typer.Exit(code=1) from exc


@cli.command("reimport-product-truth")
def reimport_product_truth_command(
    path: Path = Path("seed_data/product_truth/product_truth.json"),
    pipeline_profile: str | None = typer.Option(None, help="Optional pipeline profile name."),
    pipeline_config_path: Path | None = typer.Option(
        None,
        help="Optional JSON file containing a pipeline override object.",
    ),
    reproducibility_mode: ReproducibilityMode = typer.Option(
        ReproducibilityMode.BEST_EFFORT,
        help="Reproducibility mode for the reimport run.",
    ),
) -> None:
    settings = get_settings()
    container = build_container(settings)
    resolved_path = _resolve_repo_path(path)
    try:
        with container.session_factory() as session:
            context = ensure_local_identity(session, settings)
            records = reimport_product_truth_file(
                session,
                storage=container.storage,
                ai_service=container.ai_service,
                tenant_id=context.tenant.id,
                path=resolved_path,
                settings=settings,
                pipeline_profile_name=pipeline_profile,
                pipeline_override=_load_json_object(pipeline_config_path),
                reproducibility_mode=reproducibility_mode,
            )
            session.commit()
            typer.echo(f"Reimported product truth records={len(records)}")
    except ValidationFailure as exc:
        typer.echo(f"Product-truth reimport failed: {exc}")
        raise typer.Exit(code=1) from exc


@cli.command("rebuild-case-index-artifacts")
def rebuild_case_index_artifacts_command(
    case_id: UUID = typer.Argument(..., help="Case ID to rebuild."),
    pdf_upload_id: UUID | None = typer.Option(
        None,
        help="Optional explicit case PDF upload ID when the case has multiple PDFs.",
    ),
    pipeline_profile: str | None = typer.Option(None, help="Optional pipeline profile name."),
    pipeline_config_path: Path | None = typer.Option(
        None,
        help="Optional JSON file containing a pipeline override object.",
    ),
    reproducibility_mode: ReproducibilityMode = typer.Option(
        ReproducibilityMode.BEST_EFFORT,
        help="Reproducibility mode for the rebuild run.",
    ),
) -> None:
    settings = get_settings()
    container = build_container(settings)
    try:
        with container.session_factory() as session:
            case = session.get(RfxCase, case_id)
            if case is None:
                raise ValidationFailure(f"Case {case_id} does not exist.")
            rebuild_case_index_artifacts(
                session,
                storage=container.storage,
                ai_service=container.ai_service,
                case=case,
                settings=settings,
                pipeline_profile_name=pipeline_profile,
                pipeline_override=_load_json_object(pipeline_config_path),
                pdf_upload_id=pdf_upload_id,
                reproducibility_mode=reproducibility_mode,
            )
            session.commit()
            typer.echo(f"Rebuilt case index artifacts for case={case.id}")
    except ValidationFailure as exc:
        typer.echo(f"Case index rebuild failed: {exc}")
        raise typer.Exit(code=1) from exc


@cli.command("export-run-manifest")
def export_run_manifest_command(
    run_id: UUID = typer.Argument(..., help="Execution run ID to export."),
    output_path: Path | None = typer.Option(
        None,
        help="Optional path to write the canonical JSON manifest.",
    ),
) -> None:
    settings = get_settings()
    container = build_container(settings)
    with container.session_factory() as session:
        manifest = build_execution_run_manifest(session, run_id=run_id)
    payload = json.dumps(manifest, sort_keys=True, separators=(",", ":"), indent=2)
    if output_path is None:
        typer.echo(payload)
        return
    resolved = _resolve_repo_path(output_path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    resolved.write_text(payload + "\n", encoding="utf-8")
    typer.echo(f"Wrote manifest to {resolved}")


@cli.command("run-bulk-fill-worker")
def run_bulk_fill_worker_command(
    once: bool = typer.Option(False, help="Claim and run at most one queued bulk-fill job."),
    poll_interval_seconds: float = typer.Option(
        1.0,
        help="Polling interval in seconds when not running once.",
    ),
    runner_id: str = typer.Option(
        "bulk-fill-worker.local",
        help="Explicit runner identity recorded on claimed jobs.",
    ),
) -> None:
    settings = get_settings()
    container = build_container(settings)
    try:
        _ensure_bulk_fill_generation_available(container)
        if once:
            request_id = run_bulk_fill_worker_once(
                container,
                runner_id=runner_id,
                execution_mode="worker_cli",
            )
            typer.echo(
                _bulk_fill_worker_status_message(container, request_id)
                if request_id
                else "No queued bulk-fill jobs."
            )
            return
        while True:
            request_id = run_bulk_fill_worker_once(
                container,
                runner_id=runner_id,
                execution_mode="worker_cli",
            )
            if request_id is None:
                time.sleep(poll_interval_seconds)
                continue
            typer.echo(_bulk_fill_worker_status_message(container, request_id))
    except ValidationFailure as exc:
        typer.echo(f"Bulk-fill worker failed: {exc}")
        raise typer.Exit(code=1) from exc
    except SQLAlchemyError as exc:
        typer.echo("Bulk-fill worker failed due to database error.")
        raise typer.Exit(code=1) from exc


if __name__ == "__main__":
    cli()
