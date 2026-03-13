from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.api.deps import get_session, get_user_context
from app.exceptions import ValidationFailure
from app.models.entities import (
    AnswerVersion,
    ArtifactBuild,
    BulkFillRequest,
    BulkFillRowExecution,
    CaseProfile,
    CaseProfileItem,
    ChatMessage,
    ChatThread,
    ExecutionRun,
    ExportJob,
    HistoricalCaseProfile,
    HistoricalCaseProfileItem,
    HistoricalClientPackage,
    HistoricalDataset,
    HistoricalQARow,
    HistoricalWorkbook,
    Membership,
    ModelInvocation,
    PdfChunk,
    ProductTruthChunk,
    ProductTruthRecord,
    Questionnaire,
    QuestionnaireRow,
    RepoSnapshot,
    RetrievalRun,
    RetrievalSnapshotItem,
    RfxCase,
    RuntimeSnapshot,
    SourceManifest,
    Upload,
)
from app.schemas.api import DevTableListResponse, DevTableRowsResponse, DevTableSummaryResponse
from app.services.cases import require_case_scope
from app.services.identity import UserContext

router = APIRouter(prefix="/api/dev", tags=["dev"])


@dataclass(frozen=True)
class DevTableConfig:
    model: Any
    excluded_columns: set[str]
    case_filter_column: str | None = "case_id"
    tenant_filter_column: str | None = "tenant_id"


TABLE_CONFIGS: dict[str, DevTableConfig] = {
    "memberships": DevTableConfig(Membership, set(), case_filter_column=None),
    "rfx_cases": DevTableConfig(RfxCase, set(), case_filter_column="id"),
    "uploads": DevTableConfig(Upload, {"payload"}),
    "historical_datasets": DevTableConfig(HistoricalDataset, set(), case_filter_column=None),
    "historical_client_packages": DevTableConfig(
        HistoricalClientPackage,
        set(),
        case_filter_column=None,
    ),
    "historical_case_profiles": DevTableConfig(
        HistoricalCaseProfile,
        {"signature_embedding"},
        case_filter_column=None,
    ),
    "historical_case_profile_items": DevTableConfig(
        HistoricalCaseProfileItem,
        {"embedding"},
        case_filter_column=None,
    ),
    "historical_workbooks": DevTableConfig(HistoricalWorkbook, set(), case_filter_column=None),
    "historical_qa_rows": DevTableConfig(
        HistoricalQARow,
        {"embedding"},
        case_filter_column=None,
    ),
    "product_truth_records": DevTableConfig(
        ProductTruthRecord,
        set(),
        case_filter_column=None,
    ),
    "product_truth_chunks": DevTableConfig(
        ProductTruthChunk,
        {"embedding"},
        case_filter_column=None,
    ),
    "case_profiles": DevTableConfig(CaseProfile, set()),
    "case_profile_items": DevTableConfig(CaseProfileItem, {"embedding"}),
    "pdf_chunks": DevTableConfig(PdfChunk, {"embedding"}),
    "questionnaires": DevTableConfig(Questionnaire, set()),
    "questionnaire_rows": DevTableConfig(QuestionnaireRow, set()),
    "chat_threads": DevTableConfig(ChatThread, set()),
    "chat_messages": DevTableConfig(ChatMessage, set()),
    "retrieval_runs": DevTableConfig(RetrievalRun, set()),
    "retrieval_snapshot_items": DevTableConfig(RetrievalSnapshotItem, set()),
    "answer_versions": DevTableConfig(AnswerVersion, set()),
    "execution_runs": DevTableConfig(ExecutionRun, set()),
    "model_invocations": DevTableConfig(ModelInvocation, set()),
    "artifact_builds": DevTableConfig(ArtifactBuild, set(), case_filter_column=None),
    "source_manifests": DevTableConfig(SourceManifest, set(), case_filter_column=None),
    "repo_snapshots": DevTableConfig(RepoSnapshot, set(), case_filter_column=None, tenant_filter_column=None),
    "runtime_snapshots": DevTableConfig(RuntimeSnapshot, set(), case_filter_column=None, tenant_filter_column=None),
    "export_jobs": DevTableConfig(ExportJob, set()),
    "bulk_fill_requests": DevTableConfig(BulkFillRequest, set()),
    "bulk_fill_row_executions": DevTableConfig(BulkFillRowExecution, set()),
}


def _serialize_value(value: Any) -> Any:
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, dict):
        return {key: _serialize_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_serialize_value(item) for item in value]
    return value


def _get_filters(
    *,
    config: DevTableConfig,
    user_context: UserContext,
    case_id: UUID | None,
    session: Session,
) -> list[Any]:
    filters: list[Any] = []
    if config.tenant_filter_column is not None:
        filters.append(
            getattr(config.model, config.tenant_filter_column) == user_context.tenant.id
        )
    if case_id is not None:
        require_case_scope(session, case_id=case_id, tenant_id=user_context.tenant.id)
        if config.case_filter_column is None:
            raise ValidationFailure(
                f"Table does not support case filtering: {config.model.__tablename__}."
            )
        filters.append(getattr(config.model, config.case_filter_column) == case_id)
    return filters


@router.get("/tables", response_model=DevTableListResponse)
def list_dev_tables(
    case_id: UUID | None = Query(default=None),
    session: Session = Depends(get_session),
    user_context: UserContext = Depends(get_user_context),
) -> DevTableListResponse:
    tables: list[DevTableSummaryResponse] = []
    for name, config in TABLE_CONFIGS.items():
        filters = _get_filters(
            config=config,
            user_context=user_context,
            case_id=None,
            session=session,
        )
        row_count = session.scalar(
            select(func.count()).select_from(config.model).where(*filters)
        ) or 0
        tables.append(
            DevTableSummaryResponse(
                name=name,
                row_count=row_count,
                case_filter_supported=config.case_filter_column is not None,
            )
        )
    return DevTableListResponse(tables=tables)


@router.get("/tables/{table_name}", response_model=DevTableRowsResponse)
def browse_dev_table(
    table_name: str,
    limit: int = Query(default=50, ge=1, le=200),
    case_id: UUID | None = Query(default=None),
    session: Session = Depends(get_session),
    user_context: UserContext = Depends(get_user_context),
) -> DevTableRowsResponse:
    config = TABLE_CONFIGS.get(table_name)
    if config is None:
        raise ValidationFailure(f"Unsupported dev table: {table_name}.")
    filters = _get_filters(
        config=config,
        user_context=user_context,
        case_id=case_id,
        session=session,
    )
    columns = [
        column.name
        for column in config.model.__table__.columns
        if column.name not in config.excluded_columns
    ]
    query: Any = select(config.model).where(*filters)
    if "created_at" in config.model.__table__.columns:
        query = query.order_by(config.model.created_at.desc())
    rows = session.scalars(query.limit(limit)).all()
    row_count = session.scalar(
        select(func.count()).select_from(config.model).where(*filters)
    ) or 0
    return DevTableRowsResponse(
        table_name=table_name,
        row_count=row_count,
        case_filter_applied=case_id is not None,
        columns=columns,
        rows=[
            {
                column_name: _serialize_value(getattr(row, column_name))
                for column_name in columns
            }
            for row in rows
        ],
    )
