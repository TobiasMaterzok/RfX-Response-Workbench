from __future__ import annotations

from io import BytesIO
from pathlib import Path

from openpyxl import Workbook
from sqlalchemy import text

from app.services.cases import create_case_from_uploads
from app.services.identity import ensure_local_identity
from tests.seed_paths import historical_customer_dir


def sample_pdf_bytes(repo_root: Path) -> bytes:
    return (
        historical_customer_dir(repo_root, "nordtransit_logistik_ag")
        / "nordtransit_logistik_ag_context_brief.pdf"
    ).read_bytes()


def build_questionnaire_payload() -> bytes:
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = "QA"
    worksheet["A1"] = "Context"
    worksheet["B1"] = "Question"
    worksheet["C1"] = "Answer"
    worksheet["A2"] = "Context A"
    worksheet["B2"] = "Question A"
    worksheet["C2"] = ""
    output = BytesIO()
    workbook.save(output)
    return output.getvalue()


def test_enum_columns_persist_values_not_names(session, container, repo_root: Path, settings) -> None:
    context = ensure_local_identity(session, settings)
    create_case_from_uploads(
        session,
        storage=container.storage,
        ai_service=container.ai_service,
        tenant_id=context.tenant.id,
        user_id=context.user.id,
        case_name="Enum Case",
        client_name="Enum Client",
        pdf_file_name="context.pdf",
        pdf_media_type="application/pdf",
        pdf_payload=sample_pdf_bytes(repo_root),
        questionnaire_file_name="qa.xlsx",
        questionnaire_media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        questionnaire_payload=build_questionnaire_payload(),
        settings=settings,
    )
    session.flush()

    membership_role = session.execute(
        text("select role from memberships limit 1")
    ).scalar_one()
    case_status = session.execute(
        text("select status from rfx_cases order by created_at desc limit 1"),
    ).scalar_one()
    upload_kind = session.execute(
        text("select kind from uploads order by created_at asc limit 1"),
    ).scalar_one()
    row_status = session.execute(
        text(
            "select review_status from questionnaire_rows order by source_row_number asc limit 1"
        )
    ).scalar_one()

    assert membership_role == "admin"
    assert case_status == "active"
    assert upload_kind == "case_pdf"
    assert row_status == "not_started"
