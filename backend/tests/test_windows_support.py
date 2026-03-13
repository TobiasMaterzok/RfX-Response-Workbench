from __future__ import annotations

from io import BytesIO
from pathlib import Path

from openpyxl import Workbook
from sqlalchemy import select

from app.models.entities import Upload
from app.services.cases import create_case_from_uploads
from app.services.identity import ensure_local_identity
from app.services.object_keys import safe_object_key_filename


def _questionnaire_payload() -> bytes:
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


def test_safe_object_key_filename_sanitizes_windows_unsafe_names() -> None:
    assert safe_object_key_filename("client:brief?.pdf") == "client-brief.pdf"
    assert safe_object_key_filename(r"nested\\RFX<draft>.xlsx") == "RFX-draft.xlsx"
    assert safe_object_key_filename("COM1.txt") == "COM1-file.txt"
    assert safe_object_key_filename("   ") == "file"


def test_create_case_upload_object_keys_are_windows_safe(
    session,
    container,
    repo_root: Path,
    settings,
) -> None:
    context = ensure_local_identity(session, settings)
    pdf_payload = (
        repo_root
        / "seed_data"
        / "historical_customers"
        / "nordtransit_logistik_ag"
        / "nordtransit_logistik_ag_context_brief.pdf"
    ).read_bytes()
    case = create_case_from_uploads(
        session,
        storage=container.storage,
        ai_service=container.ai_service,
        tenant_id=context.tenant.id,
        user_id=context.user.id,
        case_name="Windows Safe Paths",
        client_name="Windows Client",
        pdf_file_name="client:brief?.pdf",
        pdf_media_type="application/pdf",
        pdf_payload=pdf_payload,
        questionnaire_file_name=r"nested\RFX<draft>.xlsx",
        questionnaire_media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        questionnaire_payload=_questionnaire_payload(),
        settings=settings,
    )
    uploads = session.scalars(
        select(Upload).where(Upload.case_id == case.id)
    ).all()
    assert {upload.original_file_name for upload in uploads} == {
        "client:brief?.pdf",
        r"nested\RFX<draft>.xlsx",
    }
    for upload in uploads:
        assert upload.object_key.startswith(f"cases/{case.id}/")
        assert ":" not in upload.object_key
        assert "?" not in upload.object_key
        assert "<" not in upload.object_key
        assert ">" not in upload.object_key
        assert "\\" not in upload.object_key
