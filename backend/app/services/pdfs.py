from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO

from pypdf import PdfReader

from app.exceptions import ValidationFailure
from app.services.hashing import sha256_hex, sha256_text


@dataclass(frozen=True)
class ExtractedPdfPage:
    page_number: int
    text: str
    text_hash: str


@dataclass(frozen=True)
class ExtractedPdfDocument:
    pages: list[ExtractedPdfPage]
    file_hash: str


def extract_pdf(payload: bytes) -> ExtractedPdfDocument:
    file_hash = sha256_hex(payload)
    reader = PdfReader(BytesIO(payload))
    if not reader.pages:
        raise ValidationFailure("Uploaded PDF contains no pages.")
    pages: list[ExtractedPdfPage] = []
    for index, page in enumerate(reader.pages, start=1):
        text = (page.extract_text() or "").strip()
        if not text:
            raise ValidationFailure(f"Uploaded PDF page {index} has no extractable text.")
        pages.append(ExtractedPdfPage(page_number=index, text=text, text_hash=sha256_text(text)))
    return ExtractedPdfDocument(pages=pages, file_hash=file_hash)
