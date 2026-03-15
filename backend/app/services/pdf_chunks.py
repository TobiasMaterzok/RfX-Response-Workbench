from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import tiktoken
from sqlalchemy.orm import Session

from app.exceptions import ValidationFailure
from app.models.entities import PdfChunk, PdfPage
from app.pipeline.config import PipelineSelection, artifact_index_hashes
from app.services.ai import AIService, embedding_model_name
from app.services.hashing import sha256_text
from app.services.progress import (
    ProgressCallback,
    progress_interval,
    report_progress,
    should_report_progress,
)
from app.services.reproducibility import ReproContext, embed_text_recorded

PDF_CHUNKER_VERSION_LEGACY = "pdf_chunker.v1"
PDF_CHUNKER_VERSION_TOKEN = "pdf_chunker.v2"
PDF_CHUNK_TARGET_CHARS = 900
PDF_CHUNK_OVERLAP_CHARS = 150
PDF_CHUNK_MIN_BREAK_CHARS = 450


@dataclass(frozen=True)
class PdfChunkBuildContext:
    case_name: str
    client_name: str | None
    language: str
    source_file_name: str
    total_pages: int


@dataclass(frozen=True)
class ChunkedPdfSegment:
    page_number: int
    chunk_index: int
    start_offset: int
    end_offset: int
    raw_content: str


@dataclass(frozen=True)
class ChunkedPdfText:
    page_number: int
    chunk_index: int
    start_offset: int
    end_offset: int
    content: str
    chunk_hash: str


@dataclass(frozen=True)
class TokenSpan:
    start_offset: int
    end_offset: int


def current_pdf_chunking_version(selection: PipelineSelection) -> str:
    current_pdf = selection.resolved_pipeline.indexing.current_pdf
    if current_pdf.chunk_unit == "legacy_char" and not current_pdf.contextualize_chunks:
        return PDF_CHUNKER_VERSION_LEGACY
    return PDF_CHUNKER_VERSION_TOKEN


def _next_chunk_end(text: str, start_offset: int) -> int:
    hard_end = min(len(text), start_offset + PDF_CHUNK_TARGET_CHARS)
    if hard_end == len(text):
        return hard_end
    split_at = text.rfind(" ", start_offset + PDF_CHUNK_MIN_BREAK_CHARS, hard_end)
    if split_at == -1 or split_at <= start_offset:
        return hard_end
    return split_at


def _trim_segment(text: str, *, start_offset: int, end_offset: int) -> tuple[int, int, str]:
    raw_content = text[start_offset:end_offset]
    leading = len(raw_content) - len(raw_content.lstrip())
    trailing = len(raw_content) - len(raw_content.rstrip())
    trimmed_start = start_offset + leading
    trimmed_end = end_offset - trailing
    trimmed_content = text[trimmed_start:trimmed_end]
    return trimmed_start, trimmed_end, trimmed_content


def _build_contextualized_content(
    raw_content: str,
    *,
    page_number: int,
    context: PdfChunkBuildContext,
) -> str:
    header_lines = [
        f"Case: {context.case_name}",
        f"Client: {context.client_name or 'Unknown'}",
        f"Language: {context.language}",
        f"Source PDF: {context.source_file_name}",
        f"Page: {page_number}/{context.total_pages}",
    ]
    return "\n".join(header_lines) + "\n\n" + raw_content


def _finalize_segments(
    segments: Sequence[ChunkedPdfSegment],
    *,
    context: PdfChunkBuildContext,
    contextualize_chunks: bool,
) -> list[ChunkedPdfText]:
    finalized: list[ChunkedPdfText] = []
    for segment in segments:
        content = segment.raw_content
        if contextualize_chunks:
            content = _build_contextualized_content(
                segment.raw_content,
                page_number=segment.page_number,
                context=context,
            )
        finalized.append(
            ChunkedPdfText(
                page_number=segment.page_number,
                chunk_index=segment.chunk_index,
                start_offset=segment.start_offset,
                end_offset=segment.end_offset,
                content=content,
                chunk_hash=sha256_text(content),
            )
        )
    return finalized


def _legacy_chunk_pdf_page_text(*, page_number: int, text: str) -> list[ChunkedPdfSegment]:
    if not text.strip():
        raise ValidationFailure(f"PDF page {page_number} has no chunkable text.")
    chunks: list[ChunkedPdfSegment] = []
    start_offset = 0
    chunk_index = 0
    text_length = len(text)
    while start_offset < text_length:
        while start_offset < text_length and text[start_offset].isspace():
            start_offset += 1
        if start_offset >= text_length:
            break
        end_offset = _next_chunk_end(text, start_offset)
        trimmed_start, trimmed_end, raw_content = _trim_segment(
            text,
            start_offset=start_offset,
            end_offset=end_offset,
        )
        if not raw_content:
            raise ValidationFailure(
                f"PDF page {page_number} produced an empty chunk at offset {start_offset}."
            )
        chunk_index += 1
        chunks.append(
            ChunkedPdfSegment(
                page_number=page_number,
                chunk_index=chunk_index,
                start_offset=trimmed_start,
                end_offset=trimmed_end,
                raw_content=raw_content,
            )
        )
        if end_offset >= text_length:
            break
        next_start = max(end_offset - PDF_CHUNK_OVERLAP_CHARS, start_offset + 1)
        if next_start <= start_offset:
            raise ValidationFailure(
                f"PDF chunking stalled on page {page_number} at offset {start_offset}."
            )
        start_offset = next_start
    if not chunks:
        raise ValidationFailure(f"PDF page {page_number} produced no chunks.")
    return chunks


def _encoding_for_model(model_id: str):
    try:
        return tiktoken.encoding_for_model(model_id)
    except KeyError as exc:
        raise ValidationFailure(
            f"Token-aware chunking does not support embedding model {model_id!r}. Use a tokenizer-known model or keep chunk_unit='legacy_char'."
        ) from exc


def _byte_to_char_index(text: str) -> list[int]:
    mapping = [0]
    for char_index, character in enumerate(text, start=1):
        for _ in character.encode("utf-8"):
            mapping.append(char_index)
    return mapping


def _token_spans(text: str, *, model_id: str) -> list[TokenSpan]:
    encoding = _encoding_for_model(model_id)
    token_ids = encoding.encode_ordinary(text)
    if not token_ids:
        return []
    text_bytes = text.encode("utf-8")
    byte_to_char = _byte_to_char_index(text)
    reconstructed = b""
    spans: list[TokenSpan] = []
    cursor = 0
    for token_id in token_ids:
        token_bytes = encoding.decode_single_token_bytes(token_id)
        reconstructed += token_bytes
        next_cursor = cursor + len(token_bytes)
        if next_cursor > len(text_bytes):
            raise ValidationFailure("Token-aware chunking produced out-of-range byte offsets.")
        spans.append(
            TokenSpan(
                start_offset=byte_to_char[cursor],
                end_offset=byte_to_char[next_cursor],
            )
        )
        cursor = next_cursor
    if reconstructed != text_bytes:
        raise ValidationFailure(
            "Token-aware chunking could not prove byte-for-byte reconstruction of the source text."
        )
    return spans


def _token_chunk_pdf_page_text(
    *,
    page_number: int,
    text: str,
    model_id: str,
    chunk_size: int,
    chunk_overlap: int,
) -> list[ChunkedPdfSegment]:
    if not text.strip():
        raise ValidationFailure(f"PDF page {page_number} has no chunkable text.")
    token_spans = _token_spans(text, model_id=model_id)
    if not token_spans:
        raise ValidationFailure(f"PDF page {page_number} produced no token spans.")
    chunks: list[ChunkedPdfSegment] = []
    chunk_index = 0
    start_token = 0
    while start_token < len(token_spans):
        end_token = min(len(token_spans), start_token + chunk_size)
        raw_start = token_spans[start_token].start_offset
        raw_end = token_spans[end_token - 1].end_offset
        trimmed_start, trimmed_end, raw_content = _trim_segment(
            text,
            start_offset=raw_start,
            end_offset=raw_end,
        )
        if not raw_content:
            raise ValidationFailure(
                f"Token-aware chunking produced an empty chunk on page {page_number} at token {start_token}."
            )
        chunk_index += 1
        chunks.append(
            ChunkedPdfSegment(
                page_number=page_number,
                chunk_index=chunk_index,
                start_offset=trimmed_start,
                end_offset=trimmed_end,
                raw_content=raw_content,
            )
        )
        if end_token >= len(token_spans):
            break
        next_start = max(end_token - chunk_overlap, start_token + 1)
        if next_start <= start_token:
            raise ValidationFailure(
                f"Token-aware chunking stalled on page {page_number} at token {start_token}."
            )
        start_token = next_start
    return chunks


def chunk_pdf_pages(
    pages: Sequence[PdfPage],
    *,
    pipeline: PipelineSelection,
    context: PdfChunkBuildContext,
) -> list[ChunkedPdfText]:
    current_pdf = pipeline.resolved_pipeline.indexing.current_pdf
    embedding_model = pipeline.resolved_pipeline.indexing.embedding_model
    if embedding_model is None:
        raise ValidationFailure("Resolved pipeline is missing current-PDF embedding model.")
    all_segments: list[ChunkedPdfSegment] = []
    for page in sorted(pages, key=lambda item: item.page_number):
        if current_pdf.chunk_unit == "legacy_char":
            all_segments.extend(_legacy_chunk_pdf_page_text(page_number=page.page_number, text=page.extracted_text))
        else:
            all_segments.extend(
                _token_chunk_pdf_page_text(
                    page_number=page.page_number,
                    text=page.extracted_text,
                    model_id=embedding_model,
                    chunk_size=current_pdf.chunk_size,
                    chunk_overlap=current_pdf.chunk_overlap,
                )
            )
    return _finalize_segments(
        all_segments,
        context=context,
        contextualize_chunks=current_pdf.contextualize_chunks,
    )


def persist_pdf_chunks(
    session: Session,
    *,
    ai_service: AIService,
    pipeline: PipelineSelection,
    tenant_id,
    case_id,
    upload_id,
    pages: Sequence[PdfPage],
    case_name: str = "unknown_case",
    client_name: str | None = None,
    language: str = "unknown",
    source_file_name: str = "unknown.pdf",
    artifact_build_id=None,
    repro_context: ReproContext | None = None,
    storage=None,
    progress_callback: ProgressCallback | None = None,
) -> list[PdfChunk]:
    embedding_model = (
        pipeline.resolved_pipeline.indexing.embedding_model or embedding_model_name(ai_service)
    )
    chunks = chunk_pdf_pages(
        pages,
        pipeline=pipeline,
        context=PdfChunkBuildContext(
            case_name=case_name,
            client_name=client_name,
            language=language,
            source_file_name=source_file_name,
            total_pages=len(pages),
        ),
    )
    total_chunks = len(chunks)
    report_progress(
        progress_callback,
        f"Prepared current-PDF chunks for case {case_id}: pages={len(pages)} chunks={total_chunks}",
    )
    artifact_hashes = artifact_index_hashes(pipeline)
    models: list[PdfChunk] = []
    every = progress_interval(total_chunks)
    for index, chunk in enumerate(chunks, start=1):
        model = PdfChunk(
            tenant_id=tenant_id,
            case_id=case_id,
            upload_id=upload_id,
            page_number=chunk.page_number,
            chunk_index=chunk.chunk_index,
            start_offset=chunk.start_offset,
            end_offset=chunk.end_offset,
            chunking_version=current_pdf_chunking_version(pipeline),
            embedding_model=embedding_model,
            index_config_hash=artifact_hashes.current_pdf,
            artifact_build_id=artifact_build_id,
            chunk_hash=chunk.chunk_hash,
            content=chunk.content,
            embedding=(
                embed_text_recorded(
                    session,
                    storage=storage,
                    execution_run=repro_context.execution_run,
                    ai_service=ai_service,
                    text=chunk.content,
                    model_id=pipeline.resolved_pipeline.indexing.embedding_model,
                    dimensions=pipeline.resolved_pipeline.indexing.embedding_dimensions,
                    tokenizer_identity=(
                        pipeline.resolved_pipeline.indexing.embedding_model
                        if pipeline.resolved_pipeline.indexing.current_pdf.chunk_unit == "token"
                        else None
                    ),
                    tokenizer_version=(
                        tiktoken.__version__
                        if pipeline.resolved_pipeline.indexing.current_pdf.chunk_unit == "token"
                        else None
                    ),
                    metadata_json={
                        "artifact_family": "current_pdf",
                        "page_number": chunk.page_number,
                        "chunk_index": chunk.chunk_index,
                    },
                )
                if repro_context is not None and storage is not None
                else ai_service.embed_text(
                    chunk.content,
                    model_id=pipeline.resolved_pipeline.indexing.embedding_model,
                    dimensions=pipeline.resolved_pipeline.indexing.embedding_dimensions,
                )
            ),
        )
        session.add(model)
        models.append(model)
        if should_report_progress(index, total_chunks, every=every):
            report_progress(
                progress_callback,
                f"Embedded current-PDF chunks for case {case_id}: {index}/{total_chunks}",
            )
    session.flush()
    report_progress(
        progress_callback,
        f"Persisted current-PDF chunks for case {case_id}: {len(models)}",
    )
    return models
