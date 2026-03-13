from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from importlib import metadata as importlib_metadata

import tiktoken

from app.exceptions import ValidationFailure
from app.pipeline.config import PipelineSelection
from app.schemas.answer_plan import NormalizedEvidenceItem
from app.services.hashing import sha256_text
from app.services.reproducibility import canonical_json_text

PACKING_ALGORITHM_VERSION = "evidence_packer.v1"

_BLOCK_TO_LAYER = {
    "current_case_facts": "current_case_facts",
    "raw_current_pdf": "raw_current_pdf",
    "product_truth": "product_truth",
    "historical_exemplars": "historical_exemplar",
}


@dataclass(frozen=True)
class PackedEvidence:
    items: list[NormalizedEvidenceItem]
    packed_json: str
    packed_hash: str
    metadata: dict[str, object]


def _packed_json(items: Sequence[NormalizedEvidenceItem]) -> str:
    return canonical_json_text([item.model_dump(mode="json") for item in items])


def _ordered_items(
    items: Sequence[NormalizedEvidenceItem],
    *,
    source_block_order: Sequence[str],
) -> list[NormalizedEvidenceItem]:
    grouped: dict[str, list[NormalizedEvidenceItem]] = {
        "current_case_facts": [],
        "raw_current_pdf": [],
        "product_truth": [],
        "historical_exemplar": [],
    }
    for item in items:
        if item.layer not in grouped:
            raise ValidationFailure(
                f"Packing received unsupported evidence layer {item.layer!r}."
            )
        grouped[item.layer].append(item)
    ordered: list[NormalizedEvidenceItem] = []
    for block_name in source_block_order:
        layer = _BLOCK_TO_LAYER.get(block_name)
        if layer is None:
            raise ValidationFailure(
                f"Packing received unsupported source block {block_name!r}."
            )
        ordered.extend(grouped[layer])
    return ordered


def _planning_tokenizer(selection: PipelineSelection):
    model_id = selection.resolved_pipeline.models.answer_planning.model_id
    if model_id is None or not model_id.strip():
        raise ValidationFailure(
            "packing.max_context_tokens requires a resolved answer-planning model."
        )
    try:
        return (
            model_id,
            importlib_metadata.version("tiktoken"),
            tiktoken.encoding_for_model(model_id),
        )
    except KeyError as exc:
        raise ValidationFailure(
            "packing.max_context_tokens requires a tokenizer-known answer-planning model; "
            f"observed {model_id!r}."
        ) from exc


def pack_normalized_evidence(
    items: Sequence[NormalizedEvidenceItem],
    *,
    selection: PipelineSelection,
) -> PackedEvidence:
    if not items:
        raise ValidationFailure("Packing requires at least one normalized evidence item.")
    packing = selection.resolved_pipeline.packing
    ordered = _ordered_items(items, source_block_order=packing.source_block_order)
    ordered_json = _packed_json(ordered)
    ordered_hash = sha256_text(ordered_json)
    max_context_tokens = packing.max_context_tokens
    tokenizer_identity: str | None = None
    tokenizer_version: str | None = None
    input_token_count: int | None = None
    output_token_count: int | None = None
    if max_context_tokens is None:
        return PackedEvidence(
            items=list(ordered),
            packed_json=ordered_json,
            packed_hash=ordered_hash,
            metadata={
                "algorithm_version": PACKING_ALGORITHM_VERSION,
                "order_strategy": packing.order_strategy,
                "source_block_order": list(packing.source_block_order),
                "max_context_tokens": None,
                "tokenizer_identity": None,
                "tokenizer_version": None,
                "input_item_ids": [item.id for item in items],
                "input_layers": [item.layer for item in items],
                "ordered_item_ids": [item.id for item in ordered],
                "ordered_layers": [item.layer for item in ordered],
                "output_item_ids": [item.id for item in ordered],
                "output_layers": [item.layer for item in ordered],
                "dropped_item_ids": [],
                "input_token_count": None,
                "output_token_count": None,
                "truncated": False,
                "input_evidence_hash": sha256_text(_packed_json(items)),
                "ordered_evidence_hash": ordered_hash,
                "output_evidence_hash": ordered_hash,
            },
        )
    tokenizer_identity, tokenizer_version, encoding = _planning_tokenizer(selection)
    input_token_count = len(encoding.encode(ordered_json))
    packed_items: list[NormalizedEvidenceItem] = []
    packed_json = ""
    output_token_count = 0
    for item in ordered:
        candidate_items = [*packed_items, item]
        candidate_json = _packed_json(candidate_items)
        candidate_tokens = len(encoding.encode(candidate_json))
        if candidate_tokens > max_context_tokens:
            break
        packed_items = candidate_items
        packed_json = candidate_json
        output_token_count = candidate_tokens
    if not packed_items:
        raise ValidationFailure(
            "packing.max_context_tokens is too small to fit even the first packed evidence item. "
            "Increase the budget or disable packing truncation."
        )
    packed_hash = sha256_text(packed_json)
    return PackedEvidence(
        items=packed_items,
        packed_json=packed_json,
        packed_hash=packed_hash,
        metadata={
            "algorithm_version": PACKING_ALGORITHM_VERSION,
            "order_strategy": packing.order_strategy,
            "source_block_order": list(packing.source_block_order),
            "max_context_tokens": max_context_tokens,
            "tokenizer_identity": tokenizer_identity,
            "tokenizer_version": tokenizer_version,
            "input_item_ids": [item.id for item in items],
            "input_layers": [item.layer for item in items],
            "ordered_item_ids": [item.id for item in ordered],
            "ordered_layers": [item.layer for item in ordered],
            "output_item_ids": [item.id for item in packed_items],
            "output_layers": [item.layer for item in packed_items],
            "dropped_item_ids": [item.id for item in ordered[len(packed_items) :]],
            "input_token_count": input_token_count,
            "output_token_count": output_token_count,
            "truncated": len(packed_items) != len(ordered),
            "input_evidence_hash": sha256_text(_packed_json(items)),
            "ordered_evidence_hash": ordered_hash,
            "output_evidence_hash": packed_hash,
        },
    )
