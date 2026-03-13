from __future__ import annotations

from typing import Final, Literal

from app.models.enums import ExportMode, QuestionnaireRowStatus

APPROVED_ANSWER_SELECTION_KIND: Final = "approved_answer"
STATUS_PLACEHOLDER_SELECTION_KIND: Final = "status_placeholder"
type ExportSelectionKind = Literal[
    "approved_answer",
    "status_placeholder",
]


def _review_status_value(status: QuestionnaireRowStatus | str) -> str:
    if isinstance(status, QuestionnaireRowStatus):
        return status.value
    return status


def humanize_review_status(status: QuestionnaireRowStatus | str) -> str:
    return _review_status_value(status).replace("_", " ")


def _export_mode_value(mode: ExportMode | str) -> str:
    if isinstance(mode, ExportMode):
        return mode.value
    return mode


def export_placeholder_text(
    mode: ExportMode | str,
    status: QuestionnaireRowStatus | str,
) -> str:
    prefix = {
        ExportMode.APPROVED_ONLY.value: "approved",
        ExportMode.LATEST_AVAILABLE.value: "latest",
    }.get(_export_mode_value(mode))
    if prefix is None:
        raise ValueError(f"Unsupported export mode for placeholder text: {mode!r}")
    return (
        f"No {prefix} answer exported due to status: "
        f"{humanize_review_status(status)}."
    )


def approved_only_placeholder_text(status: QuestionnaireRowStatus | str) -> str:
    return export_placeholder_text(ExportMode.APPROVED_ONLY, status)


def latest_available_placeholder_text(status: QuestionnaireRowStatus | str) -> str:
    return export_placeholder_text(ExportMode.LATEST_AVAILABLE, status)
