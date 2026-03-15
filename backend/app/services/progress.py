from __future__ import annotations

from collections.abc import Callable
from typing import TypeAlias

ProgressCallback: TypeAlias = Callable[[str], None]


def report_progress(callback: ProgressCallback | None, message: str) -> None:
    if callback is not None:
        callback(message)


def progress_interval(total: int) -> int:
    if total <= 10:
        return 1
    if total <= 50:
        return 5
    return 25


def should_report_progress(completed: int, total: int, *, every: int) -> bool:
    if total <= 0:
        return False
    return completed == 1 or completed == total or completed % every == 0
