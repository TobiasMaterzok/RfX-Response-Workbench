from __future__ import annotations

import re
from pathlib import PurePosixPath

_INVALID_WINDOWS_FILENAME_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1F]+')
_WHITESPACE_RUN = re.compile(r"\s+")
_SEPARATOR_RUN = re.compile(r"-{2,}")
_WINDOWS_RESERVED_BASENAMES = {
    "CON",
    "PRN",
    "AUX",
    "NUL",
    "COM1",
    "COM2",
    "COM3",
    "COM4",
    "COM5",
    "COM6",
    "COM7",
    "COM8",
    "COM9",
    "LPT1",
    "LPT2",
    "LPT3",
    "LPT4",
    "LPT5",
    "LPT6",
    "LPT7",
    "LPT8",
    "LPT9",
}


def _normalize_component(value: str) -> str:
    normalized = _INVALID_WINDOWS_FILENAME_CHARS.sub("-", value)
    normalized = _WHITESPACE_RUN.sub("-", normalized)
    normalized = _SEPARATOR_RUN.sub("-", normalized)
    return normalized.strip(" .-")


def safe_object_key_filename(file_name: str, *, fallback_stem: str = "file") -> str:
    candidate = re.split(r"[\\/]+", str(file_name))[-1].strip().rstrip(". ")
    if not candidate:
        candidate = fallback_stem

    suffix = "".join(PurePosixPath(candidate).suffixes)
    stem = candidate[: -len(suffix)] if suffix else candidate
    normalized_stem = _normalize_component(stem) or fallback_stem
    if normalized_stem.upper() in _WINDOWS_RESERVED_BASENAMES:
        normalized_stem = f"{normalized_stem}-file"

    normalized_suffix = _normalize_component(suffix.lstrip("."))
    if not normalized_suffix:
        return normalized_stem
    return f"{normalized_stem}.{normalized_suffix}"
