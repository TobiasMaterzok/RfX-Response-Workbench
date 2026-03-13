from __future__ import annotations

from dataclasses import dataclass

from app.config import Settings
from app.exceptions import ValidationFailure
from app.services.hashing import sha256_hex


@dataclass(frozen=True)
class StoredObject:
    object_key: str
    size_bytes: int
    file_hash: str
    payload: bytes


class LocalObjectStorage:
    def __init__(self, settings: Settings) -> None:
        self._root = settings.storage_root.resolve()
        self._root.mkdir(parents=True, exist_ok=True)

    def save_bytes(self, object_key: str, payload: bytes) -> StoredObject:
        if not payload:
            raise ValidationFailure(f"Cannot store empty payload for object_key={object_key}.")
        target_path = self._root / object_key
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_bytes(payload)
        return StoredObject(
            object_key=object_key,
            size_bytes=len(payload),
            file_hash=sha256_hex(payload),
            payload=payload,
        )

    def read_bytes(self, object_key: str) -> bytes:
        target_path = self._root / object_key
        if not target_path.exists():
            raise ValidationFailure(f"Stored object is missing for object_key={object_key}.")
        return target_path.read_bytes()
