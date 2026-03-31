from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class IngestResult:
    source_id: str
    status: str
    records: int
    raw_files: int
    error: str | None = None
    notes: str | None = None


@dataclass
class SourceMeta:
    source_id: str
    name: str
    base_url: str
    tipo: str


def as_dict(result: IngestResult) -> dict[str, Any]:
    return {
        "source_id": result.source_id,
        "status": result.status,
        "records": result.records,
        "raw_files": result.raw_files,
        "error": result.error,
        "notes": result.notes,
    }
