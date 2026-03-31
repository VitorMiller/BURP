from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlparse


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def compute_sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def safe_filename(value: str, fallback: str) -> str:
    name = re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("_")
    return name or fallback


def filename_from_url(url: str, fallback: str) -> str:
    try:
        parsed = urlparse(url)
        name = Path(parsed.path).name
        if name:
            return safe_filename(name, fallback)
    except Exception:
        pass
    return safe_filename(fallback, fallback)


def dump_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"))


def parse_decimal(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    text = text.replace("R$", "").strip()
    # Handle Brazilian formats like 1.234,56
    if "," in text and "." in text:
        text = text.replace(".", "").replace(",", ".")
    else:
        text = text.replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def parse_competencia(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    # Normalize common formats: MM/YYYY, YYYY-MM, YYYYMM
    match = re.search(r"(\d{2})[/-](\d{4})", text)
    if match:
        month, year = match.groups()
        return f"{year}-{month}"
    match = re.search(r"(\d{4})[/-](\d{2})", text)
    if match:
        year, month = match.groups()
        return f"{year}-{month}"
    match = re.search(r"(\d{4})(\d{2})", text)
    if match:
        year, month = match.groups()
        return f"{year}-{month}"
    match = re.search(r"(\d{4})", text)
    if match:
        return match.group(1)
    return None


def find_key(keys: Iterable[str], candidates: Iterable[str]) -> str | None:
    for candidate in candidates:
        for key in keys:
            if candidate in key:
                return key
    return None


def batch(iterable: Iterable[Any], size: int) -> Iterable[list[Any]]:
    chunk: list[Any] = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk
