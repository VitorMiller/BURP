from __future__ import annotations

import csv
from io import StringIO
from typing import Iterable

from burp.normalization.name import normalize_header


def decode_bytes(data: bytes) -> str:
    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            continue
    return data.decode("latin-1", errors="ignore")


def sniff_delimiter(sample: str) -> str:
    if sample.count(";") >= sample.count(","):
        return ";"
    return ","


def iter_csv_rows(data: bytes) -> Iterable[dict[str, str]]:
    text = decode_bytes(data)
    sample = "\n".join(text.splitlines()[:5])
    delimiter = sniff_delimiter(sample)
    reader = csv.DictReader(StringIO(text), delimiter=delimiter)
    for row in reader:
        yield {normalize_header(k): (v.strip() if isinstance(v, str) else v) for k, v in row.items()}
