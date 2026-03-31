from __future__ import annotations

from typing import Iterable

from bs4 import BeautifulSoup

from burp.normalization.name import normalize_header


def iter_table_rows(html: str, table_id: str) -> Iterable[dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", id=table_id)
    if not table:
        return
    header_cells = table.find_all("th")
    headers = [normalize_header(cell.get_text(" ", strip=True)) for cell in header_cells]
    keep_indexes = [idx for idx, header in enumerate(headers) if header]
    headers = [headers[idx] for idx in keep_indexes]
    for row in table.find_all("tr"):
        cells = row.find_all("td")
        if not cells:
            continue
        values = [cell.get_text(" ", strip=True) for cell in cells]
        values = [values[idx] for idx in keep_indexes if idx < len(values)]
        if len(values) != len(headers):
            continue
        yield dict(zip(headers, values))
