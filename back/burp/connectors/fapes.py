from __future__ import annotations

import logging
import re

import requests

from burp.connectors.base import IngestResult
from burp.connectors.ckan import ckan_package_search, ckan_package_show
from burp.normalization.name import normalize_name
from burp.parsers.xlsx_parser import iter_xlsx_rows
from burp.settings import PARSER_VERSION, get_settings
from burp.storage import insert_raw_file, insert_records, update_source_run
from burp.utils import compute_sha256, filename_from_url, now_utc_iso, parse_competencia, parse_decimal, find_key

logger = logging.getLogger(__name__)
FAPES_MIN_YEAR = 2024


def _find_dataset(base_url: str) -> dict:
    search = ckan_package_search(base_url, "bolsas auxilios fapes")
    for item in search.get("results", []):
        name = (item.get("title") or "").lower()
        if "fapes" in name and ("bolsa" in name or "auxilio" in name):
            return ckan_package_show(base_url, item.get("name"))
    raise RuntimeError("FAPES dataset not found via search")


def _parse_year(value: str) -> int | None:
    match = re.search(r"(\d{4})", value)
    if match:
        return int(match.group(1))
    return None


def _select_xlsx_resources(resources: list[dict], min_year: int = FAPES_MIN_YEAR) -> list[dict]:
    candidates = []
    seen_keys: set[tuple[str, str]] = set()
    for res in resources:
        name = res.get("name", "")
        url = res.get("url", "")
        if ".xlsx" not in name.lower() and ".xlsx" not in url.lower():
            continue
        dedupe_key = (name, url)
        if dedupe_key in seen_keys:
            continue
        seen_keys.add(dedupe_key)
        year = _parse_year(name) or _parse_year(url)
        candidates.append((year or 0, name.lower(), url.lower(), res))
    if not candidates:
        return []

    selected = [item for item in candidates if item[0] >= min_year]
    if selected:
        selected.sort(key=lambda item: (item[0], item[1], item[2]))
        return [item[3] for item in selected]

    candidates.sort(key=lambda item: (item[0], item[1], item[2]))
    return [candidates[-1][3]]


def ingest_fapes() -> IngestResult:
    settings = get_settings()
    collected_at = now_utc_iso()
    try:
        dataset = _find_dataset(settings.ckan_base_url)
        resources = dataset.get("resources", [])
        selected_resources = _select_xlsx_resources(resources)
        if not selected_resources:
            raise RuntimeError("No XLSX resource found")
        count = 0
        raw_files = 0
        for resource in selected_resources:
            url = resource.get("url")
            if not url:
                raise RuntimeError("FAPES resource missing URL")
            resp = requests.get(url, timeout=120)
            resp.raise_for_status()
            content = resp.content
            raw_hash = compute_sha256(content)
            year = _parse_year(resource.get("name", "")) or _parse_year(url)
            filename = filename_from_url(url, f"fapes-{year or 'sem-ano'}.xlsx")
            local_path = str(settings.data_dir / "raw" / "fapes_bolsas" / filename)
            (settings.data_dir / "raw" / "fapes_bolsas").mkdir(parents=True, exist_ok=True)
            with open(local_path, "wb") as handle:
                handle.write(content)
            raw_id = insert_raw_file(
                source_id="fapes_bolsas",
                url=url,
                collected_at=collected_at,
                sha256=raw_hash,
                local_path=local_path,
                content_type=resp.headers.get("content-type"),
            )
            records = []
            for row in iter_xlsx_rows(local_path):
                keys = list(row.keys())
                name_key = find_key(keys, ["nome", "beneficiario", "bolsista"])
                programa_key = find_key(keys, ["programa", "modalidade", "projeto", "perfil"])
                valor_key = find_key(keys, ["valor", "valor pago", "valor total"])
                competencia_key = find_key(keys, ["competencia", "mes", "referencia"])
                data_credito_key = find_key(keys, ["data de credito", "data credito", "data pagamento", "data"])
                matricula_key = find_key(keys, ["cpf", "matricula"])

                name = row.get(name_key) if name_key else None
                if not name:
                    continue
                programa = row.get(programa_key) if programa_key else None
                valor = parse_decimal(row.get(valor_key)) if valor_key else None
                data_credito = row.get(data_credito_key) if data_credito_key else None
                competencia = parse_competencia(row.get(competencia_key)) if competencia_key else None
                competencia_data = parse_competencia(data_credito) if data_credito else None
                record = {
                    "source_id": "fapes_bolsas",
                    "raw_id": raw_id,
                    "person_name_original": name,
                    "person_name_norm": normalize_name(name),
                    "person_hint_id": row.get(matricula_key) if matricula_key else None,
                    "uf": "ES",
                    "municipio": None,
                    "orgao": "FAPES",
                    "tipo_recebimento": "BOLSA",
                    "competencia": competencia or competencia_data or (str(year) if year else None),
                    "data_pagamento": str(data_credito).split(" ", 1)[0] if data_credito else None,
                    "valor_bruto": valor,
                    "descontos": None,
                    "valor_liquido": valor,
                    "cargo_funcao": programa,
                    "detalhes_json": {"programa": programa, "raw": row},
                    "source_url": url,
                    "collected_at": collected_at,
                    "parser_version": PARSER_VERSION,
                }
                records.append(record)
            count += insert_records(records)
            raw_files += 1
        update_source_run("fapes_bolsas", "ok", collected_at, None)
        return IngestResult(source_id="fapes_bolsas", status="ok", records=count, raw_files=raw_files)
    except Exception as exc:  # pylint: disable=broad-except
        logger.exception("fapes ingest failed: %s", exc)
        update_source_run("fapes_bolsas", "error", collected_at, str(exc))
        return IngestResult(source_id="fapes_bolsas", status="error", records=0, raw_files=0, error=str(exc))
