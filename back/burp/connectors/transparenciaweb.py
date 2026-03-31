from __future__ import annotations

import logging
from typing import Iterable

import requests

from burp.connectors.base import IngestResult
from burp.normalization.name import normalize_name
from burp.parsers.csv_parser import iter_csv_rows
from burp.settings import PARSER_VERSION, get_settings
from burp.storage import insert_raw_file, insert_records, update_source_run
from burp.utils import compute_sha256, filename_from_url, now_utc_iso, parse_competencia, parse_decimal, find_key

logger = logging.getLogger(__name__)


def _get_years(session: requests.Session, base_url: str) -> list[int]:
    url = f"{base_url}/api/pessoal/anos"
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, dict):
        years = data.get("anos") or data.get("data") or data.get("result") or []
    else:
        years = data
    result = []
    for year in years:
        try:
            if isinstance(year, dict):
                value = year.get("Valor") or year.get("valor") or year.get("Nome") or year.get("nome")
                if value is None:
                    continue
                result.append(int(value))
            else:
                result.append(int(year))
        except (ValueError, TypeError):
            continue
    return sorted(set(result))


def _find_latest_period(session: requests.Session, base_url: str, year: int) -> int | None:
    latest = None
    for period in range(12, 0, -1):
        url = f"{base_url}/api/pessoal/csv?exercicio={year}&periodo={period}"
        try:
            resp = session.get(url, timeout=15)
        except requests.RequestException:
            continue
        if resp.status_code != 200:
            continue
        content = resp.content
        if b";" not in content and b"," not in content:
            continue
        if b"<html" in content[:200].lower():
            continue
        latest = period
    return latest


def _map_records(
    rows: Iterable[dict[str, str]],
    source_id: str,
    municipio: str,
    raw_id: int,
    source_url: str,
    collected_at: str,
    competencia: str,
) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    municipio_norm = normalize_name(municipio)
    for row in rows:
        keys = list(row.keys())
        name_key = find_key(keys, ["nome servidor", "nome", "servidor"])
        orgao_key = find_key(keys, ["orgao", "secretaria"])
        cargo_key = find_key(keys, ["cargo", "funcao"])
        bruto_key = find_key(keys, ["valor bruto", "remuneracao bruta", "remuneracao", "provento"])
        liquido_key = find_key(keys, ["valor liquido", "remuneracao liquida"])
        descontos_key = find_key(keys, ["desconto", "descontos"])
        competencia_key = find_key(keys, ["competencia", "periodo", "mes"])
        matricula_key = find_key(keys, ["matricula", "cpf"])

        name = row.get(name_key) if name_key else None
        if not name:
            continue
        orgao = row.get(orgao_key) if orgao_key else None
        cargo = row.get(cargo_key) if cargo_key else None
        valor_bruto = parse_decimal(row.get(bruto_key)) if bruto_key else None
        valor_liquido = parse_decimal(row.get(liquido_key)) if liquido_key else None
        descontos = parse_decimal(row.get(descontos_key)) if descontos_key else None
        competencia_row = parse_competencia(row.get(competencia_key)) if competencia_key else None
        record = {
            "source_id": source_id,
            "raw_id": raw_id,
            "person_name_original": name,
            "person_name_norm": normalize_name(name),
            "person_hint_id": row.get(matricula_key) if matricula_key else None,
            "uf": "ES",
            "municipio": municipio_norm,
            "orgao": orgao,
            "tipo_recebimento": "FOLHA",
            "competencia": competencia_row or competencia,
            "data_pagamento": None,
            "valor_bruto": valor_bruto,
            "descontos": descontos,
            "valor_liquido": valor_liquido,
            "cargo_funcao": cargo,
            "detalhes_json": {"raw": row},
            "source_url": source_url,
            "collected_at": collected_at,
            "parser_version": PARSER_VERSION,
        }
        records.append(record)
    return records


def ingest_transparenciaweb(source_id: str, municipio: str) -> IngestResult:
    settings = get_settings()
    base_url = settings.vitoria_base_url if source_id == "vitoria_pessoal" else settings.vilavelha_base_url
    session = requests.Session()
    collected_at = now_utc_iso()
    try:
        years = _get_years(session, base_url)
        if not years:
            raise RuntimeError("No years returned")
        year = None
        period = None
        for candidate_year in sorted(set(years), reverse=True):
            candidate_period = _find_latest_period(session, base_url, candidate_year)
            if candidate_period:
                year = candidate_year
                period = candidate_period
                break
        if not year or not period:
            raise RuntimeError("No valid period found")
        csv_url = f"{base_url}/api/pessoal/csv?exercicio={year}&periodo={period}"
        resp = session.get(csv_url, timeout=60)
        resp.raise_for_status()
        content = resp.content
        raw_hash = compute_sha256(content)
        filename = filename_from_url(csv_url, f"{source_id}_{year}_{period}.csv")
        local_path = str(settings.data_dir / "raw" / source_id / filename)
        (settings.data_dir / "raw" / source_id).mkdir(parents=True, exist_ok=True)
        with open(local_path, "wb") as handle:
            handle.write(content)
        raw_id = insert_raw_file(
            source_id=source_id,
            url=csv_url,
            collected_at=collected_at,
            sha256=raw_hash,
            local_path=local_path,
            content_type=resp.headers.get("content-type"),
        )
        rows = list(iter_csv_rows(content))
        competencia = f"{year}-{period:02d}"
        records = _map_records(rows, source_id, municipio, raw_id, csv_url, collected_at, competencia)
        count = insert_records(records)
        update_source_run(source_id, "ok", collected_at, None)
        return IngestResult(source_id=source_id, status="ok", records=count, raw_files=1)
    except Exception as exc:  # pylint: disable=broad-except
        logger.exception("transparenciaweb ingest failed: %s", exc)
        update_source_run(source_id, "error", collected_at, str(exc))
        return IngestResult(source_id=source_id, status="error", records=0, raw_files=0, error=str(exc))
