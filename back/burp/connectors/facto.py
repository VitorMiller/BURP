from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from urllib.parse import urlencode

import requests

from burp.connectors.base import IngestResult
from burp.normalization.name import normalize_name
from burp.parsers.html_parser import iter_table_rows
from burp.settings import PARSER_VERSION, get_settings
from burp.storage import insert_raw_file, insert_records, update_source_run
from burp.utils import compute_sha256, filename_from_url, now_utc_iso, parse_decimal, parse_competencia, find_key

logger = logging.getLogger(__name__)


def _format_period(start: date, end: date) -> str:
    return f"{start.strftime('%d/%m/%Y')} - {end.strftime('%d/%m/%Y')}"


def _build_url(base_url: str, pagina: str, params: dict[str, str]) -> str:
    url = f"{base_url.rstrip('/')}/Default.aspx"
    params = {**params, "pagina": pagina}
    return url + "?" + urlencode(params)


def _default_period() -> tuple[date, date]:
    end = date.today()
    start = end - timedelta(days=30)
    return start, end


def _iter_period_windows(start: date, end: date, max_days: int = 31) -> list[tuple[date, date]]:
    if end < start:
        return []
    windows = []
    current = start
    while current <= end:
        window_end = min(current + timedelta(days=max_days - 1), end)
        windows.append((current, window_end))
        current = window_end + timedelta(days=1)
    return windows


def _map_facto_rows(
    rows,
    source_id: str,
    raw_id: int,
    source_url: str,
    collected_at: str,
    categoria: str,
    periodo: str | None,
) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for row in rows:
        keys = list(row.keys())
        name_key = find_key(keys, ["nome", "favorecido", "beneficiario", "credor"]) or keys[-1]
        valor_key = find_key(keys, ["valor", "pagamento", "remuneracao", "liquido", "bruto"])
        data_key = find_key(keys, ["data", "competencia", "periodo", "mes"]) 
        orgao_key = find_key(keys, ["orgao", "secretaria", "unidade"]) 
        cpf_key = find_key(keys, ["cpf", "documento"])
        matricula_key = find_key(keys, ["matricula"])
        name = row.get(name_key)
        if not name:
            continue
        cpf = row.get(cpf_key) if cpf_key else None
        matricula = row.get(matricula_key) if matricula_key else None
        valor = parse_decimal(row.get(valor_key)) if valor_key else None
        competencia = parse_competencia(row.get(data_key)) if data_key else None
        orgao = row.get(orgao_key) if orgao_key else None
        record = {
            "source_id": source_id,
            "raw_id": raw_id,
            "person_name_original": name,
            "person_name_norm": normalize_name(name),
            "person_hint_id": cpf or matricula,
            "uf": "ES",
            "municipio": None,
            "orgao": orgao,
            "tipo_recebimento": "BOLSA",
            "competencia": competencia,
            "data_pagamento": None,
            "valor_bruto": valor,
            "descontos": None,
            "valor_liquido": valor,
            "cargo_funcao": None,
            "detalhes_json": {"categoria": categoria, "periodo": periodo, "raw": row},
            "source_url": source_url,
            "collected_at": collected_at,
            "parser_version": PARSER_VERSION,
        }
        records.append(record)
    return records


def ingest_facto(nome: str | None, start_date: date | None = None, end_date: date | None = None) -> IngestResult:
    settings = get_settings()
    collected_at = now_utc_iso()
    if not nome:
        update_source_run("facto_conveniar", "skipped", collected_at, "name_required")
        return IngestResult(
            source_id="facto_conveniar",
            status="skipped",
            records=0,
            raw_files=0,
            error="name_required",
        )
    try:
        start, end = (start_date, end_date) if start_date and end_date else _default_period()
        window_days = settings.facto_window_days
        if window_days <= 0:
            window_days = 31
        if window_days > 365:
            window_days = 365
        if (end - start).days + 1 > window_days:
            windows = _iter_period_windows(start, end, max_days=window_days)
        else:
            windows = [(start, end)]
        session = requests.Session()
        total_records = 0
        raw_files = 0
        window_count = 0
        for pagina, periodo_key, table_id, categoria in [
            ("pessoafisica", "txtPeriodo", "gvPagamentosPessoaFisica", "pessoas_fisicas"),
            ("servidores", "txtPeriodoServidor", "gvPagamentosServidor", "servidores"),
        ]:
            for win_start, win_end in windows:
                window_count += 1
                period = _format_period(win_start, win_end)
                params = {
                    periodo_key: period,
                    "txtNome": nome,
                    "txtDocumento": "",
                }
                url = _build_url(settings.facto_base_url, pagina, params)
                resp = session.get(url, timeout=60)
                resp.raise_for_status()
                content = resp.content
                raw_hash = compute_sha256(content)
                filename = filename_from_url(url, f"facto_{pagina}_{win_start}_{win_end}.html")
                local_path = str(settings.data_dir / "raw" / "facto_conveniar" / filename)
                (settings.data_dir / "raw" / "facto_conveniar").mkdir(parents=True, exist_ok=True)
                with open(local_path, "wb") as handle:
                    handle.write(content)
                raw_id = insert_raw_file(
                    source_id="facto_conveniar",
                    url=url,
                    collected_at=collected_at,
                    sha256=raw_hash,
                    local_path=local_path,
                    content_type=resp.headers.get("content-type"),
                )
                raw_files += 1
                rows = list(iter_table_rows(resp.text, table_id))
                records = _map_facto_rows(rows, "facto_conveniar", raw_id, url, collected_at, categoria, period)
                total_records += insert_records(records)
        update_source_run("facto_conveniar", "ok", collected_at, None)
        notes = f"windows={window_count}" if window_count > 1 else None
        return IngestResult(
            source_id="facto_conveniar",
            status="ok",
            records=total_records,
            raw_files=raw_files,
            notes=notes,
        )
    except Exception as exc:  # pylint: disable=broad-except
        logger.exception("facto ingest failed: %s", exc)
        update_source_run("facto_conveniar", "error", collected_at, str(exc))
        return IngestResult(source_id="facto_conveniar", status="error", records=0, raw_files=0, error=str(exc))
