from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import urlencode

import requests
from bs4 import BeautifulSoup

from burp.connectors.base import IngestResult
from burp.normalization.name import normalize_header, normalize_name
from burp.settings import PARSER_VERSION
from burp.storage import insert_raw_file, insert_records, update_source_run
from burp.utils import compute_sha256, find_key, now_utc_iso, parse_competencia, parse_decimal, safe_filename

logger = logging.getLogger(__name__)

CONVENIAR_PAGES = [
    ("pessoafisica", "txtPeriodo", "gvPagamentosPessoaFisica", "pessoas_fisicas"),
    ("servidores", "txtPeriodoServidor", "gvPagamentosServidor", "servidores"),
]


def _format_period(start: date, end: date) -> str:
    return f"{start.strftime('%d/%m/%Y')} - {end.strftime('%d/%m/%Y')}"


def _build_url(base_url: str, pagina: str, params: dict[str, str]) -> str:
    url = f"{base_url.rstrip('/')}/Default.aspx"
    params = {**params, "pagina": pagina}
    return url + "?" + urlencode(params)


def _normalize_cpf_digits(value: str | None) -> str | None:
    if not value:
        return None
    digits = re.sub(r"\D", "", str(value))
    return digits if len(digits) == 11 else None


def _format_cpf(value: str | None) -> str | None:
    digits = _normalize_cpf_digits(value)
    if not digits:
        return None
    return f"{digits[:3]}.{digits[3:6]}.{digits[6:9]}-{digits[9:]}"


def _mask_cpf(value: str | None) -> str | None:
    digits = _normalize_cpf_digits(value)
    if not digits:
        return None
    return f"***.{digits[3:6]}.{digits[6:9]}-**"


def _parse_conveniar_date(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _parse_conveniar_competencia(value: object) -> str | None:
    iso_date = _parse_conveniar_date(value)
    if iso_date:
        if iso_date.startswith("0001-01-01"):
            return None
        return iso_date[:7]
    competencia = parse_competencia(value)
    if not competencia or competencia.startswith("0001"):
        return None
    return competencia


def _iter_period_windows(start: date, end: date, max_days: int = 31) -> list[tuple[date, date]]:
    if end < start:
        return []
    windows: list[tuple[date, date]] = []
    current = start
    while current <= end:
        window_end = min(current + timedelta(days=max_days - 1), end)
        windows.append((current, window_end))
        current = window_end + timedelta(days=1)
    return windows


def _resolve_period(
    start_date: date | None,
    end_date: date | None,
    configured_start_date: date | None,
    configured_end_date: date | None,
    default_days: int,
) -> tuple[date, date]:
    if start_date and end_date:
        return start_date, end_date
    if configured_start_date and configured_end_date:
        return configured_start_date, configured_end_date
    end = date.today()
    days = default_days if default_days > 0 else 30
    start = end - timedelta(days=days)
    return start, end


def _extract_conveniar_rows(html: str, table_id: str) -> list[dict[str, object]]:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", id=table_id)
    if not table:
        return []

    header_cells = table.find_all("th")
    headers = [normalize_header(cell.get_text(" ", strip=True)) for cell in header_cells]
    keep_indexes = [idx for idx, header in enumerate(headers) if header]
    normalized_headers = [headers[idx] for idx in keep_indexes]
    rows: list[dict[str, object]] = []

    for row in table.find_all("tr"):
        if "empty-content" in (row.get("class") or []):
            continue
        cells = row.find_all("td", recursive=False)
        if not cells:
            continue
        values = [cell.get_text(" ", strip=True) for cell in cells]
        summary_values = [values[idx] for idx in keep_indexes if idx < len(values)]
        summary = dict(zip(normalized_headers, summary_values)) if len(summary_values) == len(normalized_headers) else {}

        pagamentos_attr = row.get("data-pagamentos")
        pagamentos: list[dict[str, object]] = []
        if pagamentos_attr:
            try:
                parsed = json.loads(pagamentos_attr)
                if isinstance(parsed, list):
                    pagamentos = [item for item in parsed if isinstance(item, dict)]
            except json.JSONDecodeError:
                logger.warning("conveniar detail payload could not be decoded for table=%s", table_id)

        if pagamentos:
            for pagamento in pagamentos:
                rows.append({**summary, **pagamento, "_conveniar_detail": pagamento})
            continue

        if summary:
            rows.append(summary)

    return rows


def _map_conveniar_rows(
    rows,
    source_id: str,
    raw_id: int,
    source_url: str,
    collected_at: str,
    categoria: str,
    periodo: str | None,
    orgao: str,
    default_cpf: str | None = None,
) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        keys = list(row.keys())
        name_key = find_key(keys, ["nome", "favorecido", "beneficiario", "credor"]) or (keys[-1] if keys else None)
        valor_key = find_key(keys, ["valor", "pagamento", "remuneracao", "liquido", "bruto"])
        data_key = find_key(keys, ["data", "competencia", "periodo", "mes"])
        cpf_key = find_key(keys, ["cpf", "documento"])
        matricula_key = find_key(keys, ["matricula"])
        name = row.get(name_key) if name_key else None
        if not name:
            continue

        detail = row.get("_conveniar_detail") if isinstance(row.get("_conveniar_detail"), dict) else None
        if detail is None and isinstance(row.get("_facto_detail"), dict):
            detail = row.get("_facto_detail")
        cpf = row.get(cpf_key) if cpf_key else None
        matricula = row.get(matricula_key) if matricula_key else None
        valor = parse_decimal(detail.get("Valor")) if detail else (parse_decimal(row.get(valor_key)) if valor_key else None)
        data_pagamento = _parse_conveniar_date(detail.get("DataPagamento")) if detail else None
        if not data_pagamento and data_key:
            data_pagamento = _parse_conveniar_date(row.get(data_key))
        competencia = _parse_conveniar_competencia(detail.get("DataCompetencia")) if detail else None
        if not competencia and data_key:
            competencia = _parse_conveniar_competencia(row.get(data_key))

        projeto = str(detail.get("NomeConvenio")).strip() if detail and detail.get("NomeConvenio") else None
        tipo_pagamento = str(detail.get("NomeTipoPedido")).strip() if detail and detail.get("NomeTipoPedido") else None
        cod_lancamento = detail.get("CodLancamento") if detail else None
        masked_row_cpf = _mask_cpf(str(cpf)) if cpf else None
        masked_default_cpf = _mask_cpf(default_cpf)

        record = {
            "source_id": source_id,
            "raw_id": raw_id,
            "person_name_original": name,
            "person_name_norm": normalize_name(name),
            "person_hint_id": masked_row_cpf or masked_default_cpf or matricula,
            "uf": "ES",
            "municipio": None,
            "orgao": orgao,
            "tipo_recebimento": "BOLSA",
            "competencia": competencia,
            "data_pagamento": data_pagamento,
            "valor_bruto": valor,
            "descontos": None,
            "valor_liquido": valor,
            "cargo_funcao": tipo_pagamento or projeto,
            "detalhes_json": {
                "categoria": categoria,
                "periodo": periodo,
                "aba": categoria,
                "cod_lancamento": cod_lancamento,
                "referencia_projeto": projeto,
                "tipo_pagamento": tipo_pagamento,
                "cpf": masked_row_cpf or masked_default_cpf,
                "matricula_servidor": matricula,
                "resumo": {
                    key: value
                    for key, value in row.items()
                    if key not in {"_conveniar_detail", "_facto_detail"}
                    and key not in {"CodLancamento", "NomeConvenio", "NomeTipoPedido", "DataPagamento", "DataCompetencia", "Valor"}
                },
                "raw": detail or row,
            },
            "source_url": source_url,
            "collected_at": collected_at,
            "parser_version": PARSER_VERSION,
        }
        records.append(record)
    return records


def ingest_conveniar(
    nome: str | None,
    *,
    cpf: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    source_id: str,
    orgao: str,
    base_url: str,
    data_dir: Path,
    default_days: int,
    window_days: int,
    configured_start_date: date | None = None,
    configured_end_date: date | None = None,
    raw_file_prefix: str | None = None,
) -> IngestResult:
    collected_at = now_utc_iso()
    if not nome:
        update_source_run(source_id, "skipped", collected_at, "name_required")
        return IngestResult(source_id=source_id, status="skipped", records=0, raw_files=0, error="name_required")

    formatted_cpf = _format_cpf(cpf)
    if not formatted_cpf:
        update_source_run(source_id, "skipped", collected_at, "cpf_required")
        return IngestResult(source_id=source_id, status="skipped", records=0, raw_files=0, error="cpf_required")

    try:
        start, end = _resolve_period(
            start_date,
            end_date,
            configured_start_date,
            configured_end_date,
            default_days,
        )
        resolved_window_days = window_days if window_days > 0 else 31
        if resolved_window_days > 365:
            resolved_window_days = 365
        windows = (
            _iter_period_windows(start, end, max_days=resolved_window_days)
            if (end - start).days + 1 > resolved_window_days
            else [(start, end)]
        )

        session = requests.Session()
        session.headers.update({"User-Agent": "Mozilla/5.0"})
        total_records = 0
        raw_files = 0
        window_count = 0
        raw_prefix = raw_file_prefix or source_id

        for pagina, periodo_key, table_id, categoria in CONVENIAR_PAGES:
            for win_start, win_end in windows:
                window_count += 1
                period = _format_period(win_start, win_end)
                params = {
                    periodo_key: period,
                    "txtNome": nome,
                    "txtDocumento": formatted_cpf,
                }
                url = _build_url(base_url, pagina, params)
                resp = session.get(url, timeout=60)
                resp.raise_for_status()
                content = resp.content
                raw_hash = compute_sha256(content)
                filename = safe_filename(
                    f"{raw_prefix}_{pagina}_{win_start.isoformat()}_{win_end.isoformat()}.html",
                    f"{raw_prefix}_{pagina}.html",
                )
                raw_dir = data_dir / "raw" / source_id
                raw_dir.mkdir(parents=True, exist_ok=True)
                local_path = str(raw_dir / filename)
                with open(local_path, "wb") as handle:
                    handle.write(content)
                raw_id = insert_raw_file(
                    source_id=source_id,
                    url=url,
                    collected_at=collected_at,
                    sha256=raw_hash,
                    local_path=local_path,
                    content_type=resp.headers.get("content-type"),
                )
                raw_files += 1
                rows = _extract_conveniar_rows(resp.text, table_id)
                records = _map_conveniar_rows(
                    rows,
                    source_id,
                    raw_id,
                    url,
                    collected_at,
                    categoria,
                    period,
                    orgao,
                    default_cpf=formatted_cpf,
                )
                total_records += insert_records(records)

        update_source_run(source_id, "ok", collected_at, None)
        notes = f"windows={window_count}" if window_count > 1 else None
        return IngestResult(source_id=source_id, status="ok", records=total_records, raw_files=raw_files, notes=notes)
    except Exception as exc:  # pylint: disable=broad-except
        logger.exception("%s ingest failed: %s", source_id, exc)
        update_source_run(source_id, "error", collected_at, str(exc))
        return IngestResult(source_id=source_id, status="error", records=0, raw_files=0, error=str(exc))
