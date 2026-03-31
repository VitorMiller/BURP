from __future__ import annotations

from calendar import monthrange
from collections import defaultdict
from datetime import date, datetime
import re
from typing import Any, Iterable

from burp.analysis.ceiling import build_ceiling_reference, resolve_constitutional_ceiling


def _round_money(value: float) -> float:
    rounded = round(float(value), 2)
    return 0.0 if abs(rounded) < 0.005 else rounded


def _record_value(record: dict[str, Any]) -> float | None:
    value = record.get("valor_liquido")
    if value is None:
        value = record.get("valor_bruto")
    if value is None:
        return None
    return float(value)


def _origin_label(record: dict[str, Any]) -> str:
    origin = record.get("orgao") or record.get("source_name") or record.get("source_id") or "DESCONHECIDO"
    return str(origin).strip() or "DESCONHECIDO"


def _coerce_date(value: Any, allow_year_only: bool = False) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value

    text = str(value).strip()
    if not text:
        return None

    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue

    if len(text) >= 7:
        try:
            return datetime.strptime(text[:7], "%Y-%m").date()
        except ValueError:
            pass

    if "/" in text and len(text) >= 7:
        try:
            month_text, year_text = text[:7].split("/")
            return date(int(year_text), int(month_text), 1)
        except (TypeError, ValueError):
            pass

    if allow_year_only and len(text) == 4 and text.isdigit():
        return date(int(text), 1, 1)

    return None


def _extract_period_end_date(record: dict[str, Any]) -> date | None:
    detalhes = record.get("detalhes_json")
    if not isinstance(detalhes, dict):
        return None
    periodo = detalhes.get("periodo")
    if not periodo:
        return None
    candidates = re.findall(r"\d{2}/\d{2}/\d{4}|\d{4}-\d{2}-\d{2}", str(periodo))
    matches = [_coerce_date(candidate) for candidate in candidates]
    valid = [item for item in matches if item]
    return valid[-1] if valid else None


def extract_record_date(record: dict[str, Any]) -> date | None:
    for key in ("data_pagamento",):
        parsed = _coerce_date(record.get(key))
        if parsed:
            return parsed

    detalhes = record.get("detalhes_json")
    raw = detalhes.get("raw") if isinstance(detalhes, dict) else None
    if isinstance(raw, dict):
        for key in (
            "data de credito",
            "data_credito",
            "data pagamento",
            "data_pagamento",
            "data",
        ):
            parsed = _coerce_date(raw.get(key))
            if parsed:
                return parsed

    parsed_period = _extract_period_end_date(record)
    if parsed_period:
        return parsed_period

    competencia = record.get("competencia")
    parsed_competencia = _coerce_date(competencia)
    if parsed_competencia and "-" in str(competencia):
        return parsed_competencia

    return None


def extract_record_month_key(record: dict[str, Any]) -> str | None:
    parsed = extract_record_date(record)
    if not parsed:
        return None
    return parsed.strftime("%Y-%m")


def _iter_month_starts(start_date: date, end_date: date) -> Iterable[date]:
    current = start_date.replace(day=1)
    end_marker = end_date.replace(day=1)
    while current <= end_marker:
        yield current
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)


def _month_end(month_start: date) -> date:
    return month_start.replace(day=monthrange(month_start.year, month_start.month)[1])


def build_period_report(records: list[dict[str, Any]], start_date: date, end_date: date) -> dict[str, Any]:
    month_starts = list(_iter_month_starts(start_date, end_date))
    month_slots = {month.strftime("%Y-%m"): month for month in month_starts}
    bucketed: dict[str, dict[str, Any]] = {
        month_key: {
            "records": [],
            "totals_by_tipo": {"FOLHA": 0.0, "BOLSA": 0.0, "DIARIA": 0.0},
            "totals_by_source": defaultdict(float),
            "totals_by_orgao": defaultdict(float),
            "total": 0.0,
        }
        for month_key in month_slots
    }

    totals_by_tipo: dict[str, float] = {"FOLHA": 0.0, "BOLSA": 0.0, "DIARIA": 0.0}
    totals_by_source: dict[str, float] = defaultdict(float)
    matched_records = 0
    records_without_month = 0
    records_outside_period = 0
    monthly_records = 0

    for record in records:
        value = _record_value(record)
        if value is None:
            continue
        record_date = extract_record_date(record)
        if not record_date:
            records_without_month += 1
            continue
        if record_date < start_date or record_date > end_date:
            records_outside_period += 1
            continue
        month_key = record_date.strftime("%Y-%m")
        if month_key not in bucketed:
            records_outside_period += 1
            continue

        tipo = str(record.get("tipo_recebimento") or "OUTROS").upper()
        source_id = record.get("source_id") or "unknown"
        orgao = _origin_label(record)
        bucket = bucketed[month_key]
        bucket["records"].append(record)
        bucket["total"] += value
        bucket["totals_by_source"][source_id] += value
        bucket["totals_by_orgao"][orgao] += value
        if tipo not in bucket["totals_by_tipo"]:
            bucket["totals_by_tipo"][tipo] = 0.0
        bucket["totals_by_tipo"][tipo] += value

        if tipo not in totals_by_tipo:
            totals_by_tipo[tipo] = 0.0
        totals_by_tipo[tipo] += value
        totals_by_source[source_id] += value
        matched_records += 1
        monthly_records += 1

    months_payload: list[dict[str, Any]] = []
    years_in_scope = {month.year for month in month_starts}
    months_over_ceiling = 0
    max_month_total = 0.0
    max_month_key: str | None = None

    for month_key in sorted(month_slots):
        month_start = month_slots[month_key]
        ceiling = resolve_constitutional_ceiling(month_start.year)
        bucket = bucketed[month_key]
        total = _round_money(bucket["total"])
        exceeds = total > ceiling["value"]
        if exceeds:
            months_over_ceiling += 1
        if total > max_month_total:
            max_month_total = total
            max_month_key = month_key
        months_payload.append(
            {
                "month": month_key,
                "start": month_start.isoformat(),
                "end": _month_end(month_start).isoformat(),
                "total": total,
                "teto_constitucional": ceiling["value"],
                "teto_reference_year": ceiling["reference_year"],
                "estourou_teto": exceeds,
                "excesso": _round_money(max(total - ceiling["value"], 0.0)),
                "records_count": len(bucket["records"]),
                "totals_by_tipo": {
                    key: _round_money(value) for key, value in bucket["totals_by_tipo"].items() if value
                },
                "totals_by_source": {
                    key: _round_money(value)
                    for key, value in sorted(bucket["totals_by_source"].items(), key=lambda item: item[1], reverse=True)
                },
                "totals_by_orgao": {
                    key: _round_money(value)
                    for key, value in sorted(bucket["totals_by_orgao"].items(), key=lambda item: item[1], reverse=True)
                },
            }
        )

    total_period = _round_money(sum(month["total"] for month in months_payload))
    return {
        "period": {
            "start": start_date.isoformat(),
            "end": end_date.isoformat(),
            "months_in_scope": len(months_payload),
        },
        "calculation_basis": "valor_liquido_or_valor_bruto",
        "ceiling_reference": build_ceiling_reference(years_in_scope),
        "monthly": months_payload,
        "totals": {
            "overall": total_period,
            "average_per_month": _round_money(total_period / len(months_payload)) if months_payload else 0.0,
            "by_tipo": {key: _round_money(value) for key, value in totals_by_tipo.items() if value},
            "by_source": {
                key: _round_money(value)
                for key, value in sorted(totals_by_source.items(), key=lambda item: item[1], reverse=True)
            },
            "months_over_ceiling": months_over_ceiling,
            "max_month_total": _round_money(max_month_total),
            "max_month": max_month_key,
        },
        "coverage": {
            "records_considered": matched_records,
            "records_with_month": monthly_records,
            "records_without_month": records_without_month,
            "records_outside_period": records_outside_period,
        },
        "notes": [
            "O relatório soma FOLHA, BOLSA e DIARIA encontrados para o nome consultado.",
            "O teto é uma referência anual configurável no backend; subtetos específicos podem exigir validação jurídica complementar.",
        ],
    }
