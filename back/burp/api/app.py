from __future__ import annotations

import json
import re
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any

from dotenv import load_dotenv
from fastapi import Body, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from burp.analysis import build_period_report, extract_record_month_key, resolve_record_amount
from burp.connectors.facto import ingest_facto
from burp.connectors.fest import ingest_fest
from burp.connectors.fapes import ingest_fapes
from burp.connectors.portal_federal import (
    ingest_portal_federal_for_cpfs,
    ingest_portal_federal_for_names,
    mes_anos_for_period,
)
from burp.connectors.sources import list_sources_meta
from burp.er.clustering import cluster_id_for_record, cluster_records
from burp.ingest import run_ingest
from burp.normalization.name import normalize_name
from burp.normalization.recebimento import normalize_tipo, normalize_tipo_filter
from burp.settings import get_settings
from burp.storage import (
    ensure_sources,
    get_cluster,
    init_db,
    list_records_for_cluster,
    list_sources,
    search_records,
)

load_dotenv()
init_db()
ensure_sources([meta.__dict__ for meta in list_sources_meta()])

app = FastAPI(title="BURP ES - Auditoria de Recebimentos Publicos")
app.add_middleware(
    CORSMiddleware,
    allow_origins=get_settings().api_cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _extract_month_key(record: dict[str, Any]) -> str | None:
    return extract_record_month_key(record)


def _round_money(value: float) -> float:
    rounded = round(float(value), 2)
    return 0.0 if abs(rounded) < 0.005 else rounded


def _origin_label(record: dict[str, Any]) -> str:
    origin = record.get("orgao") or record.get("source_name") or record.get("source_id") or "DESCONHECIDO"
    return str(origin).strip() or "DESCONHECIDO"


def _build_simple_monthly_summary(records: list[dict[str, Any]]) -> dict[str, Any]:
    monthly_buckets: dict[str, dict[tuple[str, str], float]] = {}
    monthly_totals: dict[str, float] = {}
    totals_by_tipo: dict[str, float] = {"FOLHA": 0.0, "BOLSA": 0.0, "DIARIA": 0.0}
    total_value = 0.0
    valid_months: set[str] = set()

    for record in records:
        value = resolve_record_amount(record)
        if value is None:
            continue
        value_float = float(value)
        total_value += value_float
        tipo = record.get("tipo_recebimento") or "OUTROS"
        if tipo not in totals_by_tipo:
            totals_by_tipo[tipo] = 0.0
        totals_by_tipo[tipo] += value_float
        month_key = _extract_month_key(record) or "SEM_DATA"
        if month_key != "SEM_DATA":
            valid_months.add(month_key)
        origin = _origin_label(record)
        bucket = monthly_buckets.setdefault(month_key, {})
        key = (origin, tipo)
        bucket[key] = bucket.get(key, 0.0) + value_float
        monthly_totals[month_key] = monthly_totals.get(month_key, 0.0) + value_float

    valid_months_sorted = sorted(valid_months)
    month_range_start = valid_months_sorted[0] if valid_months_sorted else None
    month_range_end = valid_months_sorted[-1] if valid_months_sorted else None
    months_in_range = 0
    if month_range_start and month_range_end:
        start_year, start_month = [int(part) for part in month_range_start.split("-")]
        end_year, end_month = [int(part) for part in month_range_end.split("-")]
        year, month = start_year, start_month
        while (year, month) <= (end_year, end_month):
            months_in_range += 1
            month += 1
            if month > 12:
                month = 1
                year += 1

    monthly_total_value = sum(monthly_totals.get(month, 0.0) for month in valid_months_sorted)
    monthly_avg_all = monthly_total_value / months_in_range if months_in_range else 0.0

    valid_desc = sorted(valid_months, reverse=True)
    other_months = sorted([month for month in monthly_buckets.keys() if month not in valid_months])
    months_sorted = valid_desc + other_months

    monthly = []
    for month in months_sorted:
        bucket = monthly_buckets.get(month, {})
        entries = []
        for (origin, tipo), value in sorted(bucket.items(), key=lambda item: item[1], reverse=True):
            entries.append(
                {
                    "origem": origin,
                    "tipo": tipo,
                    "valor": _round_money(value),
                }
            )
        monthly.append(
            {
                "month": month,
                "items": entries,
                "total": _round_money(monthly_totals.get(month, 0.0)),
            }
        )

    return {
        "monthly": monthly,
        "totals": {
            "by_tipo": {key: _round_money(value) for key, value in totals_by_tipo.items()},
            "overall": _round_money(total_value),
            "monthly_avg": _round_money(monthly_avg_all),
            "months_with_data": len(valid_months_sorted),
            "months_in_range": months_in_range,
        },
    }


def _build_monthly_summary(records: list[dict[str, Any]]) -> dict[str, Any]:
    totals: dict[str, float] = {}
    totals_by_tipo: dict[str, dict[str, float]] = {"FOLHA": {}, "BOLSA": {}, "DIARIA": {}}
    records_with_value_total = 0
    records_with_value_monthly = 0
    total_value = 0.0
    total_value_by_source: dict[str, float] = {}
    total_value_by_tipo: dict[str, float] = {"FOLHA": 0.0, "BOLSA": 0.0, "DIARIA": 0.0}
    for record in records:
        value = resolve_record_amount(record)
        if value is None:
            continue
        records_with_value_total += 1
        value_float = float(value)
        total_value += value_float
        source_id = record.get("source_id") or "unknown"
        total_value_by_source[source_id] = total_value_by_source.get(source_id, 0.0) + value_float
        tipo = record.get("tipo_recebimento")
        if tipo in total_value_by_tipo:
            total_value_by_tipo[tipo] += value_float
        month_key = _extract_month_key(record)
        if not month_key:
            continue
        records_with_value_monthly += 1
        totals[month_key] = totals.get(month_key, 0.0) + value_float
        if tipo in totals_by_tipo:
            totals_by_tipo[tipo][month_key] = totals_by_tipo[tipo].get(month_key, 0.0) + value_float

    months = sorted(totals.keys())
    monthly_total_value = sum(totals.values())
    avg = monthly_total_value / len(months) if months else 0.0
    month_range_start = months[0] if months else None
    month_range_end = months[-1] if months else None
    months_in_range = 0
    if month_range_start and month_range_end:
        start_year, start_month = [int(part) for part in month_range_start.split("-")]
        end_year, end_month = [int(part) for part in month_range_end.split("-")]
        year, month = start_year, start_month
        while (year, month) <= (end_year, end_month):
            months_in_range += 1
            month += 1
            if month > 12:
                month = 1
                year += 1
    avg_inclusive = monthly_total_value / months_in_range if months_in_range else 0.0
    avg_by_tipo = {}
    avg_by_tipo_inclusive = {}
    for tipo, values in totals_by_tipo.items():
        if values:
            total_tipo = sum(values.values())
            avg_by_tipo[tipo] = total_tipo / len(values)
            avg_by_tipo_inclusive[tipo] = total_tipo / months_in_range if months_in_range else 0.0
    return {
        "total_value": total_value,
        "total_value_unassigned": total_value - monthly_total_value,
        "monthly_total_value": monthly_total_value,
        "monthly_total_avg": avg,
        "monthly_total_avg_inclusive": avg_inclusive,
        "monthly_total_avg_by_tipo": avg_by_tipo,
        "monthly_total_avg_inclusive_by_tipo": avg_by_tipo_inclusive,
        "total_value_by_tipo": total_value_by_tipo,
        "total_value_by_source": total_value_by_source,
        "facto_total_value": total_value_by_source.get("facto_conveniar", 0.0),
        "fest_total_value": total_value_by_source.get("fest_conveniar", 0.0),
        "monthly_totals": totals,
        "month_range": {
            "start": month_range_start,
            "end": month_range_end,
            "months_with_data": len(months),
            "months_in_range": months_in_range,
            "missing_months": max(months_in_range - len(months), 0),
        },
        "records_with_value": records_with_value_total,
        "records_with_value_monthly": records_with_value_monthly,
        "records_without_month": records_with_value_total - records_with_value_monthly,
        "currency": "BRL",
    }


def _parse_cpfs(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def _mask_cpf(value: str) -> str:
    digits = re.sub(r"\D", "", value)
    if len(digits) != 11:
        return value
    return f"***.{digits[3:6]}.{digits[6:9]}-**"


def _normalize_cpf_digits(value: str | None) -> str | None:
    if not value:
        return None
    digits = re.sub(r"\D", "", str(value))
    return digits if len(digits) == 11 else None


def _extract_masked_cpf(value: Any) -> str | None:
    match = re.search(r"([0-9*]{3}\.[0-9*]{3}\.[0-9*]{3}-[0-9*]{2})", str(value or ""))
    if not match:
        return None
    return match.group(1)


def _record_matches_cpf(record: dict[str, Any], cpf_digits: str | None) -> bool:
    if not cpf_digits:
        return True
    target_masked = _mask_cpf(cpf_digits)
    candidates: list[Any] = [record.get("person_hint_id")]
    detalhes = record.get("detalhes_json")
    if isinstance(detalhes, dict):
        candidates.extend([detalhes.get("cpf"), detalhes.get("matricula_servidor")])
        raw = detalhes.get("raw")
        if isinstance(raw, dict):
            candidates.extend([raw.get("cpf"), raw.get("CPF"), raw.get("documento")])

    has_cpf_evidence = False
    for candidate in candidates:
        if candidate is None:
            continue
        candidate_digits = re.sub(r"\D", "", str(candidate))
        candidate_masked = _extract_masked_cpf(candidate)
        if len(candidate_digits) == 11 or candidate_masked:
            has_cpf_evidence = True
        if len(candidate_digits) == 11 and candidate_digits == cpf_digits:
            return True
        if candidate_masked and candidate_masked == target_masked:
            return True
    return not has_cpf_evidence


def _filter_records_by_cpf(records: list[dict[str, Any]], cpf: str | None) -> list[dict[str, Any]]:
    cpf_digits = _normalize_cpf_digits(cpf)
    if not cpf_digits:
        return records
    return [record for record in records if _record_matches_cpf(record, cpf_digits)]


def _is_conveniar_source(source_id: Any) -> bool:
    return str(source_id or "").endswith("_conveniar")


def _is_legacy_conveniar_window_summary(record: dict[str, Any]) -> bool:
    if not _is_conveniar_source(record.get("source_id")):
        return False
    if record.get("competencia") or record.get("data_pagamento"):
        return False
    detalhes = record.get("detalhes_json")
    if not isinstance(detalhes, dict):
        return False
    raw = detalhes.get("raw")
    if isinstance(raw, dict) and raw.get("CodLancamento") is not None:
        return False
    return True


def _parse_date_param(value: str | None, field_name: str) -> date | None:
    if value is None or not str(value).strip():
        return None
    try:
        return datetime.strptime(str(value).strip(), "%Y-%m-%d").date()
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=f"{field_name}_must_be_yyyy_mm_dd") from exc


def _resolve_period_bounds(data_inicio: str | None, data_fim: str | None) -> tuple[date, date] | None:
    start_date = _parse_date_param(data_inicio, "data_inicio")
    end_date = _parse_date_param(data_fim, "data_fim")
    if not start_date and not end_date:
        return None
    if start_date and not end_date:
        end_date = date.today()
    if end_date and not start_date:
        start_date = date(end_date.year, 1, 1)
    if start_date and end_date and start_date > end_date:
        raise HTTPException(status_code=422, detail="data_inicio_must_be_before_or_equal_data_fim")
    return start_date, end_date


def _normalize_search_filters(
    nome: str,
    uf: str,
    municipio: str | None,
    tipo: str,
) -> dict[str, Any]:
    name_norm = normalize_name(nome)
    uf_norm = uf.upper() if uf else None
    if uf_norm in {"TODOS", "ALL"}:
        uf_norm = None
    municipio_norm = normalize_name(municipio) if municipio else None
    tipo_norm = normalize_tipo(tipo) or "TODOS"
    tipo_filter = normalize_tipo_filter(tipo)
    return {
        "nome": nome,
        "nome_norm": name_norm,
        "uf": uf,
        "uf_norm": uf_norm,
        "municipio": municipio,
        "municipio_norm": municipio_norm,
        "tipo": tipo,
        "tipo_norm": tipo_norm,
        "tipo_filter": tipo_filter,
    }


def _build_match_context(records: list[dict[str, Any]], clusters: list[dict[str, Any]]) -> dict[str, Any]:
    hint_ids = sorted({value for value in (record.get("person_hint_id") for record in records) if value})
    municipios = sorted({value for value in (record.get("municipio") for record in records) if value})
    orgaos = sorted({value for value in (record.get("orgao") for record in records) if value})
    homonym_risk = "LOW"
    if len(hint_ids) > 1:
        homonym_risk = "HIGH"
    elif len(clusters) > 1 or len(orgaos) > 1 or len(municipios) > 1:
        homonym_risk = "MEDIUM"
    notes = [
        "A análise consolida todos os registros retornados pelo nome pesquisado.",
    ]
    if homonym_risk != "LOW":
        notes.append(
            "Há múltiplos sinais de identidade (clusters, órgãos, municípios ou hint ids). Revise os agrupamentos antes de concluir irregularidade."
        )
    return {
        "clusters": len(clusters),
        "distinct_person_hint_ids": hint_ids,
        "distinct_municipios": municipios,
        "distinct_orgaos": orgaos,
        "homonym_risk": homonym_risk,
        "notes": notes,
    }


def _resolve_conveniar_period(
    start_date: date | None,
    end_date: date | None,
    configured_start_date: date | None,
    configured_end_date: date | None,
    fallback_days: int,
) -> tuple[date, date]:
    resolved_start = start_date
    resolved_end = end_date
    if resolved_start and not resolved_end:
        resolved_end = date.today()
    elif resolved_end and not resolved_start:
        resolved_start = date(resolved_end.year, 1, 1)
    elif not resolved_start and not resolved_end:
        if configured_start_date and configured_end_date:
            resolved_start = configured_start_date
            resolved_end = configured_end_date
        else:
            resolved_end = date.today()
            resolved_start = resolved_end - timedelta(days=fallback_days if fallback_days > 0 else 30)
    return resolved_start, resolved_end


def _rebusca_facto(
    names: list[str],
    cpf: str,
    start_date: date | None = None,
    end_date: date | None = None,
) -> dict[str, Any]:
    settings = get_settings()
    if not settings.source_facto_enabled:
        return {
            "performed": False,
            "source": "facto_conveniar",
            "names": names,
            "results": [],
            "enabled": False,
            "error": "source_disabled",
        }
    resolved_start, resolved_end = _resolve_conveniar_period(
        start_date,
        end_date,
        settings.facto_start_date,
        settings.facto_end_date,
        settings.facto_days,
    )

    results = []
    for name in names:
        if not name:
            continue
        result = ingest_facto(name, cpf=cpf, start_date=resolved_start, end_date=resolved_end)
        results.append(result.__dict__)
    return {
        "performed": bool(results),
        "source": "facto_conveniar",
        "names": names,
        "results": results,
        "enabled": True,
        "period": {
            "start": resolved_start.isoformat() if resolved_start else None,
            "end": resolved_end.isoformat() if resolved_end else None,
        },
    }


def _rebusca_fest(
    names: list[str],
    cpf: str,
    start_date: date | None = None,
    end_date: date | None = None,
) -> dict[str, Any]:
    settings = get_settings()
    if not settings.source_fest_enabled:
        return {
            "performed": False,
            "source": "fest_conveniar",
            "names": names,
            "results": [],
            "enabled": False,
            "error": "source_disabled",
        }
    resolved_start, resolved_end = _resolve_conveniar_period(
        start_date,
        end_date,
        settings.fest_start_date,
        settings.fest_end_date,
        settings.fest_days,
    )

    results = []
    for name in names:
        if not name:
            continue
        result = ingest_fest(name, cpf=cpf, start_date=resolved_start, end_date=resolved_end)
        results.append(result.__dict__)
    return {
        "performed": bool(results),
        "source": "fest_conveniar",
        "names": names,
        "results": results,
        "enabled": True,
        "period": {
            "start": resolved_start.isoformat() if resolved_start else None,
            "end": resolved_end.isoformat() if resolved_end else None,
        },
    }


def _rebusca_federal(
    names: list[str],
    cpfs: list[str] | None = None,
    data_inicio: date | None = None,
    data_fim: date | None = None,
) -> dict[str, Any]:
    settings = get_settings()
    if not settings.source_federal_enabled:
        return {
            "performed": False,
            "source": "portal_federal",
            "names": names,
            "results": [],
            "enabled": False,
            "error": "source_disabled",
        }
    results = []
    cpf_list = cpfs or []
    mes_anos = mes_anos_for_period(data_inicio, data_fim, settings.federal_mes_ano)
    if cpf_list:
        cpf_result = ingest_portal_federal_for_cpfs(cpf_list, mes_anos=mes_anos)
        results.append(cpf_result.__dict__)
    elif settings.federal_api_key:
        result = ingest_portal_federal_for_names(names, mes_anos=mes_anos)
        results.append(result.__dict__)
    else:
        results.append(
            {
                "source_id": "portal_federal_remuneracao",
                "status": "skipped",
                "records": 0,
                "raw_files": 0,
                "error": "missing_api_key",
                "notes": None,
            }
        )
    return {
        "performed": True,
        "source": "portal_federal",
        "names": names,
        "results": results,
        "enabled": True,
    }


def _ensure_fapes_ingested(force: bool = False) -> dict[str, Any]:
    settings = get_settings()
    if not settings.source_fapes_enabled:
        return {
            "performed": False,
            "source": "fapes_bolsas",
            "results": [],
            "enabled": False,
            "error": "source_disabled",
        }
    if not force:
        sources = list_sources()
        for source in sources:
            if source.get("source_id") == "fapes_bolsas":
                if source.get("last_status") == "ok" and source.get("last_run_at"):
                    return {
                        "performed": False,
                        "source": "fapes_bolsas",
                        "results": [],
                        "enabled": True,
                        "notes": "already_ingested",
                    }
                break
    result = ingest_fapes()
    return {
        "performed": True,
        "source": "fapes_bolsas",
        "results": [result.__dict__],
        "enabled": True,
    }


def _run_query_refresh(
    nome: str,
    cpf: str | None = None,
    data_inicio: date | None = None,
    data_fim: date | None = None,
    include_fapes: bool = True,
    include_facto: bool = True,
    include_fest: bool = True,
    include_federal: bool = False,
) -> dict[str, Any]:
    cpf_list = _parse_cpfs(cpf)
    cpf_masked = [_mask_cpf(value) for value in cpf_list if _normalize_cpf_digits(value)]
    facto_cpf = next((digits for value in cpf_list if (digits := _normalize_cpf_digits(value))), None)
    if (include_facto or include_fest) and not facto_cpf:
        raise HTTPException(status_code=422, detail="cpf_is_required_for_facto_or_fest")
    candidate_names: dict[str, str] = {normalize_name(nome): nome}
    existing_records = _filter_records_by_cpf(
        _deserialize_records(search_records(normalize_name(nome), None, "ES", None)),
        facto_cpf,
    )
    for record in existing_records:
        candidate = record.get("person_name_original")
        if candidate:
            candidate_norm = normalize_name(candidate)
            candidate_names.setdefault(candidate_norm, candidate)
    candidate_list = sorted(candidate_names.values())
    payload: dict[str, Any] = {
        "performed": True,
        "cpf_masked": cpf_masked,
        "period": {
            "start": data_inicio.isoformat() if data_inicio else None,
            "end": data_fim.isoformat() if data_fim else None,
        },
    }
    if include_fapes:
        payload["fapes"] = _ensure_fapes_ingested(force=True)
    if include_facto:
        payload["facto"] = _rebusca_facto(candidate_list[:3], facto_cpf, data_inicio, data_fim)
    if include_fest:
        payload["fest"] = _rebusca_fest(candidate_list[:3], facto_cpf, data_inicio, data_fim)
    if include_federal:
        payload["federal"] = _rebusca_federal(candidate_list[:1], cpf_list, data_inicio, data_fim)
    return payload


def _deserialize_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    parsed = []
    for record in records:
        if isinstance(record.get("detalhes_json"), str):
            try:
                record["detalhes_json"] = json.loads(record["detalhes_json"])
            except json.JSONDecodeError:
                pass
        if _is_legacy_conveniar_window_summary(record):
            continue
        parsed.append(record)
    return parsed


def _dedup_key(record: dict[str, Any]) -> tuple[Any, ...]:
    source_id = record.get("source_id")
    if _is_conveniar_source(source_id):
        detalhes = record.get("detalhes_json")
        raw = detalhes.get("raw") if isinstance(detalhes, dict) else None
        cod_lancamento = raw.get("CodLancamento") if isinstance(raw, dict) else None
        if cod_lancamento is not None:
            return (source_id, str(cod_lancamento))
        raw_key = json.dumps(raw, ensure_ascii=True, sort_keys=True) if isinstance(raw, dict) else None
        return (
            source_id,
            raw_key,
            record.get("person_name_norm"),
            record.get("data_pagamento"),
            record.get("competencia"),
        )
    base = (
        source_id,
        record.get("person_name_norm"),
        record.get("competencia"),
        record.get("data_pagamento"),
        record.get("valor_bruto"),
        record.get("valor_liquido"),
        record.get("orgao"),
        record.get("municipio"),
        record.get("cargo_funcao"),
        record.get("source_url"),
    )
    if source_id == "fapes_bolsas":
        detalhes = record.get("detalhes_json")
        raw = detalhes.get("raw") if isinstance(detalhes, dict) else None
        raw_key = json.dumps(raw, ensure_ascii=True, sort_keys=True) if isinstance(raw, dict) else None
        # FAPES rows can be reingested with a better monthly competence later on.
        # When the raw row is the same, we keep only one logical receipt in reports.
        if raw_key:
            return (source_id, raw_key)
        return base
    return base


def _dedup_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[Any, ...]] = set()
    result: list[dict[str, Any]] = []
    for record in records:
        key = _dedup_key(record)
        if key in seen:
            continue
        seen.add(key)
        result.append(record)
    return result


def _record_sort_key(record: dict[str, Any]) -> tuple[str, str]:
    competencia = record.get("competencia") or ""
    data_pagamento = record.get("data_pagamento") or ""
    return (competencia, data_pagamento)


def _load_query_records(filters: dict[str, Any]) -> list[dict[str, Any]]:
    return _deserialize_records(
        search_records(
            filters["nome_norm"],
            filters["tipo_filter"],
            filters["uf_norm"],
            filters["municipio_norm"],
        )
    )


def _build_search_payload(
    filters: dict[str, Any],
    records: list[dict[str, Any]],
    cpf_masked: list[str] | None = None,
    period_bounds: tuple[date, date] | None = None,
    rebusca_info: dict[str, Any] | None = None,
) -> dict[str, Any]:
    deduped_records = _dedup_records(records)
    deduped_records.sort(key=_record_sort_key, reverse=True)
    clusters = cluster_records(deduped_records)
    for cluster in clusters:
        cluster["top_records"] = _deserialize_records(cluster.get("top_records", []))
    records_by_cluster: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in deduped_records:
        records_by_cluster[cluster_id_for_record(record)].append(record)
    for cluster in clusters:
        grouped: dict[str, dict[str, Any]] = {}
        for record in records_by_cluster.get(cluster["cluster_id"], []):
            source_id = record.get("source_id") or "unknown"
            if source_id not in grouped:
                grouped[source_id] = {
                    "source_id": source_id,
                    "source_name": record.get("source_name"),
                    "records": [],
                }
            if len(grouped[source_id]["records"]) < 5:
                grouped[source_id]["records"].append(record)
        cluster["top_records_by_source"] = list(grouped.values())

    payload: dict[str, Any] = {
        "query": {
            "nome": filters["nome"],
            "nome_norm": filters["nome_norm"],
            "uf": filters["uf_norm"] or filters["uf"].upper(),
            "municipio": filters["municipio"],
            "municipio_norm": filters["municipio_norm"],
            "tipo": filters["tipo"],
            "tipo_norm": filters["tipo_norm"],
            "cpf_masked": cpf_masked or [],
        },
        "summary": _build_monthly_summary(deduped_records),
        "match_context": _build_match_context(deduped_records, clusters),
        "rebusca": rebusca_info or {"performed": False},
        "clusters": clusters,
        "total_records": len(deduped_records),
    }
    if period_bounds:
        payload["period_report"] = build_period_report(deduped_records, period_bounds[0], period_bounds[1])
    return payload


@app.get("/health")
async def health() -> dict[str, Any]:
    settings = get_settings()
    return {
        "status": "ok",
        "db_path": str(settings.db_path),
    }


@app.get("/sources")
async def sources() -> dict[str, Any]:
    return {"sources": list_sources()}


@app.post("/ingest/run")
async def ingest_run(payload: dict = Body(default=None)) -> dict[str, Any]:
    targets = []
    facto_nome = None
    facto_cpf = None
    fest_nome = None
    fest_cpf = None
    if payload:
        targets = payload.get("targets") or []
        facto_nome = payload.get("facto_nome")
        facto_cpf = payload.get("facto_cpf")
        fest_nome = payload.get("fest_nome")
        fest_cpf = payload.get("fest_cpf")
    result = run_ingest(
        targets=targets or ["all"],
        facto_nome=facto_nome,
        facto_cpf=facto_cpf,
        fest_nome=fest_nome,
        fest_cpf=fest_cpf,
    )
    return result


@app.post("/refresh/query")
async def refresh_query(payload: dict = Body(default=None)) -> dict[str, Any]:
    payload = payload or {}
    nome = payload.get("nome")
    if not nome:
        raise HTTPException(status_code=422, detail="nome_is_required")
    period_bounds = _resolve_period_bounds(payload.get("data_inicio"), payload.get("data_fim"))
    data_inicio = period_bounds[0] if period_bounds else None
    data_fim = period_bounds[1] if period_bounds else None
    include_fapes = bool(payload.get("include_fapes", True))
    include_facto = bool(payload.get("include_facto", True))
    include_fest = bool(payload.get("include_fest", True))
    include_federal = bool(payload.get("include_federal", False))
    refresh_result = _run_query_refresh(
        nome=nome,
        cpf=payload.get("cpf"),
        data_inicio=data_inicio,
        data_fim=data_fim,
        include_fapes=include_fapes,
        include_facto=include_facto,
        include_fest=include_fest,
        include_federal=include_federal,
    )
    return {
        "query": {
            "nome": nome,
            "cpf_masked": refresh_result.get("cpf_masked", []),
        },
        "refresh": refresh_result,
    }


@app.get("/search")
async def search(
    nome: str = Query(...),
    uf: str = Query("ES"),
    municipio: str | None = Query(None),
    tipo: str = Query("todos"),
    rebusca: bool = Query(False),
    cpf: str | None = Query(None, description="CPF (11 digitos) para filtro local e rebusca FACTO/FEST/Portal"),
    data_inicio: str | None = Query(None, description="YYYY-MM-DD"),
    data_fim: str | None = Query(None, description="YYYY-MM-DD"),
) -> dict[str, Any]:
    filters = _normalize_search_filters(nome, uf, municipio, tipo)
    period_bounds = _resolve_period_bounds(data_inicio, data_fim)
    records = _filter_records_by_cpf(_load_query_records(filters), cpf)
    rebusca_info = {"performed": False}
    cpf_masked: list[str] = [_mask_cpf(digits) for digits in [_normalize_cpf_digits(cpf)] if digits]
    if rebusca:
        refresh_payload = _run_query_refresh(
            nome=nome,
            cpf=cpf,
            data_inicio=period_bounds[0] if period_bounds else None,
            data_fim=period_bounds[1] if period_bounds else None,
            include_fapes=True,
            include_facto=True,
            include_fest=True,
            include_federal=True,
        )
        rebusca_info = refresh_payload
        cpf_masked = refresh_payload.get("cpf_masked", [])
        records = _filter_records_by_cpf(_load_query_records(filters), cpf)
    return _build_search_payload(filters, records, cpf_masked, period_bounds, rebusca_info)


@app.get("/summary")
async def summary(
    nome: str = Query(...),
    uf: str = Query("ES"),
    municipio: str | None = Query(None),
    tipo: str = Query("todos"),
    rebusca: bool = Query(False),
    cpf: str | None = Query(None, description="CPF (11 digitos) para filtro local e rebusca FACTO/FEST/Portal"),
    data_inicio: str | None = Query(None, description="YYYY-MM-DD"),
    data_fim: str | None = Query(None, description="YYYY-MM-DD"),
) -> dict[str, Any]:
    filters = _normalize_search_filters(nome, uf, municipio, tipo)
    period_bounds = _resolve_period_bounds(data_inicio, data_fim)
    records = _filter_records_by_cpf(_load_query_records(filters), cpf)

    cpf_masked: list[str] = [_mask_cpf(digits) for digits in [_normalize_cpf_digits(cpf)] if digits]
    if rebusca:
        refresh_payload = _run_query_refresh(
            nome=nome,
            cpf=cpf,
            data_inicio=period_bounds[0] if period_bounds else None,
            data_fim=period_bounds[1] if period_bounds else None,
            include_fapes=True,
            include_facto=True,
            include_fest=True,
            include_federal=True,
        )
        cpf_masked = refresh_payload.get("cpf_masked", [])
        records = _filter_records_by_cpf(_load_query_records(filters), cpf)

    records = _dedup_records(records)
    records.sort(key=_record_sort_key, reverse=True)
    summary = _build_simple_monthly_summary(records)
    return {
        "query": {
            "nome": filters["nome"],
            "nome_norm": filters["nome_norm"],
            "uf": filters["uf_norm"] or filters["uf"].upper(),
            "municipio": filters["municipio"],
            "municipio_norm": filters["municipio_norm"],
            "tipo": filters["tipo"],
            "tipo_norm": filters["tipo_norm"],
            "cpf_masked": cpf_masked if rebusca else [],
        },
        **summary,
        "period_report": build_period_report(records, period_bounds[0], period_bounds[1]) if period_bounds else None,
        "total_records": len(records),
    }


@app.get("/person/{person_key}")
async def person(person_key: str) -> dict[str, Any]:
    cluster = get_cluster(person_key)
    if not cluster:
        raise HTTPException(status_code=404, detail="person_key_not_found")
    records = list_records_for_cluster(cluster)
    records = _deserialize_records(records)
    timeline = sorted(
        records,
        key=lambda r: (r.get("competencia") or "", r.get("data_pagamento") or ""),
        reverse=True,
    )
    folha = [r for r in records if r.get("tipo_recebimento") == "FOLHA"]
    bolsa = [r for r in records if r.get("tipo_recebimento") == "BOLSA"]
    diaria = [r for r in records if r.get("tipo_recebimento") == "DIARIA"]
    return {
        "person_key": person_key,
        "person_name_norm": cluster.get("person_name_norm"),
        "confidence": cluster.get("confidence"),
        "evidence": json.loads(cluster.get("evidence_json") or "{}"),
        "recebimentos": {
            "folha": folha,
            "bolsa": bolsa,
            "diaria": diaria,
        },
        "timeline": timeline,
        "total_records": len(records),
    }
