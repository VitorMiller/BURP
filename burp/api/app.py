from __future__ import annotations

import json
import re
from collections import defaultdict
from typing import Any

from dotenv import load_dotenv
from fastapi import Body, FastAPI, HTTPException, Query

from datetime import date, timedelta

from burp.connectors.facto import ingest_facto
from burp.connectors.fapes import ingest_fapes
from burp.connectors.portal_federal import (
    ingest_portal_federal_favorecido_for_names,
    ingest_portal_federal_for_cpfs,
    ingest_portal_federal_for_names,
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

app = FastAPI(title="BURP - Buscador Universal de Recebimentos Publicos")


def _extract_month_key(record: dict[str, Any]) -> str | None:
    competencia = record.get("competencia") or ""
    match = re.search(r"(\d{4})[-/](\d{2})", competencia)
    if match:
        return f"{match.group(1)}-{match.group(2)}"
    detalhes = record.get("detalhes_json")
    raw = detalhes.get("raw") if isinstance(detalhes, dict) else None
    if isinstance(raw, dict):
        for key in ("data de credito", "data_credito", "data pagamento", "data_pagamento", "data"):
            if key in raw and raw[key]:
                value = str(raw[key])
                match = re.search(r"(\d{4})[-/](\d{2})", value)
                if match:
                    return f"{match.group(1)}-{match.group(2)}"
                match = re.search(r"(\d{2})/(\d{2})/(\d{4})", value)
                if match:
                    return f"{match.group(3)}-{match.group(2)}"
    if isinstance(detalhes, dict):
        periodo = detalhes.get("periodo")
        if periodo:
            matches = re.findall(r"(\d{2})/(\d{2})/(\d{4})", str(periodo))
            if matches:
                _, month, year = matches[-1]
                return f"{year}-{month}"
    return None


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
        value = record.get("valor_liquido")
        if value is None:
            value = record.get("valor_bruto")
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
        value = record.get("valor_liquido")
        if value is None:
            value = record.get("valor_bruto")
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
    digits = re.sub(r"\\D", "", value)
    if len(digits) != 11:
        return value
    return f"***.{digits[3:6]}.{digits[6:9]}-**"


def _rebusca_facto(names: list[str]) -> dict[str, Any]:
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
    start_date: date | None = None
    end_date: date | None = None
    if settings.facto_start_date and settings.facto_end_date:
        start_date = settings.facto_start_date
        end_date = settings.facto_end_date
    else:
        end_date = date.today()
        start_date = end_date - timedelta(days=settings.facto_days)

    results = []
    for name in names:
        if not name:
            continue
        result = ingest_facto(name, start_date=start_date, end_date=end_date)
        results.append(result.__dict__)
    return {
        "performed": bool(results),
        "source": "facto_conveniar",
        "names": names,
        "results": results,
        "enabled": True,
        "period": {
            "start": start_date.isoformat() if start_date else None,
            "end": end_date.isoformat() if end_date else None,
        },
    }


def _rebusca_federal(names: list[str], cpfs: list[str] | None = None) -> dict[str, Any]:
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
    result_favorecido = ingest_portal_federal_favorecido_for_names(names)
    results.append(result_favorecido.__dict__)
    cpf_list = cpfs or []
    if cpf_list:
        cpf_result = ingest_portal_federal_for_cpfs(cpf_list)
        results.append(cpf_result.__dict__)
    elif settings.federal_api_key:
        result = ingest_portal_federal_for_names(names)
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


def _ensure_fapes_ingested() -> dict[str, Any]:
    settings = get_settings()
    if not settings.source_fapes_enabled:
        return {
            "performed": False,
            "source": "fapes_bolsas",
            "results": [],
            "enabled": False,
            "error": "source_disabled",
        }
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


def _deserialize_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    parsed = []
    for record in records:
        if isinstance(record.get("detalhes_json"), str):
            try:
                record["detalhes_json"] = json.loads(record["detalhes_json"])
            except json.JSONDecodeError:
                pass
        parsed.append(record)
    return parsed


def _dedup_key(record: dict[str, Any]) -> tuple[Any, ...]:
    source_id = record.get("source_id")
    if source_id == "facto_conveniar":
        detalhes = record.get("detalhes_json")
        raw = detalhes.get("raw") if isinstance(detalhes, dict) else None
        raw_key = json.dumps(raw, ensure_ascii=True, sort_keys=True) if isinstance(raw, dict) else None
        return (
            source_id,
            record.get("person_name_norm"),
            record.get("person_hint_id"),
            record.get("valor_bruto"),
            record.get("valor_liquido"),
            raw_key,
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
        return base + (raw_key,)
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
    if payload:
        targets = payload.get("targets") or []
        facto_nome = payload.get("facto_nome")
    result = run_ingest(targets=targets or ["all"], facto_nome=facto_nome)
    return result


@app.get("/search")
async def search(
    nome: str = Query(...),
    uf: str = Query("ES"),
    municipio: str | None = Query(None),
    tipo: str = Query("todos"),
    rebusca: bool = Query(True),
    cpf: str | None = Query(None, description="CPF (11 digitos) para busca de folha federal"),
) -> dict[str, Any]:
    name_norm = normalize_name(nome)
    uf_norm = uf.upper() if uf else None
    if uf_norm in {"TODOS", "ALL"}:
        uf_norm = None
    municipio_norm = normalize_name(municipio) if municipio else None
    tipo_norm = normalize_tipo(tipo) or "TODOS"
    tipo_filter = normalize_tipo_filter(tipo)
    records = _deserialize_records(search_records(name_norm, tipo_filter, uf_norm, municipio_norm))

    rebusca_info = {"performed": False}
    cpf_masked: list[str] = []
    if rebusca:
        cpf_list = _parse_cpfs(cpf)
        cpf_masked = [_mask_cpf(value) for value in cpf_list if value]
        fapes_result = _ensure_fapes_ingested()
        records = _deserialize_records(search_records(name_norm, tipo_filter, uf_norm, municipio_norm))
        candidate_names: dict[str, str] = {normalize_name(nome): nome}
        for record in records:
            candidate = record.get("person_name_original")
            if candidate:
                candidate_norm = normalize_name(candidate)
                if candidate_norm not in candidate_names:
                    candidate_names[candidate_norm] = candidate
        candidate_list = sorted(candidate_names.values())
        rebusca_info = {
            "performed": True,
            "fapes": fapes_result,
            "facto": _rebusca_facto(candidate_list[:3]),
            "federal": _rebusca_federal(candidate_list[:1], cpf_list),
            "cpf_masked": cpf_masked,
        }
        records = _deserialize_records(search_records(name_norm, tipo_filter, uf_norm, municipio_norm))

    records = _dedup_records(records)
    records.sort(key=_record_sort_key, reverse=True)
    clusters = cluster_records(records)
    for cluster in clusters:
        cluster["top_records"] = _deserialize_records(cluster.get("top_records", []))
    records_by_cluster: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in records:
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
    summary = _build_monthly_summary(records)
    return {
        "query": {
            "nome": nome,
            "nome_norm": name_norm,
            "uf": uf_norm or uf.upper(),
            "municipio": municipio,
            "municipio_norm": municipio_norm,
            "tipo": tipo,
            "tipo_norm": tipo_norm,
            "cpf_masked": cpf_masked if rebusca else [],
        },
        "summary": summary,
        "rebusca": rebusca_info,
        "clusters": clusters,
        "total_records": len(records),
    }


@app.get("/summary")
async def summary(
    nome: str = Query(...),
    uf: str = Query("ES"),
    municipio: str | None = Query(None),
    tipo: str = Query("todos"),
    rebusca: bool = Query(True),
    cpf: str | None = Query(None, description="CPF (11 digitos) para busca de folha federal"),
) -> dict[str, Any]:
    name_norm = normalize_name(nome)
    uf_norm = uf.upper() if uf else None
    if uf_norm in {"TODOS", "ALL"}:
        uf_norm = None
    municipio_norm = normalize_name(municipio) if municipio else None
    tipo_norm = normalize_tipo(tipo) or "TODOS"
    tipo_filter = normalize_tipo_filter(tipo)
    records = _deserialize_records(search_records(name_norm, tipo_filter, uf_norm, municipio_norm))

    cpf_masked: list[str] = []
    if rebusca:
        cpf_list = _parse_cpfs(cpf)
        cpf_masked = [_mask_cpf(value) for value in cpf_list if value]
        _ensure_fapes_ingested()
        records = _deserialize_records(search_records(name_norm, tipo_filter, uf_norm, municipio_norm))
        candidate_names: dict[str, str] = {normalize_name(nome): nome}
        for record in records:
            candidate = record.get("person_name_original")
            if candidate:
                candidate_norm = normalize_name(candidate)
                if candidate_norm not in candidate_names:
                    candidate_names[candidate_norm] = candidate
        candidate_list = sorted(candidate_names.values())
        _rebusca_facto(candidate_list[:3])
        _rebusca_federal(candidate_list[:1], cpf_list)
        records = _deserialize_records(search_records(name_norm, tipo, uf_norm, municipio_norm))

    records = _dedup_records(records)
    records.sort(key=_record_sort_key, reverse=True)
    summary = _build_simple_monthly_summary(records)
    return {
        "query": {
            "nome": nome,
            "nome_norm": name_norm,
            "uf": uf_norm or uf.upper(),
            "municipio": municipio,
            "municipio_norm": municipio_norm,
            "tipo": tipo,
            "tipo_norm": tipo_norm,
            "cpf_masked": cpf_masked if rebusca else [],
        },
        **summary,
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
    return {
        "person_key": person_key,
        "person_name_norm": cluster.get("person_name_norm"),
        "confidence": cluster.get("confidence"),
        "evidence": json.loads(cluster.get("evidence_json") or "{}"),
        "recebimentos": {
            "folha": folha,
            "bolsa": bolsa,
        },
        "timeline": timeline,
        "total_records": len(records),
    }
