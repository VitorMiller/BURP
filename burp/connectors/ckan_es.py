from __future__ import annotations

import logging
import re
from typing import Iterable

import requests

from burp.connectors.base import IngestResult
from burp.connectors.ckan import ckan_package_show
from burp.normalization.name import normalize_name
from burp.parsers.csv_parser import iter_csv_rows
from burp.settings import PARSER_VERSION, get_settings
from burp.storage import insert_raw_file, insert_records, update_source_run
from burp.utils import compute_sha256, filename_from_url, now_utc_iso, parse_competencia, parse_decimal, find_key

logger = logging.getLogger(__name__)

DATASET_ID = "portal-da-transparencia-pessoal"


def _select_resource(resources: list[dict], needle: str) -> dict | None:
    needle = needle.lower()
    for res in resources:
        name = (res.get("name") or "").lower()
        url = (res.get("url") or "").lower()
        if needle in name or needle in url:
            return res
    return None


def _parse_competencia_from_name(name: str) -> str | None:
    match = re.search(r"(\d{2})[_-](\d{4})", name)
    if match:
        month, year = match.groups()
        return f"{year}-{month}"
    match = re.search(r"(\d{4})[_-](\d{2})", name)
    if match:
        year, month = match.groups()
        return f"{year}-{month}"
    return None


def _select_remuneracoes(resources: list[dict]) -> dict | None:
    candidates = []
    for res in resources:
        name = res.get("name", "")
        if "remuneracao" in name.lower() or "remuneracoes" in name.lower():
            competencia = _parse_competencia_from_name(name) or _parse_competencia_from_name(res.get("url", ""))
            candidates.append((competencia or "", name, res))
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0])
    return candidates[-1][2]


def _map_records(
    rows: Iterable[dict[str, str]],
    source_id: str,
    orgao_default: str,
    raw_id: int,
    source_url: str,
    collected_at: str,
    competencia: str | None,
) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for row in rows:
        keys = list(row.keys())
        name_key = find_key(keys, ["nome servidor", "nome", "servidor"])
        orgao_key = find_key(keys, ["orgao", "secretaria"])
        cargo_key = find_key(keys, ["cargo", "funcao"])
        bruto_key = find_key(keys, ["valor bruto", "remuneracao bruta", "remuneracao", "provento"])
        liquido_key = find_key(keys, ["valor liquido", "remuneracao liquida"])
        descontos_key = find_key(keys, ["desconto", "descontos"])
        competencia_key = find_key(keys, ["competencia", "mes", "referencia"])
        matricula_key = find_key(keys, ["matricula", "cpf"])

        name = row.get(name_key) if name_key else None
        if not name:
            continue
        orgao = row.get(orgao_key) if orgao_key else orgao_default
        cargo = row.get(cargo_key) if cargo_key else None
        bruto = parse_decimal(row.get(bruto_key)) if bruto_key else None
        liquido = parse_decimal(row.get(liquido_key)) if liquido_key else None
        descontos = parse_decimal(row.get(descontos_key)) if descontos_key else None
        competencia_row = parse_competencia(row.get(competencia_key)) if competencia_key else None
        record = {
            "source_id": source_id,
            "raw_id": raw_id,
            "person_name_original": name,
            "person_name_norm": normalize_name(name),
            "person_hint_id": row.get(matricula_key) if matricula_key else None,
            "uf": "ES",
            "municipio": None,
            "orgao": orgao,
            "tipo_recebimento": "FOLHA",
            "competencia": competencia_row or competencia,
            "data_pagamento": None,
            "valor_bruto": bruto,
            "descontos": descontos,
            "valor_liquido": liquido,
            "cargo_funcao": cargo,
            "detalhes_json": {"raw": row},
            "source_url": source_url,
            "collected_at": collected_at,
            "parser_version": PARSER_VERSION,
        }
        records.append(record)
    return records


def ingest_ckan_pessoal() -> IngestResult:
    settings = get_settings()
    collected_at = now_utc_iso()
    try:
        dataset = ckan_package_show(settings.ckan_base_url, DATASET_ID)
        resources = dataset.get("resources", [])
        rem_resource = _select_remuneracoes(resources)
        if not rem_resource:
            raise RuntimeError("Remuneracoes resource not found")
        vinculos_resource = _select_resource(resources, "vinculosservidores")
        url = rem_resource.get("url")
        if not url:
            raise RuntimeError("Remuneracoes resource missing URL")
        raw_files = 0
        if vinculos_resource and vinculos_resource.get("url"):
            vinc_url = vinculos_resource["url"]
            try:
                vinc_resp = requests.get(vinc_url, timeout=120)
                vinc_resp.raise_for_status()
                vinc_content = vinc_resp.content
                vinc_hash = compute_sha256(vinc_content)
                vinc_filename = filename_from_url(vinc_url, "vinculos.csv")
                vinc_path = str(settings.data_dir / "raw" / "es_ckan_pessoal" / vinc_filename)
                (settings.data_dir / "raw" / "es_ckan_pessoal").mkdir(parents=True, exist_ok=True)
                with open(vinc_path, "wb") as handle:
                    handle.write(vinc_content)
                insert_raw_file(
                    source_id="es_ckan_pessoal",
                    url=vinc_url,
                    collected_at=collected_at,
                    sha256=vinc_hash,
                    local_path=vinc_path,
                    content_type=vinc_resp.headers.get("content-type"),
                )
                raw_files += 1
            except Exception as exc:  # pylint: disable=broad-except
                logger.warning("vinculos download failed: %s", exc)

        resp = requests.get(url, timeout=120)
        resp.raise_for_status()
        content = resp.content
        raw_hash = compute_sha256(content)
        filename = filename_from_url(url, "es_remuneracoes.csv")
        local_path = str(settings.data_dir / "raw" / "es_ckan_pessoal" / filename)
        (settings.data_dir / "raw" / "es_ckan_pessoal").mkdir(parents=True, exist_ok=True)
        with open(local_path, "wb") as handle:
            handle.write(content)
        raw_id = insert_raw_file(
            source_id="es_ckan_pessoal",
            url=url,
            collected_at=collected_at,
            sha256=raw_hash,
            local_path=local_path,
            content_type=resp.headers.get("content-type"),
        )
        raw_files += 1
        competencia = _parse_competencia_from_name(rem_resource.get("name", ""))
        rows = iter_csv_rows(content)
        records = _map_records(rows, "es_ckan_pessoal", dataset.get("title", ""), raw_id, url, collected_at, competencia)
        count = insert_records(records)
        update_source_run("es_ckan_pessoal", "ok", collected_at, None)
        return IngestResult(source_id="es_ckan_pessoal", status="ok", records=count, raw_files=raw_files)
    except Exception as exc:  # pylint: disable=broad-except
        logger.exception("ckan pessoal ingest failed: %s", exc)
        update_source_run("es_ckan_pessoal", "error", collected_at, str(exc))
        return IngestResult(source_id="es_ckan_pessoal", status="error", records=0, raw_files=0, error=str(exc))
