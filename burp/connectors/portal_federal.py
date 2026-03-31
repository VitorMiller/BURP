from __future__ import annotations

import json
import logging
import re
from datetime import date
from typing import Any
from urllib.parse import urlencode

import requests

from burp.connectors.base import IngestResult
from burp.normalization.name import normalize_header, normalize_name
from burp.settings import PARSER_VERSION, get_settings
from burp.storage import insert_raw_file, insert_records, update_source_run
from burp.utils import (
    compute_sha256,
    filename_from_url,
    now_utc_iso,
    parse_competencia,
    parse_decimal,
    find_key,
    safe_filename,
)

logger = logging.getLogger(__name__)

FAVORECIDO_COLUMNS = [
    "data",
    "documentoResumido",
    "localizadorGasto",
    "fase",
    "especie",
    "favorecido",
    "ufFavorecido",
    "valor",
    "ug",
    "uo",
    "orgao",
    "orgaoSuperior",
    "grupo",
    "elemento",
    "modalidade",
    "planoOrcamentario",
    "autor",
    "subTitulo",
    "funcao",
    "subfuncao",
    "programa",
    "acao",
]
FAVORECIDO_COLUMNS_PARAM = ",".join(FAVORECIDO_COLUMNS)


def _mes_ano_default() -> str:
    today = date.today()
    month = today.month - 1
    year = today.year
    if month <= 0:
        month = 12
        year -= 1
    return f"{year}{month:02d}"


def _mes_ano_candidates(fixed: str | None) -> list[str]:
    if fixed:
        return [fixed]
    today = date.today()
    month = today.month - 1
    year = today.year
    if month <= 0:
        month = 12
        year -= 1
    candidates = []
    for _ in range(3):
        candidates.append(f"{year}{month:02d}")
        month -= 1
        if month <= 0:
            month = 12
            year -= 1
    return candidates


def _fetch_remuneracao(
    session: requests.Session,
    base_url: str,
    headers: dict[str, str],
    params: dict[str, Any],
    mes_anos: list[str],
    timeout: int,
) -> tuple[requests.Response | None, str | None]:
    for mes_ano in mes_anos:
        params["mesAno"] = mes_ano
        resp = session.get(f"{base_url}/servidores/remuneracao", params=params, headers=headers, timeout=timeout)
        if resp.status_code == 200:
            return resp, mes_ano
    return None, None


def _fetch_favorecido_resultado(
    session: requests.Session,
    base_url: str,
    favorecido_id: str,
    fases: list[str],
    offset: int,
    page_size: int,
    timeout: int,
) -> requests.Response:
    params = {
        "offset": offset,
        "tamanhoPagina": page_size,
        "direcaoOrdenacao": "desc",
        "colunaOrdenacao": "valor",
        "colunasSelecionadas": FAVORECIDO_COLUMNS_PARAM,
        "paginacaoSimples": "false",
        "favorecido": favorecido_id,
        "faseDespesa": _fase_despesa_param(fases),
    }
    return session.get(
        f"{base_url.rstrip('/')}/despesas/favorecido/resultado",
        params=params,
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=timeout,
    )


def _lookup_servidores_by_name(
    session: requests.Session,
    base_url: str,
    headers: dict[str, str],
    orgao_code: str,
    name: str,
    timeout: int,
) -> list[dict[str, Any]]:
    params = {"pagina": 1, "orgaoServidorLotacao": orgao_code, "nome": name}
    resp = session.get(f"{base_url}/servidores", params=params, headers=headers, timeout=timeout)
    if resp.status_code != 200:
        resp = session.get(
            f"{base_url}/servidores",
            params={"pagina": 1, "orgaoServidorLotacao": orgao_code},
            headers=headers,
            timeout=timeout,
        )
    if resp.status_code != 200:
        return []
    items = resp.json()
    if not isinstance(items, list):
        return []
    target_norm = normalize_name(name)
    matches = []
    for item in items:
        candidate = _extract_name(item) or _extract_name(item.get("servidor", {}) if isinstance(item, dict) else {})
        if not candidate:
            continue
        cand_norm = normalize_name(candidate)
        if target_norm in cand_norm:
            matches.append(item)
    return matches


def _choose_orgao_code(
    session: requests.Session,
    base_url: str,
    headers: dict[str, str],
    preferred: str | None,
    timeout: int,
) -> str | None:
    if preferred:
        return preferred
    for page in range(1, 6):
        resp = session.get(f"{base_url}/orgaos-siape", params={"pagina": page}, headers=headers, timeout=timeout)
        resp.raise_for_status()
        items = resp.json()
        if not items:
            break
        for item in items:
            code = item.get("codigo")
            desc = (item.get("descricao") or "").upper()
            if not code:
                continue
            if "INVALIDO" in desc or "IGNORADO" in desc:
                continue
            return code
    return None


def _value_text(value: Any) -> str | None:
    if isinstance(value, dict):
        return value.get("nome") or value.get("descricao") or value.get("sigla")
    if value is None:
        return None
    return str(value).strip() or None


def _extract_name(item: dict[str, Any]) -> str | None:
    if "nome" in item:
        return _value_text(item.get("nome"))
    pessoa = item.get("pessoa")
    if isinstance(pessoa, dict):
        return _value_text(pessoa.get("nome"))
    servidor = item.get("servidor")
    if isinstance(servidor, dict):
        pessoa = servidor.get("pessoa")
        if isinstance(pessoa, dict):
            return _value_text(pessoa.get("nome"))
        return _value_text(servidor.get("nome"))
    return None


def _extract_hint(item: dict[str, Any]) -> str | None:
    for key in ["cpf", "cpfFormatado", "cpfDescaracterizado"]:
        value = item.get(key)
        if value:
            return _mask_cpf(value)
    pessoa = item.get("pessoa")
    if isinstance(pessoa, dict):
        for key in ["cpf", "cpfFormatado", "cpfDescaracterizado"]:
            value = pessoa.get(key)
            if value:
                return _mask_cpf(value)
    servidor = item.get("servidor")
    if isinstance(servidor, dict):
        pessoa = servidor.get("pessoa")
        if isinstance(pessoa, dict):
            for key in ["cpf", "cpfFormatado", "cpfDescaracterizado"]:
                value = pessoa.get(key)
                if value:
                    return _mask_cpf(value)
    return None


def _extract_orgao(item: dict[str, Any]) -> str | None:
    for key in [
        "orgaoServidorLotacao",
        "orgaoServidorExercicio",
        "orgao",
        "orgaoSuperior",
        "orgaoExercicio",
        "orgaoLotacao",
    ]:
        value = item.get(key)
        text = _value_text(value)
        if text:
            return text
    servidor = item.get("servidor")
    if isinstance(servidor, dict):
        for key in ["orgaoServidorLotacao", "orgaoServidorExercicio", "orgao"]:
            value = servidor.get(key)
            text = _value_text(value)
            if text:
                return text
    return None


def _mask_cpf(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if "*" in text:
        return text
    digits = re.sub(r"\\D", "", text)
    if len(digits) != 11:
        return text
    return f"***.{digits[3:6]}.{digits[6:9]}-**"


def _parse_date_br(value: Any) -> tuple[str | None, str | None]:
    if value is None:
        return None, None
    text = str(value).strip()
    if not text:
        return None, None
    match = re.search(r"(\\d{2})/(\\d{2})/(\\d{4})", text)
    if match:
        day, month, year = match.groups()
        return f"{year}-{month}-{day}", f"{year}-{month}"
    match = re.search(r"(\\d{4})-(\\d{2})-(\\d{2})", text)
    if match:
        year, month, day = match.groups()
        return f"{year}-{month}-{day}", f"{year}-{month}"
    match = re.search(r"(\\d{4})-(\\d{2})", text)
    if match:
        year, month = match.groups()
        return None, f"{year}-{month}"
    return None, None


def _extract_masked_document(value: Any) -> str | None:
    if not value:
        return None
    match = re.search(r"([0-9\\*]{3}\\.[0-9\\*]{3}\\.[0-9\\*]{3}-[0-9\\*]{2})", str(value))
    if match:
        return match.group(1)
    return None


def _extract_favorecido_name(item: dict[str, Any]) -> str | None:
    name = item.get("nomeFavorecido") or item.get("favorecido")
    if name is None:
        return None
    text = str(name).strip()
    if not text:
        return None
    if " - " in text:
        return text.split(" - ", 1)[1].strip() or None
    return text


def _matches_target_name(target_norm: str | None, candidate_norm: str | None) -> bool:
    if not target_norm:
        return True
    if not candidate_norm:
        return False
    return target_norm in candidate_norm or candidate_norm in target_norm


def _fase_despesa_param(fases: list[str]) -> str:
    fases_valid = [fase.strip() for fase in fases if fase.strip()]
    return ",".join(fases_valid) if fases_valid else "3"


def _build_favorecido_page_url(base_url: str, favorecido_id: str, fases: list[str]) -> str:
    params = {
        "faseDespesa": _fase_despesa_param(fases),
        "favorecido": favorecido_id,
        "ordenarPor": "valor",
        "direcao": "desc",
    }
    return f"{base_url.rstrip('/')}/despesas/favorecido?{urlencode(params)}"


def _map_remuneracao_items(
    items: list[dict[str, Any]],
    source_id: str,
    raw_id: int,
    source_url: str,
    collected_at: str,
    extra_details: dict[str, Any] | None = None,
    default_hint_id: str | None = None,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for item in items:
        name = _extract_name(item)
        if not name:
            continue
        hint_id = _extract_hint(item) or default_hint_id
        orgao = _extract_orgao(item)
        cargo_funcao = item.get("cargo") or item.get("funcao")
        remuneracoes = item.get("remuneracoesDTO")
        if isinstance(remuneracoes, list) and remuneracoes:
            for remuneracao in remuneracoes:
                if not isinstance(remuneracao, dict):
                    continue
                competencia = parse_competencia(
                    remuneracao.get("mesAno") or remuneracao.get("skMesReferencia")
                )
                bruto = parse_decimal(
                    remuneracao.get("valorTotalRemuneracao")
                    or remuneracao.get("remuneracaoBasicaBruta")
                    or remuneracao.get("valorTotalRemuneracaoAposDeducoes")
                )
                liquido = parse_decimal(
                    remuneracao.get("valorTotalRemuneracaoAposDeducoes")
                    or remuneracao.get("valorTotalRemuneracao")
                    or remuneracao.get("remuneracaoBasicaBruta")
                )
                descontos = None
                if bruto is not None and liquido is not None:
                    diff = bruto - liquido
                    if abs(diff) > 0.0001:
                        descontos = diff
                detalhes = {"servidor": item.get("servidor", item), "remuneracao": remuneracao}
                if extra_details:
                    detalhes["portal_busca"] = extra_details
                record = {
                    "source_id": source_id,
                    "raw_id": raw_id,
                    "person_name_original": name,
                    "person_name_norm": normalize_name(name),
                    "person_hint_id": hint_id,
                    "uf": "BR",
                    "municipio": None,
                    "orgao": orgao,
                    "tipo_recebimento": "FOLHA",
                    "competencia": competencia,
                    "data_pagamento": None,
                    "valor_bruto": bruto,
                    "descontos": descontos,
                    "valor_liquido": liquido,
                    "cargo_funcao": cargo_funcao,
                    "detalhes_json": detalhes,
                    "source_url": source_url,
                    "collected_at": collected_at,
                    "parser_version": PARSER_VERSION,
                }
                records.append(record)
            continue
        key_map = {normalize_header(k): k for k in item.keys()}
        normalized_keys = list(key_map.keys())
        bruto_key = find_key(normalized_keys, [
            "remuneracao bruta",
            "remuneracao basica bruta",
            "remuneracao total",
            "valor bruto",
            "valor total",
        ])
        liquido_key = find_key(normalized_keys, [
            "remuneracao apos deducoes",
            "remuneracao liquida",
            "valor liquido",
        ])
        descontos_key = find_key(normalized_keys, [
            "desconto",
            "abateteto",
            "abate teto",
            "deducao",
        ])
        competencia_key = find_key(normalized_keys, ["mesano", "competencia", "mes ano"])

        bruto = parse_decimal(item.get(key_map[bruto_key])) if bruto_key else None
        liquido = parse_decimal(item.get(key_map[liquido_key])) if liquido_key else None
        descontos = parse_decimal(item.get(key_map[descontos_key])) if descontos_key else None
        competencia_raw = item.get(key_map[competencia_key]) if competencia_key else None
        competencia = parse_competencia(competencia_raw)
        detalhes = item
        if extra_details:
            detalhes = {"remuneracao": item, "portal_busca": extra_details}
        record = {
            "source_id": source_id,
            "raw_id": raw_id,
            "person_name_original": name,
            "person_name_norm": normalize_name(name),
            "person_hint_id": hint_id,
            "uf": "BR",
            "municipio": None,
            "orgao": orgao,
            "tipo_recebimento": "FOLHA",
            "competencia": competencia,
            "data_pagamento": None,
            "valor_bruto": bruto,
            "descontos": descontos,
            "valor_liquido": liquido,
            "cargo_funcao": cargo_funcao,
            "detalhes_json": detalhes,
            "source_url": source_url,
            "collected_at": collected_at,
            "parser_version": PARSER_VERSION,
        }
        records.append(record)
    return records


def _map_favorecido_items(
    items: list[dict[str, Any]],
    source_id: str,
    raw_id: int,
    source_url: str,
    collected_at: str,
    target_norm: str | None,
    target_hint: str | None,
    registro: dict[str, Any],
    resultado_url: str,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for item in items:
        name = _extract_favorecido_name(item)
        if not name:
            continue
        name_norm = normalize_name(name)
        item_hint = _extract_masked_document(item.get("codigoFavorecido")) or _extract_masked_document(
            item.get("favorecido")
        )
        matches = _matches_target_name(target_norm, name_norm)
        if not matches and target_hint and item_hint and target_hint == item_hint:
            matches = True
        if not matches:
            continue
        data_pagamento, competencia = _parse_date_br(item.get("data"))
        valor = parse_decimal(item.get("valor"))
        detalhes = {
            "categoria": "despesas_favorecido",
            "raw": item,
            "portal_busca": registro,
            "resultado_url": resultado_url,
        }
        record = {
            "source_id": source_id,
            "raw_id": raw_id,
            "person_name_original": name,
            "person_name_norm": name_norm,
            "person_hint_id": item_hint or target_hint,
            "uf": item.get("ufFavorecido") or "BR",
            "municipio": None,
            "orgao": item.get("orgao"),
            "tipo_recebimento": "BOLSA",
            "competencia": competencia,
            "data_pagamento": data_pagamento,
            "valor_bruto": valor,
            "descontos": None,
            "valor_liquido": valor,
            "cargo_funcao": None,
            "detalhes_json": detalhes,
            "source_url": source_url,
            "collected_at": collected_at,
            "parser_version": PARSER_VERSION,
        }
        records.append(record)
    return records


def _persist_remuneracao_response(
    remun_resp: requests.Response,
    identifier: str,
    mes_ano_used: str,
    settings,
    collected_at: str,
    extra_details: dict[str, Any] | None = None,
    default_hint_id: str | None = None,
) -> tuple[int, int]:
    if remun_resp.status_code != 200:
        logger.warning("portal federal remuneration failed: %s", remun_resp.text[:200])
        return 0, 0
    content = remun_resp.content
    raw_hash = compute_sha256(content)
    filename = filename_from_url(remun_resp.url, f"portal_federal_{identifier}_{mes_ano_used}.json")
    local_path = str(settings.data_dir / "raw" / "portal_federal_remuneracao" / filename)
    (settings.data_dir / "raw" / "portal_federal_remuneracao").mkdir(parents=True, exist_ok=True)
    with open(local_path, "wb") as handle:
        handle.write(content)
    raw_id = insert_raw_file(
        source_id="portal_federal_remuneracao",
        url=remun_resp.url,
        collected_at=collected_at,
        sha256=raw_hash,
        local_path=local_path,
        content_type=remun_resp.headers.get("content-type"),
    )
    try:
        items = remun_resp.json()
    except json.JSONDecodeError:
        items = []
    if isinstance(items, dict):
        items = [items]
    records = _map_remuneracao_items(
        items,
        "portal_federal_remuneracao",
        raw_id,
        remun_resp.url,
        collected_at,
        extra_details=extra_details,
        default_hint_id=default_hint_id,
    )
    records_count = insert_records(records)
    return 1, records_count


def _persist_favorecido_response(
    resp: requests.Response,
    favorecido_id: str,
    offset: int,
    settings,
    collected_at: str,
    source_url: str,
    target_norm: str | None,
    target_hint: str | None,
    registro: dict[str, Any],
) -> tuple[int, int, int | None]:
    if resp.status_code != 200:
        logger.warning("portal federal favorecido failed: %s", resp.text[:200])
        return 0, 0, None
    content = resp.content
    raw_hash = compute_sha256(content)
    filename = safe_filename(
        f"portal_federal_favorecido_{favorecido_id}_{offset}.json",
        "portal_federal_favorecido.json",
    )
    local_path = str(settings.data_dir / "raw" / "portal_federal_favorecido" / filename)
    (settings.data_dir / "raw" / "portal_federal_favorecido").mkdir(parents=True, exist_ok=True)
    with open(local_path, "wb") as handle:
        handle.write(content)
    raw_id = insert_raw_file(
        source_id="portal_federal_favorecido",
        url=resp.url,
        collected_at=collected_at,
        sha256=raw_hash,
        local_path=local_path,
        content_type=resp.headers.get("content-type"),
    )
    try:
        data = resp.json()
    except json.JSONDecodeError:
        data = {}
    items = data.get("data", []) if isinstance(data, dict) else []
    records_total = data.get("recordsTotal") if isinstance(data, dict) else None
    records = _map_favorecido_items(
        items,
        "portal_federal_favorecido",
        raw_id,
        source_url,
        collected_at,
        target_norm,
        target_hint,
        registro,
        resp.url,
    )
    records_count = insert_records(records)
    return 1, records_count, records_total


def _persist_busca_response(
    resp: requests.Response,
    termo: str,
    settings,
    collected_at: str,
    source_id: str,
) -> int:
    content = resp.content
    raw_hash = compute_sha256(content)
    filename = safe_filename(f"busca_pessoa_fisica_{termo}.json", "busca_pessoa_fisica.json")
    local_path = str(settings.data_dir / "raw" / "portal_federal_busca" / filename)
    (settings.data_dir / "raw" / "portal_federal_busca").mkdir(parents=True, exist_ok=True)
    with open(local_path, "wb") as handle:
        handle.write(content)
    insert_raw_file(
        source_id=source_id,
        url=resp.url,
        collected_at=collected_at,
        sha256=raw_hash,
        local_path=local_path,
        content_type=resp.headers.get("content-type"),
    )
    return 1


def _buscar_pessoa_fisica(
    session: requests.Session,
    base_url: str,
    termo: str,
    timeout: int,
    settings,
    collected_at: str,
    source_id: str,
) -> tuple[list[dict[str, Any]], int]:
    params = {"termo": termo, "pagina": 1, "tamanhoPagina": settings.federal_max_servers}
    resp = session.get(
        f"{base_url.rstrip('/')}/busca/pessoa-fisica",
        params=params,
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=timeout,
    )
    if resp.status_code != 200:
        logger.warning("portal federal busca failed: %s", resp.text[:200])
        return [], 0
    raw_files = _persist_busca_response(resp, termo, settings, collected_at, source_id)
    data = resp.json()
    registros = data.get("registros", []) if isinstance(data, dict) else []
    return registros, raw_files


def _filter_registros_by_name(name: str, registros: list[dict[str, Any]]) -> list[dict[str, Any]]:
    target_norm = normalize_name(name)
    if not target_norm:
        return registros
    filtered = []
    for registro in registros:
        reg_name = registro.get("nome") or ""
        reg_norm = normalize_name(reg_name)
        if not reg_norm:
            continue
        if target_norm in reg_norm or reg_norm in target_norm:
            filtered.append(registro)
    return filtered


def _fetch_pessoa_fisica_servidor_ids(
    session: requests.Session,
    portal_base_url: str,
    sk_pessoa: str,
    timeout: int,
) -> list[int]:
    url = f"{portal_base_url.rstrip('/')}/pessoa-fisica/{sk_pessoa}/servidor"
    resp = session.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=timeout)
    if resp.status_code != 200:
        return []
    try:
        data = resp.json()
    except json.JSONDecodeError:
        return []
    if not isinstance(data, list):
        return []
    ids = []
    for item in data:
        value = item.get("idServidorAposentadoPensionista")
        if value:
            ids.append(int(value))
    return ids


def ingest_portal_federal() -> IngestResult:
    settings = get_settings()
    collected_at = now_utc_iso()
    if not settings.federal_api_key:
        update_source_run("portal_federal_remuneracao", "error", collected_at, "missing_api_key")
        return IngestResult(
            source_id="portal_federal_remuneracao",
            status="error",
            records=0,
            raw_files=0,
            error="missing_api_key",
        )

    headers = {"chave-api-dados": settings.federal_api_key}
    session = requests.Session()

    try:
        mes_anos = _mes_ano_candidates(settings.federal_mes_ano)
        timeout = settings.federal_timeout
        raw_files = 0
        total_records = 0

        for cpf in settings.federal_cpfs:
            remun_params = {"pagina": 1, "cpf": cpf}
            remun_resp, mes_ano_used = _fetch_remuneracao(
                session,
                settings.federal_base_url,
                headers,
                remun_params,
                mes_anos,
                timeout,
            )
            if remun_resp and mes_ano_used:
                raw_delta, records_delta = _persist_remuneracao_response(
                    remun_resp,
                    cpf.replace(".", "").replace("-", ""),
                    mes_ano_used,
                    settings,
                    collected_at,
                )
                raw_files += raw_delta
                total_records += records_delta

        for servidor_id in settings.federal_ids:
            remun_params = {"pagina": 1, "idServidorAposentadoPensionista": servidor_id}
            remun_resp, mes_ano_used = _fetch_remuneracao(
                session,
                settings.federal_base_url,
                headers,
                remun_params,
                mes_anos,
                timeout,
            )
            if remun_resp and mes_ano_used:
                raw_delta, records_delta = _persist_remuneracao_response(
                    remun_resp,
                    str(servidor_id),
                    mes_ano_used,
                    settings,
                    collected_at,
                )
                raw_files += raw_delta
                total_records += records_delta

        if not settings.federal_cpfs and not settings.federal_ids:
            orgao_code = _choose_orgao_code(
                session,
                settings.federal_base_url,
                headers,
                settings.federal_orgao_siape,
                timeout,
            )
            if not orgao_code:
                raise RuntimeError("orgao_siape_not_found")

            servidores_url = f"{settings.federal_base_url}/servidores"
            servidores_params = {"pagina": 1, "orgaoServidorLotacao": orgao_code}
            servidores_resp = session.get(servidores_url, params=servidores_params, headers=headers, timeout=timeout)
            servidores_resp.raise_for_status()
            servidores_data = servidores_resp.json()

            for servidor in servidores_data[: settings.federal_max_servers]:
                servidor_info = servidor.get("servidor", {})
                servidor_id = servidor_info.get("idServidorAposentadoPensionista")
                servidor_alt = servidor_info.get("id")
                params = {"pagina": 1}
                if servidor_id:
                    params["idServidorAposentadoPensionista"] = servidor_id
                elif servidor_alt:
                    params["idServidor"] = servidor_alt
                else:
                    continue
                remun_resp, mes_ano_used = _fetch_remuneracao(
                    session,
                    settings.federal_base_url,
                    headers,
                    params,
                    mes_anos,
                    timeout,
                )
                if remun_resp and mes_ano_used:
                    raw_delta, records_delta = _persist_remuneracao_response(
                        remun_resp,
                        str(servidor_id or servidor_alt),
                        mes_ano_used,
                        settings,
                        collected_at,
                    )
                    raw_files += raw_delta
                    total_records += records_delta

        status = "ok" if total_records > 0 else "warning"
        update_source_run(
            "portal_federal_remuneracao",
            status,
            collected_at,
            None if total_records > 0 else "no_remuneracao_records",
        )
        return IngestResult(
            source_id="portal_federal_remuneracao",
            status=status,
            records=total_records,
            raw_files=raw_files,
            error=None if total_records > 0 else "no_remuneracao_records",
        )
    except Exception as exc:  # pylint: disable=broad-except
        logger.exception("portal federal ingest failed: %s", exc)
        update_source_run("portal_federal_remuneracao", "error", collected_at, str(exc))
        return IngestResult(
            source_id="portal_federal_remuneracao",
            status="error",
            records=0,
            raw_files=0,
            error=str(exc),
        )


def ingest_portal_federal_favorecido_for_names(names: list[str]) -> IngestResult:
    settings = get_settings()
    collected_at = now_utc_iso()
    if not names:
        update_source_run("portal_federal_favorecido", "skipped", collected_at, "no_names")
        return IngestResult(
            source_id="portal_federal_favorecido",
            status="skipped",
            records=0,
            raw_files=0,
            error="no_names",
        )

    session = requests.Session()
    timeout = settings.federal_timeout
    page_size = max(settings.federal_favorecido_page_size, 1)
    max_pages = max(settings.federal_favorecido_max_pages, 1)
    fases = settings.federal_favorecido_fases or ["3"]

    try:
        raw_files = 0
        total_records = 0
        truncated = False
        seen_ids: set[str] = set()

        for name in names:
            registros, raw_delta = _buscar_pessoa_fisica(
                session,
                settings.federal_busca_base_url,
                name,
                timeout,
                settings,
                collected_at,
                "portal_federal_favorecido",
            )
            raw_files += raw_delta
            registros = _filter_registros_by_name(name, registros)
            for registro in registros[: settings.federal_max_servers]:
                sk_pessoa = registro.get("skPessoa")
                if not sk_pessoa:
                    continue
                key_id = str(sk_pessoa)
                if key_id in seen_ids:
                    continue
                seen_ids.add(key_id)
                target_norm = normalize_name(registro.get("nome") or name)
                target_hint = _extract_masked_document(registro.get("cpfNis"))
                page_url = _build_favorecido_page_url(settings.federal_portal_base_url, key_id, fases)

                offset = 0
                pages = 0
                records_total: int | None = None
                while True:
                    resp = _fetch_favorecido_resultado(
                        session,
                        settings.federal_portal_base_url,
                        key_id,
                        fases,
                        offset,
                        page_size,
                        timeout,
                    )
                    raw_delta, records_delta, total = _persist_favorecido_response(
                        resp,
                        key_id,
                        offset,
                        settings,
                        collected_at,
                        page_url,
                        target_norm,
                        target_hint,
                        registro,
                    )
                    raw_files += raw_delta
                    total_records += records_delta
                    if total is None:
                        break
                    records_total = total
                    offset += page_size
                    pages += 1
                    if offset >= records_total:
                        break
                    if pages >= max_pages:
                        if records_total and offset < records_total:
                            truncated = True
                        break

        status = "ok" if total_records > 0 else "warning"
        update_source_run(
            "portal_federal_favorecido",
            status,
            collected_at,
            None if total_records > 0 else "no_favorecido_records",
        )
        return IngestResult(
            source_id="portal_federal_favorecido",
            status=status,
            records=total_records,
            raw_files=raw_files,
            error=None if total_records > 0 else "no_favorecido_records",
            notes="max_pages_reached" if truncated else None,
        )
    except Exception as exc:  # pylint: disable=broad-except
        logger.exception("portal federal favorecido ingest failed: %s", exc)
        update_source_run("portal_federal_favorecido", "error", collected_at, str(exc))
        return IngestResult(
            source_id="portal_federal_favorecido",
            status="error",
            records=0,
            raw_files=0,
            error=str(exc),
        )


def ingest_portal_federal_for_cpfs(cpfs: list[str]) -> IngestResult:
    settings = get_settings()
    collected_at = now_utc_iso()
    if not settings.federal_api_key:
        update_source_run("portal_federal_remuneracao", "error", collected_at, "missing_api_key")
        return IngestResult(
            source_id="portal_federal_remuneracao",
            status="error",
            records=0,
            raw_files=0,
            error="missing_api_key",
        )
    cleaned = []
    for cpf in cpfs:
        digits = re.sub(r"\\D", "", str(cpf))
        if len(digits) == 11:
            cleaned.append(digits)
    if not cleaned:
        update_source_run("portal_federal_remuneracao", "skipped", collected_at, "no_cpfs")
        return IngestResult(
            source_id="portal_federal_remuneracao",
            status="skipped",
            records=0,
            raw_files=0,
            error="no_cpfs",
        )

    headers = {"chave-api-dados": settings.federal_api_key}
    session = requests.Session()
    timeout = settings.federal_timeout
    mes_anos = _mes_ano_candidates(settings.federal_mes_ano)

    try:
        raw_files = 0
        total_records = 0
        for cpf in cleaned:
            remun_params = {"pagina": 1, "cpf": cpf}
            remun_resp, mes_ano_used = _fetch_remuneracao(
                session,
                settings.federal_base_url,
                headers,
                remun_params,
                mes_anos,
                timeout,
            )
            if remun_resp and mes_ano_used:
                raw_delta, records_delta = _persist_remuneracao_response(
                    remun_resp,
                    cpf,
                    mes_ano_used,
                    settings,
                    collected_at,
                    default_hint_id=_mask_cpf(cpf),
                )
                raw_files += raw_delta
                total_records += records_delta

        status = "ok" if total_records > 0 else "warning"
        update_source_run(
            "portal_federal_remuneracao",
            status,
            collected_at,
            None if total_records > 0 else "no_remuneracao_records",
        )
        return IngestResult(
            source_id="portal_federal_remuneracao",
            status=status,
            records=total_records,
            raw_files=raw_files,
            error=None if total_records > 0 else "no_remuneracao_records",
        )
    except Exception as exc:  # pylint: disable=broad-except
        logger.exception("portal federal cpf ingest failed: %s", exc)
        update_source_run("portal_federal_remuneracao", "error", collected_at, str(exc))
        return IngestResult(
            source_id="portal_federal_remuneracao",
            status="error",
            records=0,
            raw_files=0,
            error=str(exc),
        )


def ingest_portal_federal_for_names(names: list[str]) -> IngestResult:
    settings = get_settings()
    collected_at = now_utc_iso()
    if not settings.federal_api_key:
        update_source_run("portal_federal_remuneracao", "error", collected_at, "missing_api_key")
        return IngestResult(
            source_id="portal_federal_remuneracao",
            status="error",
            records=0,
            raw_files=0,
            error="missing_api_key",
        )
    if not names:
        update_source_run("portal_federal_remuneracao", "skipped", collected_at, "no_names")
        return IngestResult(
            source_id="portal_federal_remuneracao",
            status="skipped",
            records=0,
            raw_files=0,
            error="no_names",
        )

    headers = {"chave-api-dados": settings.federal_api_key}
    session = requests.Session()
    timeout = settings.federal_timeout
    mes_anos = _mes_ano_candidates(settings.federal_mes_ano)

    try:
        raw_files = 0
        total_records = 0
        seen_ids: set[str] = set()

        for name in names:
            registros, raw_delta = _buscar_pessoa_fisica(
                session,
                settings.federal_busca_base_url,
                name,
                timeout,
                settings,
                collected_at,
                "portal_federal_remuneracao",
            )
            raw_files += raw_delta
            registros = _filter_registros_by_name(name, registros)
            for registro in registros[: settings.federal_max_servers]:
                sk_pessoa = registro.get("skPessoa")
                if not sk_pessoa:
                    continue
                servidor_ids = _fetch_pessoa_fisica_servidor_ids(
                    session,
                    settings.federal_portal_base_url,
                    str(sk_pessoa),
                    timeout,
                )
                for servidor_id in servidor_ids:
                    key_id = f"idServidorAposentadoPensionista:{servidor_id}"
                    if key_id in seen_ids:
                        continue
                    seen_ids.add(key_id)
                    params = {"pagina": 1, "idServidorAposentadoPensionista": servidor_id}
                    remun_resp, mes_ano_used = _fetch_remuneracao(
                        session,
                        settings.federal_base_url,
                        headers,
                        params,
                        mes_anos,
                        timeout,
                    )
                    if remun_resp and mes_ano_used:
                        raw_delta, records_delta = _persist_remuneracao_response(
                            remun_resp,
                            str(servidor_id),
                            mes_ano_used,
                            settings,
                            collected_at,
                            extra_details=registro,
                            default_hint_id=registro.get("cpfNis"),
                        )
                        raw_files += raw_delta
                        total_records += records_delta

        if total_records == 0:
            orgao_code = _choose_orgao_code(
                session,
                settings.federal_base_url,
                headers,
                settings.federal_orgao_siape,
                timeout,
            )
            if not orgao_code:
                update_source_run("portal_federal_remuneracao", "warning", collected_at, "orgao_siape_not_found")
                return IngestResult(
                    source_id="portal_federal_remuneracao",
                    status="warning",
                    records=0,
                    raw_files=raw_files,
                    error="orgao_siape_not_found",
                )

            for name in names:
                matches = _lookup_servidores_by_name(
                    session,
                    settings.federal_base_url,
                    headers,
                    orgao_code,
                    name,
                    timeout,
                )
                for item in matches[: settings.federal_max_servers]:
                    servidor = item.get("servidor", {}) if isinstance(item, dict) else {}
                    servidor_id = servidor.get("idServidorAposentadoPensionista")
                    servidor_alt = servidor.get("id")
                    for key, value in (("idServidorAposentadoPensionista", servidor_id), ("idServidor", servidor_alt)):
                        if not value:
                            continue
                        key_id = f"{key}:{value}"
                        if key_id in seen_ids:
                            continue
                        seen_ids.add(key_id)
                        params = {"pagina": 1, key: value}
                        remun_resp, mes_ano_used = _fetch_remuneracao(
                            session,
                            settings.federal_base_url,
                            headers,
                            params,
                            mes_anos,
                            timeout,
                        )
                        if remun_resp and mes_ano_used:
                            raw_delta, records_delta = _persist_remuneracao_response(
                                remun_resp,
                                str(value),
                                mes_ano_used,
                                settings,
                                collected_at,
                            )
                            raw_files += raw_delta
                            total_records += records_delta

        status = "ok" if total_records > 0 else "warning"
        update_source_run(
            "portal_federal_remuneracao",
            status,
            collected_at,
            None if total_records > 0 else "no_remuneracao_records",
        )
        return IngestResult(
            source_id="portal_federal_remuneracao",
            status=status,
            records=total_records,
            raw_files=raw_files,
            error=None if total_records > 0 else "no_remuneracao_records",
        )
    except Exception as exc:  # pylint: disable=broad-except
        logger.exception("portal federal name lookup failed: %s", exc)
        update_source_run("portal_federal_remuneracao", "error", collected_at, str(exc))
        return IngestResult(
            source_id="portal_federal_remuneracao",
            status="error",
            records=0,
            raw_files=0,
            error=str(exc),
        )
