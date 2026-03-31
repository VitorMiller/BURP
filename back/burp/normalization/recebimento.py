from __future__ import annotations

from typing import Any, Iterable

from burp.normalization.name import normalize_header, normalize_name
from burp.settings import get_settings


DEFAULT_DIARIA_KEYWORDS = ["DIARIA", "DIARIAS", "AJUDA DE CUSTO"]
DEFAULT_BOLSA_KEYWORDS = ["BOLSA", "BOLSISTA", "INICIACAO", "PESQUISA", "EXTENSAO", "CAPES", "CNPQ", "FAPES"]
DEFAULT_DIARIA_PRIMARY_FIELDS = ["elemento", "natureza", "rubrica"]
DEFAULT_DIARIA_PRIMARY_JSONPATHS = [
    "$.raw.elemento",
    "$.raw.natureza",
    "$.raw.rubrica",
    "$.raw.item",
]
DEFAULT_DIARIA_STRONG_FIELDS = DEFAULT_DIARIA_PRIMARY_FIELDS + ["historico", "descricao", "observacao"]
DEFAULT_DIARIA_STRONG_JSONPATHS = [
    "$.raw.observacao",
    "$.raw.historico",
    "$.raw.descricao",
]


def normalize_tipo(value: str | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    normalized = normalize_name(text)
    if normalized in {"DIARIA", "DIARIAS"}:
        return "DIARIA"
    if normalized in {"BOLSA", "BOLSAS"}:
        return "BOLSA"
    if normalized in {"FOLHA", "FOLHAS"}:
        return "FOLHA"
    if normalized in {"TODOS", "ALL", "TODAS"}:
        return "TODOS"
    return text.upper()


def normalize_tipo_filter(value: str | None) -> str | None:
    tipo = normalize_tipo(value)
    if not tipo or tipo == "TODOS":
        return None
    return tipo


def infer_recebimento_tipo(record: dict[str, Any], source_id: str | None = None) -> tuple[str | None, str | None]:
    settings = get_settings()
    tipo_current = normalize_tipo(record.get("tipo_recebimento"))
    if tipo_current != "BOLSA":
        return tipo_current, None

    detalhes = record.get("detalhes_json")
    if not detalhes:
        return tipo_current, None

    diaria_keywords = _normalize_keywords(settings.diaria_keywords or DEFAULT_DIARIA_KEYWORDS)
    bolsa_keywords = _normalize_keywords(settings.bolsa_keywords or DEFAULT_BOLSA_KEYWORDS)
    primary_fields = _normalize_fields(settings.diaria_primary_fields or DEFAULT_DIARIA_PRIMARY_FIELDS)
    strong_fields = _normalize_fields(settings.diaria_strong_fields or DEFAULT_DIARIA_STRONG_FIELDS)
    primary_jsonpaths = settings.diaria_primary_jsonpaths or DEFAULT_DIARIA_PRIMARY_JSONPATHS
    strong_jsonpaths = settings.diaria_strong_jsonpaths or DEFAULT_DIARIA_STRONG_JSONPATHS

    bolsa_hit: tuple[str, str] | None = None
    diaria_secondary: tuple[str, str] | None = None
    for path, text in _iter_jsonpath_texts(detalhes, primary_jsonpaths):
        text_norm = normalize_name(text)
        if not text_norm:
            continue
        diaria_kw = _match_keyword(text_norm, diaria_keywords)
        if diaria_kw:
            return "DIARIA", _build_reason("primary_jsonpath_match", path, source_id, diaria_kw)
    for field_path, field_key, text in _iter_text_fields(detalhes):
        text_norm = normalize_name(text)
        if not text_norm:
            continue
        field_norm = normalize_header(field_key or field_path)
        if not bolsa_hit:
            bolsa_kw = _match_keyword(text_norm, bolsa_keywords)
            if bolsa_kw:
                bolsa_hit = (bolsa_kw, field_path)
        diaria_kw = _match_keyword(text_norm, diaria_keywords)
        if not diaria_kw:
            continue
        if _is_strong_field(field_norm, primary_fields):
            reason = _build_reason("primary_field_match", field_path, source_id, diaria_kw)
            return "DIARIA", reason
        if _is_strong_field(field_norm, strong_fields) and not diaria_secondary:
            diaria_secondary = (diaria_kw, field_path)

    if not diaria_secondary:
        for path, text in _iter_jsonpath_texts(detalhes, strong_jsonpaths):
            text_norm = normalize_name(text)
            if not text_norm:
                continue
            diaria_kw = _match_keyword(text_norm, diaria_keywords)
            if diaria_kw:
                diaria_secondary = (diaria_kw, path)
                break

    if diaria_secondary:
        diaria_kw, diaria_field = diaria_secondary
        if not bolsa_hit:
            reason = _build_reason("strong_match", diaria_field, source_id, diaria_kw)
            return "DIARIA", reason
        conflict_reason = _build_conflict_reason(diaria_kw, diaria_field, bolsa_hit, source_id)
        return tipo_current, conflict_reason

    return tipo_current, None


def _normalize_keywords(values: Iterable[str]) -> list[str]:
    normalized = []
    for value in values:
        if not value:
            continue
        key = normalize_name(value)
        if key and key not in normalized:
            normalized.append(key)
    return normalized


def _normalize_fields(values: Iterable[str]) -> list[str]:
    normalized = []
    for value in values:
        if not value:
            continue
        key = normalize_header(value)
        if key and key not in normalized:
            normalized.append(key)
    return normalized


def _match_keyword(text_norm: str, keywords: Iterable[str]) -> str | None:
    ordered = sorted([k for k in keywords if k], key=len, reverse=True)
    for keyword in ordered:
        if keyword and keyword in text_norm:
            return keyword
    return None


def _is_strong_field(field_norm: str, strong_fields: Iterable[str]) -> bool:
    for token in strong_fields:
        if token and token in field_norm:
            return True
    return False


def _iter_text_fields(value: Any, path: str = "") -> Iterable[tuple[str, str, str]]:
    if isinstance(value, dict):
        for key, item in value.items():
            key_text = str(key)
            next_path = f"{path}.{key_text}" if path else key_text
            if isinstance(item, (dict, list)):
                yield from _iter_text_fields(item, next_path)
            elif item is not None:
                text = str(item).strip()
                if text:
                    yield next_path, key_text, text
    elif isinstance(value, list):
        for idx, item in enumerate(value):
            next_path = f"{path}[{idx}]"
            if isinstance(item, (dict, list)):
                yield from _iter_text_fields(item, next_path)
            elif item is not None:
                text = str(item).strip()
                if text:
                    yield next_path, "", text


def _iter_jsonpath_texts(value: Any, jsonpaths: Iterable[str]) -> Iterable[tuple[str, str]]:
    for path in jsonpaths:
        if not path:
            continue
        tokens = _parse_jsonpath(path)
        if not tokens:
            continue
        extracted = _resolve_jsonpath(value, tokens)
        for text in _extract_texts(extracted):
            yield path, text


def _extract_texts(value: Any) -> Iterable[str]:
    if value is None:
        return
    if isinstance(value, (str, int, float)):
        text = str(value).strip()
        if text:
            yield text
        return
    if isinstance(value, list):
        for item in value:
            yield from _extract_texts(item)
        return
    if isinstance(value, dict):
        for item in value.values():
            yield from _extract_texts(item)


def _parse_jsonpath(path: str) -> list[str | int]:
    text = path.strip()
    if not text:
        return []
    if text.startswith("$"):
        text = text[1:]
    if text.startswith("."):
        text = text[1:]
    tokens: list[str | int] = []
    buf = ""
    idx = 0
    while idx < len(text):
        char = text[idx]
        if char == ".":
            if buf:
                tokens.append(buf)
                buf = ""
            idx += 1
            continue
        if char == "[":
            if buf:
                tokens.append(buf)
                buf = ""
            end = text.find("]", idx + 1)
            if end == -1:
                break
            inner = text[idx + 1 : end].strip().strip("'\"")
            if inner.isdigit():
                tokens.append(int(inner))
            elif inner:
                tokens.append(inner)
            idx = end + 1
            if idx < len(text) and text[idx] == ".":
                idx += 1
            continue
        buf += char
        idx += 1
    if buf:
        tokens.append(buf)
    return tokens


def _resolve_jsonpath(value: Any, tokens: list[str | int]) -> Any:
    current = value
    for token in tokens:
        if isinstance(token, int):
            if not isinstance(current, list) or token >= len(current):
                return None
            current = current[token]
        else:
            if not isinstance(current, dict):
                return None
            current = current.get(token)
        if current is None:
            return None
    return current


def _build_reason(reason: str, field_path: str, source_id: str | None, keyword: str | None = None) -> str:
    suffix = f"{reason}:{field_path}"
    if keyword:
        suffix = f"{suffix}={keyword}"
    if source_id:
        return f"{suffix} source={source_id}"
    return suffix


def _build_conflict_reason(
    diaria_keyword: str,
    diaria_field: str,
    bolsa_hit: tuple[str, str],
    source_id: str | None,
) -> str:
    bolsa_keyword, bolsa_field = bolsa_hit
    suffix = f"diaria:{diaria_keyword} field={diaria_field} bolsa:{bolsa_keyword} bolsa_field={bolsa_field}"
    if source_id:
        return f"diaria_conflict {suffix} source={source_id}"
    return f"diaria_conflict {suffix}"
