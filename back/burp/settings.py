from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from dotenv import load_dotenv

PARSER_VERSION = "0.1.0"

DEFAULT_TETO_CONSTITUCIONAL_BY_YEAR = {
    2023: 41650.92,
    2024: 44008.52,
    2025: 46366.19,
}

load_dotenv()


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    db_path: Path
    api_cors_origins: list[str]
    ckan_base_url: str
    vitoria_base_url: str
    vilavelha_base_url: str
    facto_base_url: str
    federal_base_url: str
    federal_busca_base_url: str
    federal_portal_base_url: str
    federal_api_key: str | None
    federal_orgao_siape: str | None
    federal_mes_ano: str | None
    federal_max_servers: int
    federal_cpfs: list[str]
    federal_ids: list[str]
    federal_timeout: int
    federal_favorecido_page_size: int
    federal_favorecido_max_pages: int
    federal_favorecido_fases: list[str]
    facto_days: int
    facto_window_days: int
    facto_start_date: date | None
    facto_end_date: date | None
    source_vitoria_enabled: bool
    source_vilavelha_enabled: bool
    source_ckan_enabled: bool
    source_fapes_enabled: bool
    source_facto_enabled: bool
    source_federal_enabled: bool
    diaria_keywords: list[str]
    bolsa_keywords: list[str]
    diaria_primary_fields: list[str]
    diaria_strong_fields: list[str]
    diaria_primary_jsonpaths: list[str]
    diaria_strong_jsonpaths: list[str]
    tipo_classification_debug: bool
    teto_constitucional_by_year: dict[int, float]


def _env_bool(key: str, default: bool) -> bool:
    value = os.getenv(key)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_list(key: str) -> list[str]:
    value = os.getenv(key, "")
    return [item.strip() for item in value.split(",") if item.strip()]


def _env_year_float_map(key: str, default: dict[int, float]) -> dict[int, float]:
    value = os.getenv(key)
    if not value:
        return dict(default)
    result: dict[int, float] = {}
    for item in value.split(","):
        chunk = item.strip()
        if not chunk or "=" not in chunk:
            continue
        year_text, amount_text = chunk.split("=", 1)
        try:
            year = int(year_text.strip())
            amount_raw = amount_text.strip()
            if "," in amount_raw and "." in amount_raw:
                amount_raw = amount_raw.replace(".", "").replace(",", ".")
            else:
                amount_raw = amount_raw.replace(",", ".")
            amount = float(amount_raw)
        except ValueError:
            continue
        result[year] = amount
    return result or dict(default)


def _env_date(key: str) -> date | None:
    value = os.getenv(key)
    if not value:
        return None
    try:
        return datetime.strptime(value.strip(), "%Y-%m-%d").date()
    except ValueError:
        return None


def get_settings() -> Settings:
    data_dir = Path(os.getenv("BURP_DATA_DIR", "data")).resolve()
    db_path = Path(os.getenv("BURP_DB_PATH", str(data_dir / "burp.db"))).resolve()
    return Settings(
        data_dir=data_dir,
        db_path=db_path,
        api_cors_origins=_env_list("BURP_CORS_ORIGINS") or [
            "http://localhost:5173",
            "http://127.0.0.1:5173",
        ],
        ckan_base_url=os.getenv("BURP_CKAN_BASE_URL", "https://dados.es.gov.br"),
        vitoria_base_url=os.getenv("BURP_VITORIA_BASE_URL", "https://wstransparencia.vitoria.es.gov.br"),
        vilavelha_base_url=os.getenv("BURP_VILAVELHA_BASE_URL", "https://wstransparencia.vilavelha.es.gov.br"),
        facto_base_url=os.getenv("BURP_FACTO_BASE_URL", "https://facto.conveniar.com.br/portaltransparencia"),
        federal_base_url=os.getenv("BURP_FEDERAL_BASE_URL", "https://api.portaldatransparencia.gov.br/api-de-dados"),
        federal_busca_base_url=os.getenv(
            "BURP_FEDERAL_BUSCA_BASE_URL",
            "https://busca.portaldatransparencia.gov.br",
        ),
        federal_portal_base_url=os.getenv(
            "BURP_FEDERAL_PORTAL_BASE_URL",
            "https://www.portaltransparencia.gov.br",
        ),
        federal_api_key=os.getenv("BURP_FEDERAL_API_KEY"),
        federal_orgao_siape=os.getenv("BURP_FEDERAL_ORGAO_SIAPE"),
        federal_mes_ano=os.getenv("BURP_FEDERAL_MES_ANO"),
        federal_max_servers=int(os.getenv("BURP_FEDERAL_MAX_SERVERS", "5")),
        federal_cpfs=_env_list("BURP_FEDERAL_CPFS"),
        federal_ids=_env_list("BURP_FEDERAL_IDS"),
        federal_timeout=int(os.getenv("BURP_FEDERAL_TIMEOUT", "30")),
        federal_favorecido_page_size=int(os.getenv("BURP_FEDERAL_FAVORECIDO_PAGE_SIZE", "100")),
        federal_favorecido_max_pages=int(os.getenv("BURP_FEDERAL_FAVORECIDO_MAX_PAGES", "10")),
        federal_favorecido_fases=_env_list("BURP_FEDERAL_FAVORECIDO_FASES") or ["3"],
        facto_days=int(os.getenv("BURP_FACTO_DAYS", "30")),
        facto_window_days=int(os.getenv("BURP_FACTO_WINDOW_DAYS", "0")),
        facto_start_date=_env_date("BURP_FACTO_START_DATE"),
        facto_end_date=_env_date("BURP_FACTO_END_DATE"),
        source_vitoria_enabled=_env_bool("BURP_SOURCE_VITORIA_ENABLED", True),
        source_vilavelha_enabled=_env_bool("BURP_SOURCE_VILAVELHA_ENABLED", True),
        source_ckan_enabled=_env_bool("BURP_SOURCE_CKAN_ENABLED", True),
        source_fapes_enabled=_env_bool("BURP_SOURCE_FAPES_ENABLED", True),
        source_facto_enabled=_env_bool("BURP_SOURCE_FACTO_ENABLED", True),
        source_federal_enabled=_env_bool("BURP_SOURCE_FEDERAL_ENABLED", False),
        diaria_keywords=_env_list("BURP_DIARIA_KEYWORDS"),
        bolsa_keywords=_env_list("BURP_BOLSA_KEYWORDS"),
        diaria_primary_fields=_env_list("BURP_DIARIA_PRIMARY_FIELDS"),
        diaria_strong_fields=_env_list("BURP_DIARIA_STRONG_FIELDS"),
        diaria_primary_jsonpaths=_env_list("BURP_DIARIA_PRIMARY_JSONPATHS"),
        diaria_strong_jsonpaths=_env_list("BURP_DIARIA_STRONG_JSONPATHS"),
        tipo_classification_debug=_env_bool("BURP_TIPO_CLASSIFICATION_DEBUG", False),
        teto_constitucional_by_year=_env_year_float_map(
            "BURP_TETO_CONSTITUCIONAL_BY_YEAR",
            DEFAULT_TETO_CONSTITUCIONAL_BY_YEAR,
        ),
    )
