from __future__ import annotations

from datetime import date

from burp.connectors.base import IngestResult
from burp.connectors.conveniar import (
    _extract_conveniar_rows as _extract_facto_rows,
    _map_conveniar_rows,
    ingest_conveniar,
)
from burp.settings import get_settings


def _map_facto_rows(*args, **kwargs):
    kwargs.setdefault("orgao", "FACTO")
    return _map_conveniar_rows(*args, **kwargs)


def ingest_facto(
    nome: str | None,
    cpf: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> IngestResult:
    settings = get_settings()
    return ingest_conveniar(
        nome,
        cpf=cpf,
        start_date=start_date,
        end_date=end_date,
        source_id="facto_conveniar",
        orgao="FACTO",
        base_url=settings.facto_base_url,
        data_dir=settings.data_dir,
        default_days=settings.facto_days,
        window_days=settings.facto_window_days,
        configured_start_date=settings.facto_start_date,
        configured_end_date=settings.facto_end_date,
        raw_file_prefix="facto",
    )
