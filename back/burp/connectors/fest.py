from __future__ import annotations

from datetime import date

from burp.connectors.base import IngestResult
from burp.connectors.conveniar import ingest_conveniar
from burp.settings import get_settings


def ingest_fest(
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
        source_id="fest_conveniar",
        orgao="FEST",
        base_url=settings.fest_base_url,
        data_dir=settings.data_dir,
        default_days=settings.fest_days,
        window_days=settings.fest_window_days,
        configured_start_date=settings.fest_start_date,
        configured_end_date=settings.fest_end_date,
        raw_file_prefix="fest",
    )
