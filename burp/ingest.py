from __future__ import annotations

from burp.connectors.base import IngestResult
from burp.connectors.facto import ingest_facto
from burp.connectors.fapes import ingest_fapes
from burp.connectors.portal_federal import ingest_portal_federal
from burp.connectors.ckan_es import ingest_ckan_pessoal
from burp.connectors.transparenciaweb import ingest_transparenciaweb
from burp.connectors.sources import list_sources_meta
from burp.er.clustering import build_clusters
from burp.settings import get_settings
from burp.storage import ensure_sources, init_db, refresh_clusters


TARGETS = {
    "vitoria": "vitoria_pessoal",
    "vilavelha": "vilavelha_pessoal",
    "ckan": "es_ckan_pessoal",
    "fapes": "fapes_bolsas",
    "facto": "facto_conveniar",
    "federal": "portal_federal_remuneracao",
}


def run_ingest(targets: list[str] | None = None, facto_nome: str | None = None) -> dict:
    init_db()
    ensure_sources([meta.__dict__ for meta in list_sources_meta()])
    settings = get_settings()

    selected = set(targets or ["all"])
    results: list[IngestResult] = []

    def wants(name: str) -> bool:
        return "all" in selected or name in selected

    if wants("vitoria") and settings.source_vitoria_enabled:
        results.append(ingest_transparenciaweb("vitoria_pessoal", "Vitoria"))
    if wants("vilavelha") and settings.source_vilavelha_enabled:
        results.append(ingest_transparenciaweb("vilavelha_pessoal", "Vila Velha"))
    if wants("ckan") and settings.source_ckan_enabled:
        results.append(ingest_ckan_pessoal())
    if wants("fapes") and settings.source_fapes_enabled:
        results.append(ingest_fapes())
    if wants("facto") and (settings.source_facto_enabled or facto_nome):
        results.append(ingest_facto(facto_nome))
    if wants("federal") and settings.source_federal_enabled:
        results.append(ingest_portal_federal())

    clusters = build_clusters()
    refresh_clusters(clusters)

    return {"results": [r.__dict__ for r in results]}
