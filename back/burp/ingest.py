from __future__ import annotations

from burp.connectors.base import IngestResult
from burp.connectors.facto import ingest_facto
from burp.connectors.fapes import ingest_fapes
from burp.connectors.portal_federal import ingest_portal_federal
from burp.connectors.sources import list_sources_meta
from burp.er.clustering import build_clusters
from burp.storage import ensure_sources, init_db, refresh_clusters


TARGETS = {
    "federal": "portal_federal_remuneracao",
    "fapes": "fapes_bolsas",
    "facto": "facto_conveniar",
}


def run_ingest(
    targets: list[str] | None = None,
    facto_nome: str | None = None,
    facto_cpf: str | None = None,
) -> dict:
    init_db()
    ensure_sources([meta.__dict__ for meta in list_sources_meta()])

    selected = set(targets or ["all"])
    results: list[IngestResult] = []

    def wants(name: str) -> bool:
        return "all" in selected or name in selected

    if wants("federal"):
        results.append(ingest_portal_federal())
    if wants("fapes"):
        results.append(ingest_fapes())
    if wants("facto"):
        results.append(ingest_facto(facto_nome, cpf=facto_cpf))

    clusters = build_clusters()
    refresh_clusters(clusters)

    return {"results": [r.__dict__ for r in results]}
