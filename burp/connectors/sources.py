from __future__ import annotations

from burp.connectors.base import SourceMeta
from burp.settings import get_settings


def list_sources_meta() -> list[SourceMeta]:
    settings = get_settings()
    return [
        SourceMeta(
            source_id="vitoria_pessoal",
            name="Vitoria - TransparenciaWeb (Pessoal)",
            base_url=settings.vitoria_base_url,
            tipo="FOLHA",
        ),
        SourceMeta(
            source_id="vilavelha_pessoal",
            name="Vila Velha - TransparenciaWeb (Pessoal)",
            base_url=settings.vilavelha_base_url,
            tipo="FOLHA",
        ),
        SourceMeta(
            source_id="es_ckan_pessoal",
            name="ES CKAN - Portal da Transparencia (Pessoal)",
            base_url=settings.ckan_base_url,
            tipo="FOLHA",
        ),
        SourceMeta(
            source_id="fapes_bolsas",
            name="FAPES - Bolsas e Auxilios",
            base_url=settings.ckan_base_url,
            tipo="BOLSA",
        ),
        SourceMeta(
            source_id="facto_conveniar",
            name="FACTO - Conveniar (Pessoas Fisicas e Servidores)",
            base_url=settings.facto_base_url,
            tipo="BOLSA",
        ),
        SourceMeta(
            source_id="portal_federal_remuneracao",
            name="Portal da Transparencia Federal - Servidores (Remuneracao)",
            base_url=settings.federal_base_url,
            tipo="FOLHA",
        ),
        SourceMeta(
            source_id="portal_federal_favorecido",
            name="Portal da Transparencia Federal - Favorecido (Despesas)",
            base_url=settings.federal_portal_base_url,
            tipo="BOLSA",
        ),
    ]
