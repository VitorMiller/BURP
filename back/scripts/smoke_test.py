from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

TEMP_DIR = tempfile.TemporaryDirectory(prefix="burp-smoke-")
TEMP_PATH = Path(TEMP_DIR.name)
os.environ["BURP_DATA_DIR"] = str(TEMP_PATH / "data")
os.environ["BURP_DB_PATH"] = str(TEMP_PATH / "data" / "burp-smoke.db")

from fastapi.testclient import TestClient

import burp.api.app as api_app_module
from burp.api.app import app
from burp.connectors.sources import list_sources_meta
from burp.connectors.base import IngestResult
from burp.normalization.name import normalize_name
from burp.storage import ensure_sources, init_db, insert_records
from burp.utils import now_utc_iso


def _fixture_records() -> list[dict[str, object]]:
    nome = "MARIA DE TESTE"
    nome_norm = normalize_name(nome)
    common = {
        "person_name_original": nome,
        "person_name_norm": nome_norm,
        "person_hint_id": "***.123.456-**",
        "uf": "ES",
        "municipio": "VITORIA",
        "collected_at": now_utc_iso(),
        "parser_version": "smoke-test",
    }
    return [
        {
            **common,
            "source_id": "portal_federal_remuneracao",
            "raw_id": None,
            "orgao": "Instituto Federal do Espírito Santo",
            "tipo_recebimento": "FOLHA",
            "competencia": "2025-01",
            "data_pagamento": None,
            "valor_bruto": 43000.0,
            "descontos": 0.0,
            "valor_liquido": 43000.0,
            "cargo_funcao": "DOCENTE",
            "detalhes_json": {
                "servidor": {
                    "estadoExercicio": {"sigla": "ES", "nome": "Espírito Santo"},
                    "orgaoServidorLotacao": {"sigla": "IFES", "nome": "Instituto Federal do Espírito Santo"},
                },
                "remuneracao": {"mesAno": "01/2025"},
            },
            "source_url": "fixture://portal-federal/2025-01",
        },
        {
            **common,
            "source_id": "fapes_bolsas",
            "raw_id": None,
            "orgao": "FAPES",
            "tipo_recebimento": "BOLSA",
            "competencia": "2025-01",
            "data_pagamento": "2025-01-14",
            "valor_bruto": 5000.0,
            "descontos": None,
            "valor_liquido": 5000.0,
            "cargo_funcao": "PESQUISA",
            "detalhes_json": {"raw": {"data de credito": "2025-01-14 00:00:00", "perfil": "Pesquisador(a)"}},
            "source_url": "fixture://fapes/2025-01",
        },
        {
            **common,
            "source_id": "facto_conveniar",
            "raw_id": None,
            "orgao": "FACTO",
            "tipo_recebimento": "BOLSA",
            "competencia": None,
            "data_pagamento": None,
            "valor_bruto": 2000.0,
            "descontos": None,
            "valor_liquido": 2000.0,
            "cargo_funcao": None,
            "detalhes_json": {"categoria": "pessoas_fisicas", "periodo": "01/02/2025 - 28/02/2025", "raw": {"favorecido": nome}},
            "source_url": "fixture://facto/2025-02",
        },
        {
            **common,
            "source_id": "portal_federal_remuneracao",
            "raw_id": None,
            "orgao": "Instituto Federal do Espírito Santo",
            "tipo_recebimento": "FOLHA",
            "competencia": "2025-02",
            "data_pagamento": None,
            "valor_bruto": 43000.0,
            "descontos": 0.0,
            "valor_liquido": 43000.0,
            "cargo_funcao": "DOCENTE",
            "detalhes_json": {
                "servidor": {
                    "estadoExercicio": {"sigla": "ES", "nome": "Espírito Santo"},
                    "orgaoServidorLotacao": {"sigla": "IFES", "nome": "Instituto Federal do Espírito Santo"},
                },
                "remuneracao": {"mesAno": "02/2025"},
            },
            "source_url": "fixture://portal-federal/2025-02",
        },
    ]


def main() -> None:
    init_db()
    ensure_sources([meta.__dict__ for meta in list_sources_meta()])
    insert_records(_fixture_records())
    client = TestClient(app)
    captured_facto_calls: list[tuple[str | None, str | None, str | None]] = []

    def fake_ingest_facto(nome: str | None, start_date=None, end_date=None) -> IngestResult:
        captured_facto_calls.append(
            (
                nome,
                start_date.isoformat() if start_date else None,
                end_date.isoformat() if end_date else None,
            )
        )
        return IngestResult(
            source_id="facto_conveniar",
            status="ok",
            records=0,
            raw_files=0,
            notes="fixture-refresh",
        )

    api_app_module.ingest_facto = fake_ingest_facto

    health = client.get("/health")
    sources = client.get("/sources")
    refresh = client.post(
        "/refresh/query",
        json={
            "nome": "MARIA DE TESTE",
            "data_inicio": "2025-02-01",
            "data_fim": "2025-02-28",
            "include_fapes": False,
            "include_facto": True,
            "include_federal": False,
        },
    )
    search = client.get(
        "/search",
        params={
            "nome": "MARIA DE TESTE",
            "uf": "ES",
            "tipo": "todos",
            "data_inicio": "2025-01-01",
            "data_fim": "2025-12-31",
        },
    )

    health_payload = health.json()
    sources_payload = sources.json()
    refresh_payload = refresh.json()
    search_payload = search.json()

    if health_payload.get("status") != "ok":
        raise AssertionError("health endpoint did not return ok")
    if not sources_payload.get("sources"):
        raise AssertionError("sources endpoint returned empty list")
    if captured_facto_calls != [("MARIA DE TESTE", "2025-02-01", "2025-02-28")]:
        raise AssertionError(f"FACTO refresh ignored requested period: {captured_facto_calls}")
    if refresh_payload.get("refresh", {}).get("period") != {"start": "2025-02-01", "end": "2025-02-28"}:
        raise AssertionError("refresh/query should echo the requested period")
    if search_payload.get("rebusca", {}).get("performed"):
        raise AssertionError("search should be read-only by default")

    january = next(item for item in search_payload["period_report"]["monthly"] if item["month"] == "2025-01")
    february = next(item for item in search_payload["period_report"]["monthly"] if item["month"] == "2025-02")
    if not january["estourou_teto"]:
        raise AssertionError("january should exceed the configured ceiling")
    if february["estourou_teto"]:
        raise AssertionError("february should stay below the configured ceiling")
    if january["totals_by_tipo"].get("BOLSA") != 5000.0:
        raise AssertionError("fapes total not captured in period report")
    if january["totals_by_source"].get("portal_federal_remuneracao") != 43000.0:
        raise AssertionError("portal federal salary should be included in aggregation")
    if february["totals_by_source"].get("facto_conveniar") != 2000.0:
        raise AssertionError("facto total not captured in period report")

    artifacts_dir = ROOT / "artifacts"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    output_path = artifacts_dir / "smoke_results.json"
    output = {
        "health": health_payload,
        "sources_count": len(sources_payload.get("sources", [])),
        "search": search_payload,
    }
    output_path.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Saved {output_path}")


if __name__ == "__main__":
    main()
