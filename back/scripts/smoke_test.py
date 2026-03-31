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

from burp.api.app import app
from burp.connectors.sources import list_sources_meta
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
            "source_id": "vitoria_pessoal",
            "raw_id": None,
            "orgao": "PREFEITURA DE VITORIA",
            "tipo_recebimento": "FOLHA",
            "competencia": "2025-01",
            "data_pagamento": "2025-01-31",
            "valor_bruto": 42000.0,
            "descontos": 2000.0,
            "valor_liquido": 40000.0,
            "cargo_funcao": "SERVIDORA",
            "detalhes_json": {"raw": {"competencia": "2025-01", "rubrica": "SALARIO BASE"}},
            "source_url": "fixture://vitoria/2025-01",
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
            "source_id": "portal_federal_favorecido",
            "raw_id": None,
            "orgao": "TESTE DIARIA",
            "tipo_recebimento": "BOLSA",
            "competencia": None,
            "data_pagamento": "2025-01-20",
            "valor_bruto": 3000.0,
            "descontos": None,
            "valor_liquido": 3000.0,
            "cargo_funcao": None,
            "detalhes_json": {"raw": {"elemento": "14 - Diárias - Civil"}},
            "source_url": "fixture://diaria/2025-01",
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
            "source_id": "vitoria_pessoal",
            "raw_id": None,
            "orgao": "PREFEITURA DE VITORIA",
            "tipo_recebimento": "FOLHA",
            "competencia": "2025-02",
            "data_pagamento": "2025-02-28",
            "valor_bruto": 43000.0,
            "descontos": 0.0,
            "valor_liquido": 43000.0,
            "cargo_funcao": "SERVIDORA",
            "detalhes_json": {"raw": {"competencia": "2025-02", "rubrica": "SALARIO BASE"}},
            "source_url": "fixture://vitoria/2025-02",
        },
    ]


def main() -> None:
    init_db()
    ensure_sources([meta.__dict__ for meta in list_sources_meta()])
    insert_records(_fixture_records())
    client = TestClient(app)

    health = client.get("/health")
    sources = client.get("/sources")
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
    search_payload = search.json()

    if health_payload.get("status") != "ok":
        raise AssertionError("health endpoint did not return ok")
    if not sources_payload.get("sources"):
        raise AssertionError("sources endpoint returned empty list")
    if search_payload.get("rebusca", {}).get("performed"):
        raise AssertionError("search should be read-only by default")

    january = next(item for item in search_payload["period_report"]["monthly"] if item["month"] == "2025-01")
    february = next(item for item in search_payload["period_report"]["monthly"] if item["month"] == "2025-02")
    if not january["estourou_teto"]:
        raise AssertionError("january should exceed the configured ceiling")
    if february["estourou_teto"]:
        raise AssertionError("february should stay below the configured ceiling")
    if january["totals_by_tipo"].get("DIARIA") != 3000.0:
        raise AssertionError("diaria total not captured in period report")

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
