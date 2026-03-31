from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from fastapi.testclient import TestClient

from burp.api.app import app
from burp.normalization.name import normalize_name
from burp.settings import get_settings
from burp.storage import insert_records
from burp.utils import now_utc_iso


def _distinct_names_by_source(db_path: Path, source_id: str, limit: int = 1) -> list[str]:
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            """
            SELECT DISTINCT person_name_original
            FROM records
            WHERE source_id = ? AND person_name_original IS NOT NULL AND person_name_original != ''
            LIMIT ?
            """,
            (source_id, limit),
        ).fetchall()
        return [row[0] for row in rows]
    finally:
        conn.close()


def main() -> None:
    settings = get_settings()
    client = TestClient(app)

    ingest_targets = ["vitoria", "vilavelha", "ckan", "fapes"]
    if settings.federal_api_key:
        ingest_targets.append("federal")
    ingest_payload = {"targets": ingest_targets}
    ingest_resp = client.post("/ingest/run", json=ingest_payload)
    ingest_data = ingest_resp.json()

    synthetic_name = "TESTE DIARIA"
    synthetic_record = {
        "source_id": "portal_federal_favorecido",
        "raw_id": None,
        "person_name_original": synthetic_name,
        "person_name_norm": normalize_name(synthetic_name),
        "person_hint_id": "TESTE_DIARIA",
        "uf": "BR",
        "municipio": None,
        "orgao": "TESTE",
        "tipo_recebimento": "BOLSA",
        "competencia": "2024-01",
        "data_pagamento": "2024-01-15",
        "valor_bruto": 100.0,
        "descontos": None,
        "valor_liquido": 100.0,
        "cargo_funcao": None,
        "detalhes_json": {
            "categoria": "despesas_favorecido",
            "raw": {"elemento": "14 - Diárias - Civil"},
        },
        "source_url": "synthetic://diaria",
        "collected_at": now_utc_iso(),
        "parser_version": "smoke-test",
    }
    insert_records([synthetic_record])

    tests = []
    resp = client.get(
        "/search",
        params={"nome": synthetic_name, "uf": "ES", "tipo": "todos", "rebusca": "false"},
    )
    payload = resp.json()
    records = []
    for cluster in payload.get("clusters", []):
        records.extend(cluster.get("top_records", []))
    diaria_record = next((rec for rec in records if rec.get("tipo_recebimento") == "DIARIA"), None)
    if not diaria_record:
        raise AssertionError("synthetic diaria record not classified as DIARIA")
    if diaria_record.get("tipo_original") != "BOLSA":
        raise AssertionError("tipo_original not set for synthetic diaria record")
    tipo_reason = diaria_record.get("tipo_reason") or ""
    if not tipo_reason:
        raise AssertionError("tipo_reason not set for synthetic diaria record")
    if "primary_jsonpath_match" not in tipo_reason or "$.raw.elemento" not in tipo_reason:
        raise AssertionError("tipo_reason missing jsonpath evidence for synthetic diaria record")
    diaria_total = payload.get("summary", {}).get("total_value_by_tipo", {}).get("DIARIA", 0)
    if not diaria_total:
        raise AssertionError("summary missing DIARIA total for synthetic record")
    tests.append({"source": "synthetic_diaria", "name": synthetic_name, "response": payload})
    sources = [
        ("vitoria_pessoal", "vitoria"),
        ("vilavelha_pessoal", "vilavelha"),
        ("fapes_bolsas", "fapes"),
    ]
    for source_id, label in sources:
        names = _distinct_names_by_source(settings.db_path, source_id, limit=1)
        if not names:
            continue
        name = names[0]
        resp = client.get(
            "/search",
            params={"nome": name, "uf": "ES", "tipo": "todos", "rebusca": "false"},
        )
        tests.append({"source": label, "name": name, "response": resp.json()})

    # Add up to 2 extra names for coverage
    conn = sqlite3.connect(settings.db_path)
    try:
        rows = conn.execute(
            """
            SELECT DISTINCT person_name_original
            FROM records
            WHERE person_name_original IS NOT NULL AND person_name_original != ''
            LIMIT 5
            """
        ).fetchall()
        extra_names = [row[0] for row in rows]
    finally:
        conn.close()
    for name in extra_names:
        if len(tests) >= 5:
            break
        resp = client.get(
            "/search",
            params={"nome": name, "uf": "ES", "tipo": "todos", "rebusca": "false"},
        )
        tests.append({"source": "mixed", "name": name, "response": resp.json()})

    output = {
        "timestamp": now_utc_iso(),
        "ingest": ingest_data,
        "tests": tests,
    }
    artifacts_dir = Path("artifacts")
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    output_path = artifacts_dir / "smoke_results.json"
    output_path.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Saved {output_path}")


if __name__ == "__main__":
    main()
