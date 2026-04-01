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
from burp.connectors.facto import _extract_facto_rows, _map_facto_rows
from burp.connectors.fapes import _select_xlsx_resources
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
            "valor_bruto": 44300.0,
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
            "competencia": "2025-01",
            "data_pagamento": "2025-02-05",
            "valor_bruto": 2000.0,
            "descontos": None,
            "valor_liquido": 2000.0,
            "cargo_funcao": "Pagamento de Bolsa Pesquisa",
            "detalhes_json": {
                "categoria": "pessoas_fisicas",
                "periodo": "01/02/2025 - 28/02/2025",
                "raw": {
                    "CodLancamento": 123,
                    "NomeConvenio": "Projeto Teste",
                    "NomeTipoPedido": "Pagamento de Bolsa Pesquisa",
                    "DataPagamento": "2025-02-05T00:00:00",
                    "DataCompetencia": "2025-01-01T00:00:00",
                    "Valor": 2000.0,
                },
            },
            "source_url": "fixture://facto/2025-02",
        },
        {
            **common,
            "source_id": "facto_conveniar",
            "raw_id": None,
            "orgao": "FACTO",
            "tipo_recebimento": "BOLSA",
            "competencia": None,
            "data_pagamento": None,
            "valor_bruto": 9999.0,
            "descontos": None,
            "valor_liquido": 9999.0,
            "cargo_funcao": None,
            "detalhes_json": {
                "categoria": "pessoas_fisicas",
                "periodo": "01/02/2025 - 28/02/2025",
                "raw": {"favorecido": nome, "valor total recebido": "R$ 9.999,00"},
            },
            "source_url": "fixture://facto/legacy-window-total",
        },
        {
            **common,
            "source_id": "portal_federal_remuneracao",
            "raw_id": None,
            "orgao": "Instituto Federal do Espírito Santo",
            "tipo_recebimento": "FOLHA",
            "competencia": "2025-02",
            "data_pagamento": None,
            "valor_bruto": 44300.0,
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


def _assert_fapes_resource_selection() -> None:
    resources = [
        {"name": "FAPES Bolsas 2023.xlsx", "url": "https://example.test/fapes-2023.xlsx"},
        {"name": "FAPES Bolsas 2024.xlsx", "url": "https://example.test/fapes-2024.xlsx"},
        {"name": "FAPES Bolsas 2025.xlsx", "url": "https://example.test/fapes-2025.xlsx"},
        {"name": "FAPES Bolsas 2026.xlsx", "url": "https://example.test/fapes-2026.xlsx"},
    ]
    selected = _select_xlsx_resources(resources)
    selected_names = [item["name"] for item in selected]
    if selected_names != [
        "FAPES Bolsas 2024.xlsx",
        "FAPES Bolsas 2025.xlsx",
        "FAPES Bolsas 2026.xlsx",
    ]:
        raise AssertionError(f"FAPES should ingest 2024+ resources, got: {selected_names}")


def _assert_fapes_report_dedup() -> None:
    record_base = {
        "source_id": "fapes_bolsas",
        "person_name_original": "MARIA DE TESTE",
        "person_name_norm": normalize_name("MARIA DE TESTE"),
        "person_hint_id": None,
        "uf": "ES",
        "municipio": None,
        "orgao": "FAPES",
        "tipo_recebimento": "BOLSA",
        "valor_bruto": 1234.56,
        "descontos": None,
        "valor_liquido": 1234.56,
        "cargo_funcao": "PESQUISA",
        "source_url": "fixture://fapes/2025",
        "collected_at": now_utc_iso(),
        "parser_version": "smoke-test",
        "detalhes_json": {
            "raw": {
                "nome": "MARIA DE TESTE",
                "programa": "PESQUISA",
                "valor": "1234,56",
                "data de credito": "2025-01-14 00:00:00",
            }
        },
    }
    deduped = api_app_module._dedup_records(
        [
            {**record_base, "competencia": "2025", "data_pagamento": None, "cargo_funcao": None},
            {**record_base, "competencia": "2025-01", "data_pagamento": "2025-01-14", "cargo_funcao": "PESQUISA"},
        ]
    )
    if len(deduped) != 1:
        raise AssertionError("FAPES yearly and monthly versions of the same raw row should dedupe in reports")


def _assert_facto_is_bolsa() -> None:
    rows = [
        {
            "favorecido": "MARIA DE TESTE",
            "cpf": "***.123.456-**",
            "NomeConvenio": "Projeto Teste",
            "NomeTipoPedido": "Pagamento de Bolsa Pesquisa",
            "DataPagamento": "2025-02-05T00:00:00",
            "DataCompetencia": "2025-01-01T00:00:00",
            "Valor": 2000.0,
            "CodLancamento": 123,
            "_facto_detail": {
                "NomeConvenio": "Projeto Teste",
                "NomeTipoPedido": "Pagamento de Bolsa Pesquisa",
                "DataPagamento": "2025-02-05T00:00:00",
                "DataCompetencia": "2025-01-01T00:00:00",
                "Valor": 2000.0,
                "CodLancamento": 123,
            },
        }
    ]
    mapped = _map_facto_rows(
        rows,
        source_id="facto_conveniar",
        raw_id=1,
        source_url="fixture://facto/servidores",
        collected_at=now_utc_iso(),
        categoria="servidores",
        periodo="01/02/2025 - 28/02/2025",
        default_cpf="12345678901",
    )
    if not mapped:
        raise AssertionError("FACTO fixture should produce at least one mapped row")
    if mapped[0].get("tipo_recebimento") != "BOLSA":
        raise AssertionError("FACTO should always be classified as BOLSA")
    if mapped[0].get("competencia") != "2025-01":
        raise AssertionError("FACTO detailed rows should preserve monthly competence")
    if mapped[0].get("data_pagamento") != "2025-02-05":
        raise AssertionError("FACTO detailed rows should preserve payment date")


def _assert_facto_detail_html_parser() -> None:
    html = """
    <table id="gvPagamentosServidor">
      <thead>
        <tr>
          <th></th><th>CPF</th><th>Matrícula do servidor</th><th>Favorecido</th><th>Valor total recebido</th>
        </tr>
      </thead>
      <tbody>
        <tr class="gridRow expandir" data-rowindex="0"
            data-pagamentos='[{"CodLancamento":123,"NomeConvenio":"Projeto Teste","NomeTipoPedido":"Pagamento de Bolsa Pesquisa","DataPagamento":"2025-02-05T00:00:00","DataCompetencia":"2025-01-01T00:00:00","Valor":2000.0}]'>
          <td></td><td>***.123.456-**</td><td>181****</td><td>MARIA DE TESTE</td><td>R$ 2.000,00</td>
        </tr>
      </tbody>
    </table>
    """
    rows = _extract_facto_rows(html, "gvPagamentosServidor")
    if len(rows) != 1:
        raise AssertionError(f"FACTO parser should extract detailed payment rows, got: {rows}")
    if rows[0].get("favorecido") != "MARIA DE TESTE":
        raise AssertionError("FACTO parser should keep beneficiary from summary row")
    if rows[0].get("CodLancamento") != 123:
        raise AssertionError("FACTO parser should expose detailed payment identity")


def _assert_facto_report_dedup() -> None:
    record_base = {
        "source_id": "facto_conveniar",
        "person_name_original": "MARIA DE TESTE",
        "person_name_norm": normalize_name("MARIA DE TESTE"),
        "person_hint_id": "***.123.456-**",
        "uf": "ES",
        "municipio": None,
        "orgao": "FACTO",
        "tipo_recebimento": "BOLSA",
        "competencia": "2025-01",
        "data_pagamento": "2025-02-05",
        "valor_bruto": 2000.0,
        "descontos": None,
        "valor_liquido": 2000.0,
        "cargo_funcao": "Pagamento de Bolsa Pesquisa",
        "source_url": "fixture://facto/a",
        "collected_at": now_utc_iso(),
        "parser_version": "smoke-test",
        "detalhes_json": {
            "categoria": "servidores",
            "periodo": "01/02/2025 - 28/02/2025",
            "raw": {"CodLancamento": 123, "Valor": 2000.0},
        },
    }
    deduped = api_app_module._dedup_records(
        [
            record_base,
            {
                **record_base,
                "source_url": "fixture://facto/b",
                "detalhes_json": {
                    "categoria": "pessoas_fisicas",
                    "periodo": "01/01/2025 - 31/12/2025",
                    "raw": {"CodLancamento": 123, "Valor": 2000.0},
                },
            },
        ]
    )
    if len(deduped) != 1:
        raise AssertionError("FACTO payments with the same CodLancamento should dedupe in reports")


def main() -> None:
    _assert_facto_is_bolsa()
    _assert_facto_detail_html_parser()
    _assert_facto_report_dedup()
    _assert_fapes_resource_selection()
    _assert_fapes_report_dedup()
    init_db()
    ensure_sources([meta.__dict__ for meta in list_sources_meta()])
    insert_records(_fixture_records())
    client = TestClient(app)
    captured_facto_calls: list[tuple[str | None, str | None, str | None, str | None]] = []

    def fake_ingest_facto(nome: str | None, cpf: str | None = None, start_date=None, end_date=None) -> IngestResult:
        captured_facto_calls.append(
            (
                nome,
                cpf,
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
            "cpf": "123.123.123-12",
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
    if captured_facto_calls != [("MARIA DE TESTE", "12312312312", "2025-02-01", "2025-02-28")]:
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
    if january["totals_by_source"].get("portal_federal_remuneracao") != 44300.0:
        raise AssertionError("portal federal salary should use valor_bruto in aggregation")
    if february["totals_by_source"].get("facto_conveniar") != 2000.0:
        raise AssertionError("facto total not captured in period report")
    if february["totals_by_source"].get("portal_federal_remuneracao") != 44300.0:
        raise AssertionError("portal federal salary should use valor_bruto in monthly totals")
    if search_payload["period_report"].get("calculation_basis") != (
        "portal_federal_uses_valor_bruto_other_sources_use_valor_liquido_or_valor_bruto"
    ):
        raise AssertionError("period report should expose the updated calculation basis")

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
