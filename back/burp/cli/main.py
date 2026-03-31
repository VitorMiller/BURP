from __future__ import annotations

import argparse
import json
from datetime import datetime

from burp.analysis import build_period_report
from burp.er.clustering import cluster_records
from burp.ingest import run_ingest
from burp.normalization.name import normalize_name
from burp.normalization.recebimento import normalize_tipo, normalize_tipo_filter
from burp.storage import list_sources, search_records


def cmd_ingest(args: argparse.Namespace) -> None:
    result = run_ingest(targets=args.target or ["all"], facto_nome=args.facto_nome)
    print(json.dumps(result, ensure_ascii=False, indent=2))


def _parse_date(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d")


def cmd_search(args: argparse.Namespace) -> None:
    name_norm = normalize_name(args.nome)
    uf_norm = args.uf.upper() if args.uf else None
    if uf_norm in {"TODOS", "ALL"}:
        uf_norm = None
    municipio_norm = normalize_name(args.municipio) if args.municipio else None
    tipo_norm = normalize_tipo(args.tipo) or "TODOS"
    tipo_filter = normalize_tipo_filter(args.tipo)
    records = search_records(name_norm, tipo_filter, uf_norm, municipio_norm)
    clusters = cluster_records(records)
    payload = {
        "query": {
            "nome": args.nome,
            "nome_norm": name_norm,
            "tipo": args.tipo,
            "tipo_norm": tipo_norm,
            "uf": uf_norm or args.uf.upper(),
            "municipio": args.municipio,
            "municipio_norm": municipio_norm,
        },
        "clusters": clusters,
        "total_records": len(records),
    }
    data_inicio = _parse_date(args.data_inicio)
    data_fim = _parse_date(args.data_fim)
    if data_inicio or data_fim:
        start = data_inicio.date() if data_inicio else datetime.strptime(f"{data_fim.year}-01-01", "%Y-%m-%d").date()
        end = data_fim.date() if data_fim else datetime.now().date()
        payload["period_report"] = build_period_report(records, start, end)
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def cmd_sources(_args: argparse.Namespace) -> None:
    print(json.dumps({"sources": list_sources()}, ensure_ascii=False, indent=2))

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="burp")
    sub = parser.add_subparsers(dest="command", required=True)

    ingest = sub.add_parser("ingest", help="Run ingestion")
    ingest.add_argument(
        "--target",
        action="append",
        default=[],
        help="Target source (federal, fapes, facto)",
    )
    ingest.add_argument("--facto-nome", default=None, help="Name filter for FACTO")
    ingest.set_defaults(func=cmd_ingest)

    search = sub.add_parser("search", help="Search by name")
    search.add_argument("--nome", required=True, help="Name to search")
    search.add_argument("--tipo", default="todos", help="folha|bolsa|todos")
    search.add_argument("--uf", default="ES", help="UF")
    search.add_argument("--municipio", default=None, help="Municipio")
    search.add_argument("--data-inicio", default=None, help="Inicio da analise YYYY-MM-DD")
    search.add_argument("--data-fim", default=None, help="Fim da analise YYYY-MM-DD")
    search.set_defaults(func=cmd_search)

    sources = sub.add_parser("sources", help="List sources status")
    sources.set_defaults(func=cmd_sources)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
