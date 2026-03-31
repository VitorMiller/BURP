from __future__ import annotations

import argparse
import json

from burp.er.clustering import cluster_records
from burp.ingest import run_ingest
from burp.normalization.name import normalize_name
from burp.normalization.recebimento import normalize_tipo, normalize_tipo_filter
from burp.storage import backfill_diaria_from_favorecido, list_sources, search_records


def cmd_ingest(args: argparse.Namespace) -> None:
    result = run_ingest(targets=args.target, facto_nome=args.facto_nome)
    print(json.dumps(result, ensure_ascii=False, indent=2))


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
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def cmd_sources(_args: argparse.Namespace) -> None:
    print(json.dumps({"sources": list_sources()}, ensure_ascii=False, indent=2))


def cmd_backfill_diaria(_args: argparse.Namespace) -> None:
    result = backfill_diaria_from_favorecido()
    print(json.dumps(result, ensure_ascii=False, indent=2))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="burp")
    sub = parser.add_subparsers(dest="command", required=True)

    ingest = sub.add_parser("ingest", help="Run ingestion")
    ingest.add_argument(
        "--target",
        action="append",
        default=["all"],
        help="Target source (vitoria, vilavelha, ckan, fapes, facto, federal)",
    )
    ingest.add_argument("--facto-nome", default=None, help="Name filter for FACTO")
    ingest.set_defaults(func=cmd_ingest)

    search = sub.add_parser("search", help="Search by name")
    search.add_argument("--nome", required=True, help="Name to search")
    search.add_argument("--tipo", default="todos", help="folha|bolsa|diaria|todos")
    search.add_argument("--uf", default="ES", help="UF")
    search.add_argument("--municipio", default=None, help="Municipio")
    search.set_defaults(func=cmd_search)

    sources = sub.add_parser("sources", help="List sources status")
    sources.set_defaults(func=cmd_sources)

    backfill = sub.add_parser("backfill-diaria", help="Backfill DIARIA from portal_federal_favorecido")
    backfill.set_defaults(func=cmd_backfill_diaria)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
