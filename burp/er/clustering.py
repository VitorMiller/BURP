from __future__ import annotations

import hashlib
from collections import defaultdict
from typing import Iterable

from burp.storage import list_all_records


def _cluster_id(name_norm: str, municipio: str | None, orgao: str | None) -> str:
    base = f"{name_norm}|{municipio or ''}|{orgao or ''}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def _confidence(person_hint_ids: list[str], municipios: list[str], orgaos: list[str]) -> str:
    hint_ids = [value for value in person_hint_ids if value]
    if hint_ids and len(set(hint_ids)) == 1 and len(set(municipios)) <= 1 and len(set(orgaos)) <= 1:
        return "HIGH"
    if hint_ids:
        return "MEDIUM"
    return "LOW"


def build_clusters(records: Iterable[dict] | None = None) -> list[dict[str, object]]:
    if records is None:
        records = list_all_records()
    grouped: dict[str, list[dict]] = defaultdict(list)
    for record in records:
        grouped[record.get("person_name_norm") or ""].append(record)

    clusters: list[dict[str, object]] = []
    for name_norm, items in grouped.items():
        if not name_norm:
            continue
        municipios_set = {item.get("municipio") for item in items}
        orgaos_set = {item.get("orgao") for item in items}
        split = len(municipios_set) > 1 or len(orgaos_set) > 1
        municipios = list(municipios_set)
        orgaos = list(orgaos_set)

        def _render(values: list[str | None]) -> list[str]:
            rendered = []
            for value in values:
                if not value:
                    rendered.append("UNKNOWN")
                else:
                    rendered.append(str(value))
            return sorted(set(rendered))
        if split:
            subgroups: dict[tuple[str | None, str | None], list[dict]] = defaultdict(list)
            for item in items:
                key = (item.get("municipio"), item.get("orgao"))
                subgroups[key].append(item)
            for (municipio, orgao), rows in subgroups.items():
                hint_ids = [row.get("person_hint_id") for row in rows]
                cluster_id = _cluster_id(name_norm, municipio, orgao)
                clusters.append(
                    {
                        "cluster_id": cluster_id,
                        "person_name_norm": name_norm,
                        "municipio": municipio,
                        "orgao": orgao,
                        "confidence": _confidence(hint_ids, municipios, orgaos),
                        "evidence": {
                            "rule": "split_by_municipio_orgao",
                            "municipios": _render(municipios),
                            "orgaos": _render(orgaos),
                            "person_hint_ids": sorted({hid for hid in hint_ids if hid}),
                            "records": len(rows),
                        },
                    }
                )
        else:
            hint_ids = [row.get("person_hint_id") for row in items]
            municipio = municipios[0] if municipios else None
            orgao = orgaos[0] if orgaos else None
            cluster_id = _cluster_id(name_norm, municipio, orgao)
            clusters.append(
                {
                    "cluster_id": cluster_id,
                    "person_name_norm": name_norm,
                    "municipio": municipio,
                    "orgao": orgao,
                    "confidence": _confidence(hint_ids, municipios, orgaos),
                    "evidence": {
                        "rule": "single_group",
                        "municipios": _render(municipios),
                        "orgaos": _render(orgaos),
                        "person_hint_ids": sorted({hid for hid in hint_ids if hid}),
                        "records": len(items),
                    },
                }
            )
    return clusters


def cluster_records(records: list[dict], limit_per_cluster: int = 10) -> list[dict[str, object]]:
    clusters = build_clusters(records)
    index: dict[str, list[dict]] = defaultdict(list)
    for record in records:
        cluster_id = _cluster_id(record.get("person_name_norm") or "", record.get("municipio"), record.get("orgao"))
        index[cluster_id].append(record)
    results = []
    for cluster in clusters:
        rows = index.get(cluster["cluster_id"], [])
        cluster_copy = dict(cluster)
        cluster_copy["top_records"] = rows[:limit_per_cluster]
        results.append(cluster_copy)
    return results


def cluster_id_for_record(record: dict) -> str:
    return _cluster_id(record.get("person_name_norm") or "", record.get("municipio"), record.get("orgao"))
