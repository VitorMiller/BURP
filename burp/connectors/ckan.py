from __future__ import annotations

import requests


def ckan_package_show(base_url: str, dataset_id: str) -> dict:
    url = f"{base_url}/api/3/action/package_show"
    resp = requests.get(url, params={"id": dataset_id}, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    if not data.get("success"):
        raise RuntimeError(f"CKAN package_show failed: {data}")
    return data["result"]


def ckan_package_search(base_url: str, query: str) -> dict:
    url = f"{base_url}/api/3/action/package_search"
    resp = requests.get(url, params={"q": query}, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    if not data.get("success"):
        raise RuntimeError(f"CKAN package_search failed: {data}")
    return data["result"]
