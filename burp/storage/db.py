from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any, Iterable
import unicodedata

from burp.normalization.recebimento import infer_recebimento_tipo, normalize_tipo
from burp.settings import get_settings
from burp.utils import dump_json


logger = logging.getLogger(__name__)

SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS sources (
    source_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    base_url TEXT,
    last_run_at TEXT,
    last_status TEXT,
    last_error TEXT
);

CREATE TABLE IF NOT EXISTS raw_files (
    raw_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id TEXT,
    url TEXT,
    collected_at TEXT,
    sha256 TEXT,
    local_path TEXT,
    content_type TEXT,
    FOREIGN KEY (source_id) REFERENCES sources(source_id)
);

CREATE TABLE IF NOT EXISTS records (
    record_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id TEXT,
    raw_id INTEGER,
    person_name_original TEXT,
    person_name_norm TEXT,
    person_hint_id TEXT,
    uf TEXT,
    municipio TEXT,
    orgao TEXT,
    tipo_recebimento TEXT,
    tipo_original TEXT,
    tipo_reason TEXT,
    competencia TEXT,
    data_pagamento TEXT,
    valor_bruto REAL,
    descontos REAL,
    valor_liquido REAL,
    cargo_funcao TEXT,
    detalhes_json TEXT,
    source_url TEXT,
    collected_at TEXT,
    parser_version TEXT,
    FOREIGN KEY (source_id) REFERENCES sources(source_id),
    FOREIGN KEY (raw_id) REFERENCES raw_files(raw_id)
);

CREATE TABLE IF NOT EXISTS person_clusters (
    cluster_id TEXT PRIMARY KEY,
    person_name_norm TEXT,
    municipio TEXT,
    orgao TEXT,
    evidence_json TEXT,
    confidence TEXT
);

CREATE VIRTUAL TABLE IF NOT EXISTS records_fts USING fts5(
    person_name_norm,
    content='records',
    content_rowid='record_id'
);

CREATE INDEX IF NOT EXISTS idx_records_name_norm ON records(person_name_norm);
CREATE INDEX IF NOT EXISTS idx_records_source ON records(source_id);
"""


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

def unaccent(s: str | None) -> str:
    if not s:
        return ""
    # NFKD separa letras de acentos; removemos os marks (combining)
    return "".join(
        ch for ch in unicodedata.normalize("NFKD", s)
        if not unicodedata.combining(ch)
    )

def get_conn() -> sqlite3.Connection:
    settings = get_settings()
    settings.data_dir.mkdir(parents=True, exist_ok=True)

    conn = _connect(settings.db_path)

    # Disponibiliza UNACCENT() dentro do SQLite
    conn.create_function("UNACCENT", 1, unaccent)

    return conn


def init_db() -> None:
    settings = get_settings()
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    conn = _connect(settings.db_path)
    try:
        conn.executescript(SCHEMA)
        _ensure_records_columns(conn)
        conn.commit()
    finally:
        conn.close()


def ensure_sources(sources: Iterable[dict[str, Any]]) -> None:
    conn = get_conn()
    try:
        for source in sources:
            conn.execute(
                """
                INSERT INTO sources (source_id, name, base_url)
                VALUES (?, ?, ?)
                ON CONFLICT(source_id) DO UPDATE SET
                    name=excluded.name,
                    base_url=excluded.base_url
                """,
                (source["source_id"], source["name"], source.get("base_url")),
            )
        conn.commit()
    finally:
        conn.close()


def update_source_run(source_id: str, status: str, last_run_at: str, error: str | None = None) -> None:
    conn = get_conn()
    try:
        conn.execute(
            """
            UPDATE sources
            SET last_run_at = ?, last_status = ?, last_error = ?
            WHERE source_id = ?
            """,
            (last_run_at, status, error, source_id),
        )
        conn.commit()
    finally:
        conn.close()


def insert_raw_file(
    source_id: str,
    url: str,
    collected_at: str,
    sha256: str,
    local_path: str,
    content_type: str | None,
) -> int:
    conn = get_conn()
    try:
        cur = conn.execute(
            """
            INSERT INTO raw_files (source_id, url, collected_at, sha256, local_path, content_type)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (source_id, url, collected_at, sha256, local_path, content_type),
        )
        conn.commit()
        return int(cur.lastrowid)
    finally:
        conn.close()


def insert_records(records: Iterable[dict[str, Any]]) -> int:
    conn = get_conn()
    count = 0
    try:
        for record in records:
            _apply_tipo_classification(record)
            cur = conn.execute(
                """
                INSERT INTO records (
                    source_id, raw_id, person_name_original, person_name_norm, person_hint_id,
                    uf, municipio, orgao, tipo_recebimento, tipo_original, tipo_reason,
                    competencia, data_pagamento, valor_bruto, descontos, valor_liquido,
                    cargo_funcao, detalhes_json, source_url, collected_at, parser_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.get("source_id"),
                    record.get("raw_id"),
                    record.get("person_name_original"),
                    record.get("person_name_norm"),
                    record.get("person_hint_id"),
                    record.get("uf"),
                    record.get("municipio"),
                    record.get("orgao"),
                    record.get("tipo_recebimento"),
                    record.get("tipo_original"),
                    record.get("tipo_reason"),
                    record.get("competencia"),
                    record.get("data_pagamento"),
                    record.get("valor_bruto"),
                    record.get("descontos"),
                    record.get("valor_liquido"),
                    record.get("cargo_funcao"),
                    dump_json(record.get("detalhes_json", {})),
                    record.get("source_url"),
                    record.get("collected_at"),
                    record.get("parser_version"),
                ),
            )
            rowid = cur.lastrowid
            conn.execute(
                "INSERT INTO records_fts (rowid, person_name_norm) VALUES (?, ?)",
                (rowid, record.get("person_name_norm") or ""),
            )
            count += 1
        conn.commit()
    finally:
        conn.close()
    return count


def _ensure_records_columns(conn: sqlite3.Connection) -> None:
    rows = conn.execute("PRAGMA table_info(records)").fetchall()
    columns = {row[1] for row in rows}
    if "tipo_original" not in columns:
        conn.execute("ALTER TABLE records ADD COLUMN tipo_original TEXT")
    if "tipo_reason" not in columns:
        conn.execute("ALTER TABLE records ADD COLUMN tipo_reason TEXT")


def _apply_tipo_classification(record: dict[str, Any]) -> None:
    settings = get_settings()
    tipo_norm = normalize_tipo(record.get("tipo_recebimento"))
    if tipo_norm:
        record["tipo_recebimento"] = tipo_norm
    tipo_infer, reason = infer_recebimento_tipo(record, record.get("source_id"))
    if tipo_infer and tipo_infer != tipo_norm:
        record.setdefault("tipo_original", tipo_norm)
        record["tipo_recebimento"] = tipo_infer
    if reason:
        record.setdefault("tipo_reason", reason)
    if settings.tipo_classification_debug:
        logger.info(
            "tipo_classification source_id=%s raw_id=%s tipo_inicial=%s tipo_final=%s tipo_reason=%s",
            record.get("source_id"),
            record.get("raw_id"),
            tipo_norm,
            record.get("tipo_recebimento"),
            record.get("tipo_reason"),
        )


def backfill_diaria_from_favorecido() -> dict[str, int]:
    init_db()
    conn = get_conn()
    try:
        where_sql = (
            "source_id = ? AND tipo_recebimento = 'BOLSA' AND ("
            "upper(UNACCENT(json_extract(detalhes_json, '$.raw.elemento'))) LIKE '%DIARIA%')"
        )
        count = conn.execute(f"SELECT COUNT(*) FROM records WHERE {where_sql}", ("portal_federal_favorecido",)).fetchone()[0]
        reason = "primary_jsonpath_match:$.raw.elemento=DIARIAS backfill"
        cur = conn.execute(
            f"""
            UPDATE records
            SET tipo_recebimento = 'DIARIA',
                tipo_original = COALESCE(tipo_original, 'BOLSA'),
                tipo_reason = COALESCE(tipo_reason, ?)
            WHERE {where_sql}
            """,
            (reason, "portal_federal_favorecido"),
        )
        conn.commit()
        updated = cur.rowcount if isinstance(cur.rowcount, int) and cur.rowcount >= 0 else 0
        return {"candidates": int(count or 0), "updated": int(updated)}
    finally:
        conn.close()


def refresh_clusters(clusters: Iterable[dict[str, Any]]) -> None:
    conn = get_conn()
    try:
        conn.execute("DELETE FROM person_clusters")
        for cluster in clusters:
            conn.execute(
                """
                INSERT INTO person_clusters (
                    cluster_id, person_name_norm, municipio, orgao, evidence_json, confidence
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    cluster["cluster_id"],
                    cluster["person_name_norm"],
                    cluster.get("municipio"),
                    cluster.get("orgao"),
                    dump_json(cluster.get("evidence")),
                    cluster.get("confidence"),
                ),
            )
        conn.commit()
    finally:
        conn.close()


def list_sources() -> list[dict[str, Any]]:
    conn = get_conn()
    try:
        rows = conn.execute("SELECT * FROM sources ORDER BY source_id").fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def list_distinct_names(limit: int = 5) -> list[str]:
    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT DISTINCT person_name_original
            FROM records
            WHERE person_name_original IS NOT NULL AND person_name_original != ''
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [row[0] for row in rows]
    finally:
        conn.close()


def list_all_records() -> list[dict[str, Any]]:
    conn = get_conn()
    try:
        rows = conn.execute("SELECT * FROM records").fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def search_records(name_norm: str, tipo: str | None, uf: str | None, municipio: str | None) -> list[dict[str, Any]]:
    conn = get_conn()
    try:
        tokens = [token for token in name_norm.split() if token]
        fts_query = " AND ".join(tokens) if tokens else name_norm
        params: list[Any] = []
        filters: list[str] = []

        if tipo and tipo.upper() != "TODOS":
            filters.append("r.tipo_recebimento = ?")
            params.append(tipo.upper())
        if uf:
            filters.append("r.uf = ?")
            params.append(uf.upper())
        if municipio:
            filters.append("r.municipio = ?")
            params.append(municipio)

        filter_sql = " AND ".join(filters)
        if filter_sql:
            filter_sql = " AND " + filter_sql

        query = (
            "SELECT r.*, s.name AS source_name, rf.sha256 AS raw_hash "
            "FROM records r "
            "JOIN sources s ON r.source_id = s.source_id "
            "LEFT JOIN raw_files rf ON r.raw_id = rf.raw_id "
            "WHERE r.record_id IN ("
            "  SELECT rowid FROM records_fts WHERE records_fts MATCH ?"
            ")" + filter_sql
        )
        params = [fts_query] + params
        rows = conn.execute(query, params).fetchall()
        results = [dict(row) for row in rows]
        if results:
            return results

        # Fallback to LIKE for edge cases
        like_query = (
            "SELECT r.*, s.name AS source_name, rf.sha256 AS raw_hash "
            "FROM records r "
            "JOIN sources s ON r.source_id = s.source_id "
            "LEFT JOIN raw_files rf ON r.raw_id = rf.raw_id "
            "WHERE r.person_name_norm LIKE ?" + filter_sql
        )
        like_params = [f"%{name_norm}%"] + params[1:]
        rows = conn.execute(like_query, like_params).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def get_cluster(cluster_id: str) -> dict[str, Any] | None:
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT * FROM person_clusters WHERE cluster_id = ?",
            (cluster_id,),
        ).fetchone()
        if not row:
            return None
        return dict(row)
    finally:
        conn.close()


def list_records_for_cluster(cluster: dict[str, Any]) -> list[dict[str, Any]]:
    conn = get_conn()
    try:
        filters = ["person_name_norm = ?"]
        params: list[Any] = [cluster["person_name_norm"]]
        if cluster.get("municipio"):
            filters.append("municipio = ?")
            params.append(cluster["municipio"])
        if cluster.get("orgao"):
            filters.append("orgao = ?")
            params.append(cluster["orgao"])
        sql = (
            "SELECT r.*, s.name AS source_name, rf.sha256 AS raw_hash "
            "FROM records r "
            "JOIN sources s ON r.source_id = s.source_id "
            "LEFT JOIN raw_files rf ON r.raw_id = rf.raw_id "
            "WHERE " + " AND ".join(filters) + " ORDER BY r.competencia DESC"
        )
        rows = conn.execute(sql, params).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()
