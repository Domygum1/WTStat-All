"""Canonical two-letter research-tree country codes and English names.

SU is the historical alpha-2 code for the Soviet Union, which remains a
separate War Thunder research tree. It must not be relabeled as Russia.
"""
from __future__ import annotations

import json
import sqlite3


COUNTRY_DATA = {
    "usa": ("US", "United States", "美国"),
    "germany": ("DE", "Germany", "德国"),
    "ussr": ("SU", "Soviet Union", "苏联"),
    "britain": ("GB", "United Kingdom", "英国"),
    "japan": ("JP", "Japan", "日本"),
    "italy": ("IT", "Italy", "意大利"),
    "france": ("FR", "France", "法国"),
    "sweden": ("SE", "Sweden", "瑞典"),
    "china": ("CN", "China", "中国"),
    "israel": ("IL", "Israel", "以色列"),
}

_BY_VALUE = {}
for _key, (_code, _name, _chinese) in COUNTRY_DATA.items():
    for _value in (_key, _code, _name, _chinese):
        _BY_VALUE[_value.casefold()] = (_code, _name)
_BY_VALUE["uk"] = ("GB", "United Kingdom")
_BY_VALUE["united states of america"] = ("US", "United States")


def country_pair(value: object) -> tuple[str | None, str | None]:
    if not isinstance(value, str) or not value.strip():
        return None, None
    return _BY_VALUE.get(value.strip().casefold(), (None, None))


def squad_pairs(value: str | None) -> tuple[str | None, str | None]:
    """Return parallel JSON lists of codes and English names."""
    if value is None:
        return None, None
    try:
        members = json.loads(value)
    except (TypeError, ValueError):
        return None, None
    if not isinstance(members, list):
        return None, None
    pairs = [country_pair(member) for member in members]
    return (json.dumps([pair[0] for pair in pairs]),
            json.dumps([pair[1] for pair in pairs]))


SAMPLE_COUNTRY_COLUMNS = (
    "country_code", "country_name", "squad_country_codes", "squad_country_names")


def migrate_samples(connection: sqlite3.Connection) -> None:
    """Backfill canonical fields once; existing Chinese fields serve the UI."""
    columns = {row["name"] for row in connection.execute("PRAGMA table_info(samples)")}
    for name in SAMPLE_COUNTRY_COLUMNS:
        if name not in columns:
            connection.execute(f"ALTER TABLE samples ADD COLUMN {name} TEXT")
    if connection.execute(
            "SELECT 1 FROM settings WHERE key='country_codes_v1'").fetchone():
        return
    last_rowid = 0
    while rows := connection.execute(
            "SELECT rowid,sid,uid,country,squad_countries FROM samples "
            "WHERE rowid>? ORDER BY rowid LIMIT 1000", (last_rowid,)).fetchall():
        last_rowid = rows[-1]["rowid"]
        updates = []
        for row in rows:
            code, name = country_pair(row["country"])
            squad_codes, squad_names = squad_pairs(row["squad_countries"])
            updates.append((code, name, squad_codes, squad_names, row["rowid"]))
        connection.executemany(
            "UPDATE samples SET country_code=?,country_name=?,"
            "squad_country_codes=?,squad_country_names=? WHERE rowid=?", updates)
    connection.execute("INSERT INTO settings(key,value) VALUES('country_codes_v1','1')")
