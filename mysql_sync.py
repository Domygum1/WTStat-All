"""Optional, retryable mirror of the local sampler database to MySQL.

SQLite remains the source of truth.  A per-database outbox records changed
sessions in the same transaction as the sampler's existing writes.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path


REPLAY_COLUMNS = ("sid", "start_utc", "map", "mode", "group_name", "last_part",
                  "status", "error", "player_count", "match_br", "processed_at",
                  "team_br_json", "br_ceiling_tick")
MATCH_COLUMNS = ("sid", "start_utc", "map", "mode", "player_count", "last_part",
                 "processed_at", "match_br_min", "match_br_max", "team1_br_min",
                 "team1_br_max", "team1_average_squad_br", "team2_br_min",
                 "team2_br_max", "team2_average_squad_br")
SAMPLE_COLUMNS = ("sid", "uid", "player_name", "country", "country_code",
                  "country_name", "squad_br", "match_br", "mode", "squad_countries",
                  "squad_country_codes", "squad_country_names", "party_key",
                  "configured_vehicles", "stats_json", "team")

CONFIG_NAME = "mysql_sync.local.json"
_state_lock = threading.Lock()
_state = {"enabled": False, "connected": False, "pending": 0, "uploaded": 0,
          "last_error": None}
_stop = threading.Event()
_worker: threading.Thread | None = None


def status() -> dict:
    with _state_lock:
        return dict(_state)


def _set_state(**values) -> None:
    with _state_lock:
        _state.update(values)


def load_config(root: Path) -> dict | None:
    path = root / CONFIG_NAME
    if not path.exists():
        return None
    data = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(data, dict):
        raise ValueError(f"{CONFIG_NAME} 必须是 JSON 对象。")
    host = str(data.get("host") or "").strip()
    if not host:
        return None
    user = str(data.get("user") or "").strip()
    password = data.get("password")
    database = str(data.get("database") or "wtstat_all").strip()
    port = data.get("port", 3306)
    if not user or not isinstance(password, str) or not password:
        raise ValueError(f"{CONFIG_NAME} 已填写 host，还需要 user 和 password。")
    if database != "wtstat_all":
        raise ValueError("目前同步目标数据库只能是 wtstat_all。")
    if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
        raise ValueError(f"{CONFIG_NAME} 的 port 必须是 1–65535 的整数。")
    return {"host": host, "port": port, "database": database,
            "user": user, "password": password}


@contextmanager
def _local_connection(main_db: Path, battle_db: Path):
    connection = sqlite3.connect(main_db, timeout=30)
    try:
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("ATTACH DATABASE ? AS battle", (str(battle_db),))
        yield connection
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def _trigger(connection: sqlite3.Connection, schema: str, table: str, event: str,
             name: str) -> None:
    reference = "OLD" if event == "DELETE" else "NEW"
    connection.execute(
        f"CREATE TRIGGER IF NOT EXISTS {schema}.{name} AFTER {event} ON {table} "
        "BEGIN INSERT INTO mysql_sync_outbox(sid,generation) "
        f"VALUES({reference}.sid,1) ON CONFLICT(sid) DO UPDATE "
        "SET generation=generation+1; END")


def install_outbox(main_db: Path, battle_db: Path, config: dict) -> None:
    """Capture future changes and queue historical rows once per target."""
    target = f"{config['host']}:{config['port']}/{config['database']}:{config['user']}"
    target_key = hashlib.sha256(target.encode("utf-8")).hexdigest()
    with _local_connection(main_db, battle_db) as connection:
        for schema in ("main", "battle"):
            connection.execute(f"CREATE TABLE IF NOT EXISTS {schema}.mysql_sync_outbox ("
                               "sid TEXT PRIMARY KEY, generation INTEGER NOT NULL DEFAULT 1)")
        for table in ("replays", "samples"):
            for event in ("INSERT", "UPDATE", "DELETE"):
                _trigger(connection, "main", table, event,
                         f"mysql_sync_{table}_{event.lower()}")
        for event in ("INSERT", "UPDATE", "DELETE"):
            _trigger(connection, "battle", "matches", event,
                     f"mysql_sync_matches_{event.lower()}")
        previous = connection.execute(
            "SELECT value FROM settings WHERE key='mysql_sync_target_v1'").fetchone()
        if not previous or previous["value"] != target_key:
            connection.execute("INSERT OR IGNORE INTO mysql_sync_outbox(sid) "
                               "SELECT sid FROM replays UNION SELECT sid FROM samples")
            connection.execute("INSERT OR IGNORE INTO battle.mysql_sync_outbox(sid) "
                               "SELECT sid FROM battle.matches")
            connection.execute("INSERT INTO settings(key,value) VALUES('mysql_sync_target_v1',?) "
                               "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (target_key,))


def _pending(connection: sqlite3.Connection, limit: int = 40) -> list[tuple[str, int | None, int | None]]:
    main = connection.execute(
        "SELECT sid,generation FROM mysql_sync_outbox ORDER BY rowid LIMIT ?", (limit,))
    batch = {row["sid"]: [row["generation"], None] for row in main}
    battle = connection.execute(
        "SELECT sid,generation FROM battle.mysql_sync_outbox ORDER BY rowid LIMIT ?", (limit,))
    for row in battle:
        batch.setdefault(row["sid"], [None, None])[1] = row["generation"]
    return [(sid, values[0], values[1]) for sid, values in batch.items()]


def _row(connection: sqlite3.Connection, table: str, columns: tuple[str, ...], sid: str):
    item = connection.execute(
        f"SELECT {','.join(columns)} FROM {table} WHERE sid=?", (sid,)).fetchone()
    return tuple(item) if item else None


def _rows(connection: sqlite3.Connection, table: str, columns: tuple[str, ...], sid: str):
    return [tuple(row) for row in connection.execute(
        f"SELECT {','.join(columns)} FROM {table} WHERE sid=?", (sid,))]


def _replace_remote_batch(cursor, table: str, columns: tuple[str, ...],
                          sids: list[str], rows: list[tuple]) -> None:
    sid_placeholders = ",".join(["%s"] * len(sids))
    cursor.execute(f"DELETE FROM `{table}` WHERE sid IN ({sid_placeholders})", tuple(sids))
    if rows:
        fields = ",".join(f"`{column}`" for column in columns)
        placeholders = ",".join(["%s"] * len(columns))
        cursor.executemany(f"INSERT INTO `{table}` ({fields}) VALUES ({placeholders})", rows)


def sync_batch(main_db: Path, battle_db: Path, remote, limit: int = 40) -> int:
    """Send many sessions per transaction, then acknowledge unchanged generations."""
    with _local_connection(main_db, battle_db) as local:
        batch = _pending(local, limit)
        if not batch:
            return 0
        sids = [sid for sid, _, _ in batch]
        replays = []
        matches = []
        samples = []
        for sid in sids:
            replay = _row(local, "replays", REPLAY_COLUMNS, sid)
            match = _row(local, "battle.matches", MATCH_COLUMNS, sid)
            if replay:
                replays.append(replay)
            if match:
                matches.append(match)
            samples.extend(_rows(local, "samples", SAMPLE_COLUMNS, sid))
    cursor = remote.cursor()
    try:
        _replace_remote_batch(cursor, "samples", SAMPLE_COLUMNS, sids, samples)
        _replace_remote_batch(cursor, "matches", MATCH_COLUMNS, sids, matches)
        _replace_remote_batch(cursor, "replays", REPLAY_COLUMNS, sids, replays)
        remote.commit()
    except Exception:
        remote.rollback()
        raise
    finally:
        cursor.close()
    with _local_connection(main_db, battle_db) as local:
        local.executemany("DELETE FROM mysql_sync_outbox WHERE sid=? AND generation=?",
                          ((sid, generation) for sid, generation, _ in batch
                           if generation is not None))
        local.executemany("DELETE FROM battle.mysql_sync_outbox WHERE sid=? AND generation=?",
                          ((sid, generation) for sid, _, generation in batch
                           if generation is not None))
    return len(batch)


def _pending_count(main_db: Path, battle_db: Path) -> int:
    with _local_connection(main_db, battle_db) as local:
        return sum(local.execute(f"SELECT COUNT(*) FROM {schema}.mysql_sync_outbox").fetchone()[0]
                   for schema in ("main", "battle"))


def _run(main_db: Path, battle_db: Path, config: dict) -> None:
    try:
        import mysql.connector
    except ImportError:
        _set_state(last_error="缺少 MySQL Python 驱动；请重新运行 start.bat 安装可选依赖。")
        return
    while not _stop.is_set():
        remote = None
        try:
            remote = mysql.connector.connect(**config, connection_timeout=6,
                                             autocommit=False)
            _set_state(connected=True, last_error=None)
            while not _stop.is_set():
                count = sync_batch(main_db, battle_db, remote)
                _set_state(uploaded=status()["uploaded"] + count,
                           pending=_pending_count(main_db, battle_db))
                if not count:
                    _stop.wait(5)
        except Exception as error:
            _set_state(connected=False, last_error=f"{type(error).__name__}: {error}")
            _stop.wait(15)
        finally:
            if remote is not None:
                try:
                    remote.close()
                except Exception:
                    pass


def start(root: Path, main_db: Path, battle_db: Path) -> None:
    global _worker
    try:
        config = load_config(root)
        if config is None:
            _set_state(enabled=False, connected=False, pending=0, last_error=None)
            return
        install_outbox(main_db, battle_db, config)
        _set_state(enabled=True, connected=False,
                   pending=_pending_count(main_db, battle_db), last_error=None)
        _stop.clear()
        _worker = threading.Thread(target=_run, args=(main_db, battle_db, config),
                                   name="mysql-sync", daemon=True)
        _worker.start()
    except Exception as error:
        _set_state(enabled=True, connected=False, last_error=f"{type(error).__name__}: {error}")


def stop() -> None:
    _stop.set()
    if _worker is not None:
        _worker.join(timeout=8)
