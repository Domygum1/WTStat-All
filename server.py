"""Independent, incremental WTStat replay sampler. Run on localhost:8767."""
from __future__ import annotations

import csv
import asyncio
import io
import json
import math
import sqlite3
import threading
import time
import tempfile
import zipfile
from collections import Counter, defaultdict
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel, Field
from starlette.background import BackgroundTask

ROOT = Path(__file__).resolve().parent
import replay_core as wt

if not wt.wiki_catalog():
    raise RuntimeError("缺少本目录 data/wiki_vehicles.csv 或载具表无效。")
if not wt.PARSER.is_file():
    raise RuntimeError("缺少本目录 bin/wtstat_results.exe。")
DB = ROOT / "data" / "wtstat_all.sqlite3"
BATTLE_DB = ROOT / "data" / "battles.sqlite3"
CACHE = ROOT / "cache" / "replays"

app = FastAPI(title="WTStat all")
GROUPS = {"aircraft": "air", "tank": "ground"}
COUNTRIES = list(wt.COUNTRIES.values())
PAGE_SIZE = 30
PENDING_RETRY_BATCH = 15
MAX_PART_BYTES = 100_000_000
MAX_IMPORT_BYTES = 300_000_000
NO_PLAYERS = "结算段未包含玩家数据"
NO_PARTICIPANTS = "结算段没有参战玩家"
PLAYER_STAT_FIELDS = ("kills", "groundKills", "humanKills", "deaths", "assists", "score",
                      "aiKills", "aiGroundKills", "aiNavalKills", "captureZone", "teamKills")
VALID_SAMPLE_SQL = ("uid <> '' AND uid NOT GLOB '*[^0-9]*' AND CAST(uid AS INTEGER)>0 "
                    "AND (player_name IS NULL OR LOWER(player_name) NOT LIKE 'coop/bot%')")
guard = threading.RLock()
stop_event = threading.Event()
active_sids: set[str] = set()
worker_thread: threading.Thread | None = None
runtime = {"running": False, "stopping": False, "page": 1, "official_total": None,
           "processed": 0, "errors": 0, "pending": 0, "samples": 0, "revision": 0,
           "last_error": None, "active": 0, "scanned_pages": 0}


@contextmanager
def db():
    connection = sqlite3.connect(DB, timeout=30, check_same_thread=False)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout=30000")
    connection.execute("ATTACH DATABASE ? AS battle", (str(BATTLE_DB),))
    try:
        yield connection
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def setting(key: str, default: int) -> int:
    with db() as connection:
        row = connection.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return int(row["value"]) if row else default


def save_setting(key: str, value: int | str) -> None:
    with db() as connection:
        connection.execute("INSERT INTO settings(key,value) VALUES(?,?) "
                           "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))


def capture_mode_setting() -> str:
    with db() as connection:
        row = connection.execute("SELECT value FROM settings WHERE key='capture_mode'").fetchone()
    return row["value"] if row and row["value"] in ("all", "air", "ground") else "all"


def init_db() -> None:
    DB.parent.mkdir(parents=True, exist_ok=True)
    CACHE.mkdir(parents=True, exist_ok=True)
    for partial in CACHE.glob("*/*.part"):
        try:
            partial.unlink(missing_ok=True)
        except OSError:
            pass  # A running sampler may still be writing this part.
    with db() as connection:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA battle.journal_mode=WAL")
        connection.execute("CREATE TABLE IF NOT EXISTS battle.matches ("
                           "sid TEXT PRIMARY KEY, start_utc TEXT, map TEXT, mode TEXT, "
                           "player_count INTEGER, last_part INTEGER, processed_at REAL, "
                           "match_br_min REAL, match_br_max REAL, "
                           "team1_br_min REAL, team1_br_max REAL, team1_average_squad_br REAL, "
                           "team2_br_min REAL, team2_br_max REAL, team2_average_squad_br REAL)")
        connection.execute("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        connection.execute("CREATE TABLE IF NOT EXISTS replays ("
                           "sid TEXT PRIMARY KEY, start_utc TEXT, map TEXT, mode TEXT, group_name TEXT, "
                           "last_part INTEGER, status TEXT NOT NULL, error TEXT, player_count INTEGER, "
                           "match_br REAL, processed_at REAL NOT NULL, team_br_json TEXT)")
        connection.execute("CREATE TABLE IF NOT EXISTS lineup_repair_attempts ("
                           "sid TEXT PRIMARY KEY, checked_at REAL NOT NULL, "
                           "retryable INTEGER NOT NULL DEFAULT 0)")
        attempt_columns = {row["name"] for row in connection.execute(
            "PRAGMA table_info(lineup_repair_attempts)")}
        if "retryable" not in attempt_columns:
            connection.execute("ALTER TABLE lineup_repair_attempts "
                               "ADD COLUMN retryable INTEGER NOT NULL DEFAULT 0")
        if not connection.execute(
                "SELECT 1 FROM settings WHERE key='team_interval_repair_v2'").fetchone():
            connection.execute("DELETE FROM lineup_repair_attempts")
            connection.execute("INSERT INTO settings VALUES('team_interval_repair_v2','1')")
        if not connection.execute(
                "SELECT 1 FROM settings WHERE key='team_interval_repair_v3'").fetchone():
            connection.execute(
                "DELETE FROM lineup_repair_attempts WHERE sid IN ("
                "SELECT sid FROM battle.matches WHERE match_br_min IS NULL "
                "OR match_br_max IS NULL)")
            connection.execute("INSERT INTO settings VALUES('team_interval_repair_v3','1')")
        connection.execute("CREATE TABLE IF NOT EXISTS official_replays ("
                           "sid TEXT PRIMARY KEY, start_utc TEXT, group_name TEXT NOT NULL)")
        connection.execute("CREATE INDEX IF NOT EXISTS official_replays_group "
                           "ON official_replays(group_name)")
        replay_columns = {row["name"] for row in connection.execute("PRAGMA table_info(replays)")}
        if "team_br_json" not in replay_columns:
            connection.execute("ALTER TABLE replays ADD COLUMN team_br_json TEXT")
        connection.execute("CREATE TABLE IF NOT EXISTS samples ("
                           "sid TEXT NOT NULL, uid TEXT NOT NULL, player_name TEXT, country TEXT, "
                           "squad_br REAL, match_br REAL, mode TEXT NOT NULL, "
                           "squad_countries TEXT, party_key TEXT, "
                           "PRIMARY KEY(sid,uid))")
        columns = {row["name"] for row in connection.execute("PRAGMA table_info(samples)")}
        if "squad_countries" not in columns:
            connection.execute("ALTER TABLE samples ADD COLUMN squad_countries TEXT")
        if "party_key" not in columns:
            connection.execute("ALTER TABLE samples ADD COLUMN party_key TEXT")
        if "configured_vehicles" not in columns:
            connection.execute("ALTER TABLE samples ADD COLUMN configured_vehicles TEXT")
        if "stats_json" not in columns:
            connection.execute("ALTER TABLE samples ADD COLUMN stats_json TEXT")
        if "team" not in columns:
            connection.execute("ALTER TABLE samples ADD COLUMN team INTEGER")
        connection.execute("CREATE INDEX IF NOT EXISTS samples_filter ON samples(mode,squad_br,country)")
        connection.execute("CREATE INDEX IF NOT EXISTS samples_uid ON samples(uid)")
        connection.execute("CREATE TABLE IF NOT EXISTS histogram ("
                           "mode TEXT NOT NULL, country TEXT NOT NULL, squad_tick INTEGER NOT NULL, "
                           "match_tick INTEGER NOT NULL, count INTEGER NOT NULL, "
                           "PRIMARY KEY(mode,country,squad_tick,match_tick))")
        for key, value in (("page", 1), ("concurrency", 2), ("keep_files", 100),
                           ("capture_mode", "all")):
            connection.execute("INSERT OR IGNORE INTO settings VALUES(?,?)", (key, str(value)))
        if not connection.execute(
                "SELECT 1 FROM settings WHERE key='official_index_initialized'").fetchone():
            # Earlier versions did not retain list-only results. Revisit page one
            # once to index both modes; completed replays will not be downloaded again.
            connection.execute("UPDATE settings SET value='1' WHERE key='page'")
            connection.execute("INSERT INTO settings VALUES('official_index_initialized','1')")
            connection.execute("INSERT OR REPLACE INTO settings VALUES('official_index_complete','0')")
        if not connection.execute(
                "SELECT 1 FROM settings WHERE key='capture_modes_seen'").fetchone():
            modes = {row[0] for row in connection.execute(
                "SELECT DISTINCT group_name FROM replays WHERE status='done' "
                "AND group_name IN ('air','ground')")}
            connection.execute("INSERT INTO settings VALUES('capture_modes_seen',?)",
                               (",".join(sorted(modes)),))
        if not connection.execute("SELECT 1 FROM settings WHERE key='keep_files_default_100'").fetchone():
            connection.execute("UPDATE settings SET value='100' WHERE key='keep_files' AND value='10'")
            connection.execute("INSERT INTO settings VALUES('keep_files_default_100','1')")
        connection.execute("UPDATE replays SET status='pending' WHERE status='error' "
                           "AND error IN ('结算段 HTTP 404', '结算段未包含玩家数据')")
        runtime["processed"] = connection.execute(
            "SELECT COUNT(*) FROM replays WHERE status='done'").fetchone()[0]
        runtime["errors"] = connection.execute(
            "SELECT COUNT(*) FROM replays WHERE status='error'").fetchone()[0]
        runtime["pending"] = connection.execute(
            "SELECT COUNT(*) FROM replays WHERE status='pending'").fetchone()[0]
        runtime["samples"] = connection.execute("SELECT COUNT(*) FROM samples").fetchone()[0]
    runtime["page"] = setting("page", 1)


def rating_tick(value, mode: str) -> int | None:
    if not isinstance(value, (int, float)) or not math.isfinite(value):
        return None
    tick = round(value * 10)
    limit = 147 if mode == "air" else 130
    if abs(value * 10 - tick) > 0.001 or not 10 <= tick <= limit or tick % 10 not in (0, 3, 7):
        return None
    return tick


def is_real_player(uid: object, name: object) -> bool:
    uid = str(uid)
    return (uid.isascii() and uid.isdecimal() and int(uid) > 0
            and not (isinstance(name, str) and name.lower().startswith("coop/bot")))


def rebuild_histogram(connection: sqlite3.Connection) -> None:
    totals: Counter[tuple[str, str, int, int]] = Counter()
    for row in connection.execute(
        f"SELECT mode,country,squad_br,match_br,COUNT(*) AS n FROM samples "
        f"WHERE {VALID_SAMPLE_SQL} GROUP BY mode,country,squad_br,match_br"
    ):
        squad_tick = rating_tick(row["squad_br"], row["mode"])
        match_tick = rating_tick(row["match_br"], row["mode"])
        if squad_tick is not None and match_tick is not None:
            totals[(row["mode"], row["country"] or "", squad_tick, match_tick)] += row["n"]
    connection.execute("DELETE FROM histogram")
    connection.executemany("INSERT INTO histogram VALUES(?,?,?,?,?)",
                           ((*key, count) for key, count in totals.items()))


def remove_saved_bots() -> int:
    """Remove legacy bot samples and rebuild their derived histogram atomically."""
    with db() as connection:
        excluded = connection.execute(
            f"SELECT COUNT(*) FROM samples WHERE NOT ({VALID_SAMPLE_SQL})").fetchone()[0]
        if excluded:
            connection.execute(f"DELETE FROM samples WHERE NOT ({VALID_SAMPLE_SQL})")
            rebuild_histogram(connection)
        remaining = connection.execute("SELECT COUNT(*) FROM samples").fetchone()[0]
    runtime["samples"] = remaining
    if excluded:
        runtime["revision"] += 1
    return excluded


init_db()
remove_saved_bots()


def remove_unjoined_samples() -> int:
    """Discard legacy team-0 roster entries that have no lineup or activity."""
    with db() as connection:
        rows = connection.execute(
            "SELECT sid,uid,configured_vehicles,stats_json FROM samples WHERE team=0"
        ).fetchall()
        unwanted = []
        for row in rows:
            try:
                configured = json.loads(row["configured_vehicles"]) if row["configured_vehicles"] else []
                stats = json.loads(row["stats_json"]) if row["stats_json"] else {}
            except (TypeError, ValueError):
                continue
            if isinstance(configured, list) and isinstance(stats, dict) and not configured \
                    and not wt.has_activity(stats):
                unwanted.append((row["sid"], row["uid"]))
        if unwanted:
            connection.executemany("DELETE FROM samples WHERE sid=? AND uid=?", unwanted)
            rebuild_histogram(connection)
        remaining = connection.execute("SELECT COUNT(*) FROM samples").fetchone()[0]
    runtime["samples"] = remaining
    if unwanted:
        runtime["revision"] += 1
    return len(unwanted)


remove_unjoined_samples()


def possible_ticks(mode: str) -> list[int]:
    limit = 147 if mode == "air" else 130
    return [tick for tick in range(10, limit + 1) if tick % 10 in (0, 3, 7)]


def fill_bot_team_br(teams: dict[str, dict | None], occupied_teams: set[int],
                     match_br: float | None, mode: str) -> None:
    """Use the match BR as a proxy when the opposing team has no participating humans."""
    if rating_tick(match_br, mode) is None:
        return
    known = [team for team in (1, 2) if teams[str(team)] is not None]
    if len(known) != 1:
        return
    missing = 3 - known[0]
    if missing not in occupied_teams:
        teams[str(missing)] = {"min": match_br, "max": match_br,
                               "average_squad_br": match_br}


def team_br_summary(players: list[dict], mode: str,
                    match_br: float | None = None) -> dict[str, dict | None]:
    """Summarize human teams and infer a bot-only team's BR from the match."""
    players = [player for player in players if wt.has_participation_evidence(player)]
    result: dict[str, dict | None] = {"1": None, "2": None}
    if any(player.get("team") not in (1, 2) or
           rating_tick(player.get("squad_br"), mode) is None for player in players):
        return result
    for team in (1, 2):
        members = [player for player in players if player.get("team") == team]
        if not members:
            continue
        ticks = [rating_tick(player.get("squad_br"), mode) for player in members]
        if any(tick is None for tick in ticks):
            continue
        parties: dict[str, int] = {}
        complete_parties = True
        for player, tick in zip(members, ticks):
            key = party_key(player)
            if not key or (key in parties and parties[key] != tick):
                complete_parties = False
                break
            parties[key] = tick
        result[str(team)] = {
            "min": min(ticks) / 10,
            "max": max(ticks) / 10,
            "average_squad_br": round(sum(parties.values()) / len(parties) / 10, 3)
            if complete_parties and parties else None,
        }
    fill_bot_team_br(result, {player.get("team") for player in players}, match_br, mode)
    return result


def rated_real_players(data: dict) -> tuple[list[dict], float | None]:
    """Ignore system bots before resolving lineups, squads, and match BR."""
    players = [player for player in data.get("players") or []
               if is_real_player(player.get("uid"), player.get("name"))
               and not wt.is_unjoined_player(player)]
    rated = {**data, "players": players}
    wt.add_battle_ratings(rated)
    return players, rated.get("match_br")


def decoded_team_br(value: str | None) -> tuple[dict, list[float] | None]:
    try:
        teams = json.loads(value) if value else {}
    except (TypeError, ValueError):
        teams = {}
    if not isinstance(teams, dict):
        teams = {}
    sides = [teams.get(str(team)) for team in (1, 2)]
    available = [side for side in sides if isinstance(side, dict)
                 and isinstance(side.get("min"), (int, float))
                 and isinstance(side.get("max"), (int, float))]
    if available:
        return teams, [min(side["min"] for side in available),
                       max(side["max"] for side in available)]
    return teams, None


BATTLE_COLUMNS = ("sid", "start_utc", "map", "mode", "player_count", "last_part",
                  "processed_at", "match_br_min", "match_br_max", "team1_br_min",
                  "team1_br_max", "team1_average_squad_br", "team2_br_min",
                  "team2_br_max", "team2_average_squad_br")


def battle_values(sid: str, start: str | None, replay_map: str | None, mode: str,
                  player_count: int | None, last_part: int | None,
                  processed_at: float | None, match_br: float | None,
                  team_br_json: str | None) -> tuple:
    teams, interval = decoded_team_br(team_br_json)
    sides = [teams.get(str(team)) for team in (1, 2)]
    def field(side, key):
        return side.get(key) if isinstance(side, dict) else None
    return (sid, start, replay_map, mode, player_count, last_part, processed_at,
            interval[0] if interval and match_br is not None else None, match_br,
            *(field(side, key) for side in sides
              for key in ("min", "max", "average_squad_br")))


def save_battle(connection: sqlite3.Connection, values: tuple, *, only_missing: bool = False) -> None:
    columns = ",".join(BATTLE_COLUMNS)
    placeholders = ",".join("?" for _ in BATTLE_COLUMNS)
    if only_missing:
        connection.execute(f"INSERT OR IGNORE INTO battle.matches({columns}) VALUES({placeholders})", values)
    else:
        updates = ",".join(f"{name}=excluded.{name}" for name in BATTLE_COLUMNS[1:])
        connection.execute(f"INSERT INTO battle.matches({columns}) VALUES({placeholders}) "
                           f"ON CONFLICT(sid) DO UPDATE SET {updates}", values)


def saved_lineup_br(configured_json: str | None, mode: str) -> float | None:
    """Recalculate one saved human lineup from vehicle codes, not old squad BR."""
    try:
        vehicles = json.loads(configured_json) if configured_json else None
    except (TypeError, ValueError):
        return None
    if not isinstance(vehicles, list) or not vehicles:
        return None
    group = {"air": "aircraft", "ground": "tank"}.get(mode)
    if group is None:
        return None
    ratings = [wt.vehicle_battle_rating(vehicle.get("code"), "realistic", group)
               if isinstance(vehicle, dict) else None for vehicle in vehicles]
    if any(rating_tick(br, mode) is None for br in ratings):
        return None
    return max(ratings)


def saved_participation_evidence(row: sqlite3.Row) -> bool:
    if row["configured_vehicles"]:
        try:
            if json.loads(row["configured_vehicles"]):
                return True
        except (TypeError, ValueError):
            return True  # Unknown lineup must not be silently excluded from BR.
    if row["stats_json"]:
        try:
            stats = json.loads(row["stats_json"])
        except (TypeError, ValueError):
            return True
        return not isinstance(stats, dict) or wt.has_activity(stats)
    return False


def repair_missing_battle_ratings() -> dict[str, int]:
    """Backfill BR from participating humans, ignoring unspawned roster entries."""
    repaired = unresolved = 0
    with db() as connection:
        battles = connection.execute(
            "SELECT b.sid,b.mode FROM battle.matches b JOIN replays r ON r.sid=b.sid "
            "WHERE (b.match_br_max IS NULL OR b.match_br_min IS NULL) "
            "AND r.status='done'").fetchall()
        for battle in battles:
            sid, mode = battle["sid"], battle["mode"]
            rows = connection.execute(
                f"SELECT uid,team,party_key,squad_br,configured_vehicles,stats_json FROM samples "
                f"WHERE sid=? AND {VALID_SAMPLE_SQL}", (sid,)).fetchall()
            rated_rows = [row for row in rows if saved_participation_evidence(row)]
            lineups = [saved_lineup_br(row["configured_vehicles"], mode) for row in rated_rows]
            if not lineups or any(br is None for br in lineups):
                unresolved += 1
                continue
            match_br = max(lineups)
            party_brs: dict[tuple[int | None, str], float] = {}
            for row, br in zip(rated_rows, lineups):
                if row["party_key"]:
                    key = (row["team"], row["party_key"])
                    party_brs[key] = max(party_brs.get(key, br), br)
            squad_brs = [party_brs[(row["team"], row["party_key"])]
                         if row["party_key"] else row["squad_br"] for row in rated_rows]
            squad_by_uid = {row["uid"]: br for row, br in zip(rated_rows, squad_brs)}
            teams: dict[str, dict | None] = {"1": None, "2": None}
            if all(row["team"] in (1, 2) for row in rated_rows) and all(
                    rating_tick(br, mode) is not None for br in squad_brs):
                for team in (1, 2):
                    members = [(row, br) for row, br in zip(rated_rows, squad_brs)
                               if row["team"] == team]
                    if not members or any(rating_tick(br, mode) is None for _, br in members):
                        continue
                    ticks = [rating_tick(br, mode) for _, br in members]
                    parties = {(row["team"], row["party_key"]): rating_tick(br, mode)
                               for row, br in members if row["party_key"]}
                    teams[str(team)] = {
                        "min": min(ticks) / 10, "max": max(ticks) / 10,
                        "average_squad_br": round(sum(parties.values()) / len(parties) / 10, 3)
                        if len(parties) and all(row["party_key"] for row, _ in members) else None,
                    }
            fill_bot_team_br(teams, {row["team"] for row in rated_rows}, match_br, mode)
            sides = [teams[str(team)] for team in (1, 2)]
            lower = min((side["min"] for side in sides if side is not None), default=None)
            connection.execute(
                "UPDATE battle.matches SET match_br_min=?,match_br_max=?,"
                "team1_br_min=?,team1_br_max=?,team1_average_squad_br=?,"
                "team2_br_min=?,team2_br_max=?,team2_average_squad_br=? WHERE sid=?",
                (lower, match_br,
                 *(side.get(field) if side else None for side in sides
                   for field in ("min", "max", "average_squad_br")), sid))
            connection.execute("UPDATE replays SET match_br=?,team_br_json=? WHERE sid=?",
                               (match_br, json.dumps(teams), sid))
            connection.executemany(
                "UPDATE samples SET squad_br=?,match_br=? WHERE sid=? AND uid=?",
                ((squad_by_uid.get(row["uid"]), match_br, sid, row["uid"])
                 for row in rows))
            repaired += 1
        if repaired:
            rebuild_histogram(connection)
    if repaired:
        runtime["revision"] += 1
    return {"repaired": repaired, "unresolved": unresolved}


repair_missing_battle_ratings()


def fill_saved_bot_team_brs() -> int:
    """Fill old bot-only sides when saved human samples confirm the empty team."""
    filled = 0
    with db() as connection:
        rows = connection.execute(
            "SELECT sid,mode,match_br_max,team1_br_min,team1_br_max,"
            "team2_br_min,team2_br_max FROM battle.matches "
            "WHERE match_br_max IS NOT NULL AND "
            "((team1_br_min IS NULL AND team1_br_max IS NULL "
            "AND team2_br_min IS NOT NULL AND team2_br_max IS NOT NULL) OR "
            "(team2_br_min IS NULL AND team2_br_max IS NULL "
            "AND team1_br_min IS NOT NULL AND team1_br_max IS NOT NULL))"
        ).fetchall()
        for row in rows:
            if rating_tick(row["match_br_max"], row["mode"]) is None:
                continue
            occupied = {sample[0] for sample in connection.execute(
                f"SELECT DISTINCT team FROM samples WHERE sid=? AND {VALID_SAMPLE_SQL} "
                "AND team IN (1,2)", (row["sid"],))}
            missing = 1 if row["team1_br_min"] is None else 2
            if occupied != {3 - missing}:
                continue
            prefix = f"team{missing}"
            connection.execute(
                f"UPDATE battle.matches SET {prefix}_br_min=?,{prefix}_br_max=?,"
                f"{prefix}_average_squad_br=? WHERE sid=?",
                (row["match_br_max"],) * 3 + (row["sid"],))
            filled += 1
    if filled:
        runtime["revision"] += 1
    return filled


fill_saved_bot_team_brs()


def battle_api_row(row: sqlite3.Row) -> dict:
    item = dict(row)
    item["match_br"] = item["match_br_max"]
    low, high = item["match_br_min"], item["match_br_max"]
    item["match_br_range"] = [low, high] if low is not None and high is not None else None
    item["team_br"] = {
        str(team): ({"min": item[f"team{team}_br_min"],
                     "max": item[f"team{team}_br_max"],
                     "average_squad_br": item[f"team{team}_average_squad_br"]}
                    if item[f"team{team}_br_min"] is not None
                    and item[f"team{team}_br_max"] is not None else None)
        for team in (1, 2)}
    return item


def fetch_page(page: int) -> dict:
    payload = {"gameMode": ["realistic"], "gameType": "randomBattle", "techType": "all",
               "findMissionValue": "", "findUserValue": "", "findUserType": "USERNAME",
               "isUserOwnReplays": False, "rankRange": "", "timeRangeFrom": "",
               "timeRangeTo": "", "limit": PAGE_SIZE, "page": page}
    response = wt.request("https://warthunder.com/en/api/replay", auth=True,
                          method="POST", payload=payload)
    try:
        if response.status_code != 200:
            raise HTTPException(response.status_code, f"官网对局列表 HTTP {response.status_code}")
        if "json" not in response.headers.get("Content-Type", ""):
            raise HTTPException(401, "官网没有返回 JSON；请检查 identity_sid。")
        result = response.json()
        if not isinstance(result.get("items"), list) or not isinstance(result.get("total_count"), int):
            raise HTTPException(502, "官网对局列表格式已变化。")
        if result["total_count"] == 0:
            raise HTTPException(401, "官网回放列表为空；identity_sid 可能已过期，请重新登录。")
        return result
    finally:
        response.close()


def download_last_part(sid: str, part: int) -> Path:
    if not 0 <= part < wt.MAX_REPLAY_PARTS:
        raise HTTPException(422, f"结算段编号异常：{part}")
    folder = CACHE / sid
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{part:04d}.wrpl"
    if path.exists() and path.stat().st_size > 1234:
        path.touch()
        return path
    response = wt.request(f"{wt.FILES}/{sid}/{part:04d}.wrpl", stream=True)
    try:
        if response.status_code in (403, 404, 429):
            raise HTTPException(response.status_code, f"结算段 HTTP {response.status_code}")
        if response.status_code != 200:
            raise HTTPException(502, f"结算段 HTTP {response.status_code}")
        if int(response.headers.get("Content-Length", "0")) > MAX_PART_BYTES:
            raise HTTPException(502, "结算段超过 100 MB")
        temp = path.with_suffix(".part")
        size = 0
        try:
            with temp.open("wb") as target:
                for chunk in response.iter_content(131072):
                    size += len(chunk)
                    if size > MAX_PART_BYTES:
                        raise HTTPException(502, "结算段超过 100 MB")
                    target.write(chunk)
            if size < 1234:
                raise HTTPException(502, "结算段不完整")
            temp.replace(path)
        except Exception:
            temp.unlink(missing_ok=True)
            raise
        return path
    finally:
        response.close()


def missing_active_lineups(players: list[dict]) -> bool:
    return any(is_real_player(player.get("uid"), player.get("name"))
               and wt.has_activity(player) and not player.get("configured")
               for player in players)


def parse_with_lineup_fallback(sid: str, last_part: int, final_path: Path) -> dict:
    """Read the final scoreboard, then 0001, then every segment if needed."""
    data = wt.parse_replay(sid, [final_path], include_log=False, persist=False)
    incomplete = False
    if missing_active_lineups(data.get("players") or []) and last_part > 1:
        try:
            early_path = download_last_part(sid, 1)
            data = wt.parse_replay(sid, [early_path, final_path],
                                   include_log=False, persist=False)
        except HTTPException as error:
            if error.status_code == 429:
                raise
            incomplete = True
    if missing_active_lineups(data.get("players") or []):
        parts = []
        for part in range(last_part + 1):
            if part == last_part:
                parts.append(final_path)
                continue
            try:
                parts.append(download_last_part(sid, part))
            except HTTPException as error:
                if error.status_code == 429:
                    raise
                incomplete = True
                # A missing intermediate segment does not hide later snapshots.
        if len(parts) > 1:
            data = wt.parse_replay(sid, parts, include_log=False, persist=False)
    data["_lineup_download_incomplete"] = incomplete
    return data


def latest_published_part(sid: str, first: int) -> int | None:
    """Probe at most eight part headers; download only the newest available candidate."""
    if not 0 <= first < wt.MAX_REPLAY_PARTS:
        return None
    probes = 0

    def exists(part: int) -> bool:
        nonlocal probes
        probes += 1
        response = wt.request(f"{wt.FILES}/{sid}/{part:04d}.wrpl", stream=True)
        try:
            if response.status_code == 200:
                return True
            if response.status_code == 404:
                return False
            raise HTTPException(response.status_code if response.status_code in (403, 429) else 502,
                                f"结算段 HTTP {response.status_code}")
        finally:
            response.close()

    if not exists(first):
        return None
    newest, step, upper = first, 1, None
    while probes < 8 and newest + step < wt.MAX_REPLAY_PARTS:
        candidate = newest + step
        if exists(candidate):
            newest, step = candidate, step * 2
        else:
            upper = candidate
            break
    while upper is not None and upper - newest > 1 and probes < 8:
        candidate = (newest + upper) // 2
        if exists(candidate):
            newest = candidate
        else:
            upper = candidate
    return newest


def prune_files() -> None:
    keep = setting("keep_files", 100)
    with guard:
        sessions: dict[str, list[Path]] = defaultdict(list)
        for path in CACHE.glob("*/*.wrpl"):
            sessions[path.parent.name].append(path)
        with db() as connection:
            start_times = {row["sid"]: row["start_utc"] or "" for row in connection.execute(
                "SELECT sid,start_utc FROM replays")}
        ordered = sorted(sessions, key=lambda sid: (start_times.get(sid, ""),
                                                     max(path.stat().st_mtime for path in sessions[sid])))
        retained = set(ordered[-keep:]) if keep else set()
        for sid in ordered:
            if sid in retained or sid in active_sids:
                continue
            for path in sessions[sid]:
                path.unlink(missing_ok=True)
            try:
                (CACHE / sid).rmdir()
            except OSError:
                pass


def record_error(sid: str, item: dict, error: str, *, pending: bool = False) -> None:
    new_status = "pending" if pending else "error"
    with db() as connection:
        existing = connection.execute("SELECT status FROM replays WHERE sid=?", (sid,)).fetchone()
        if existing and existing["status"] == "done":
            return
        connection.execute("INSERT INTO replays(sid,start_utc,map,mode,group_name,last_part,status,"
                           "error,player_count,match_br,processed_at) VALUES(?,?,?,?,?,?,?,?,NULL,NULL,?) "
                           "ON CONFLICT(sid) DO UPDATE SET status=excluded.status,error=excluded.error,"
                           "last_part=excluded.last_part,processed_at=excluded.processed_at",
                           (sid, start_time(item), item.get("missionName"), "realistic",
                            GROUPS.get(item.get("statisticGroup")), item.get("partsCount"),
                            new_status, error, time.time()))
    with guard:
        old_status = existing["status"] if existing else None
        if new_status == "error" and old_status != "error":
            runtime["errors"] += 1
        elif new_status == "pending" and old_status == "error":
            runtime["errors"] = max(0, runtime["errors"] - 1)
        if new_status == "pending" and old_status != "pending":
            runtime["pending"] += 1
        elif new_status == "error" and old_status == "pending":
            runtime["pending"] = max(0, runtime["pending"] - 1)
        if not pending:
            runtime["last_error"] = f"{sid}: {error}"


def record_no_participants(sid: str, item: dict, player_count: int,
                           previous_status: str | None) -> None:
    """Remember an unplayed replay without adding it to battle or player statistics."""
    with db() as connection:
        connection.execute(
            "INSERT INTO replays(sid,start_utc,map,mode,group_name,last_part,status,"
            "error,player_count,match_br,processed_at) VALUES(?,?,?,?,?,?,'skipped',?,?,NULL,?) "
            "ON CONFLICT(sid) DO UPDATE SET status='skipped',error=excluded.error,"
            "player_count=excluded.player_count,last_part=excluded.last_part,"
            "processed_at=excluded.processed_at",
            (sid, start_time(item), item.get("missionName"), "realistic",
             GROUPS.get(item.get("statisticGroup")), item.get("partsCount"),
             NO_PARTICIPANTS, player_count, time.time()))
    with guard:
        if previous_status == "error":
            runtime["errors"] = max(0, runtime["errors"] - 1)
        elif previous_status == "pending":
            runtime["pending"] = max(0, runtime["pending"] - 1)
        runtime["revision"] += 1


def start_time(item: dict) -> str | None:
    timestamp = item.get("startTime")
    return datetime.fromtimestamp(timestamp, timezone.utc).isoformat() if isinstance(timestamp, (int, float)) else None


def player_country(player: dict) -> str | None:
    configured = player.get("configured") or []
    countries = {vehicle.get("country") for vehicle in configured}
    return next(iter(countries)) if configured and len(countries) == 1 else None


def squad_countries(players: list[dict]) -> dict[str, str | None]:
    """JSON country list of actual (non-auto) squadmates; NULL means unknown."""
    result = {}
    for player in players:
        uid = str(player["uid"])
        if player.get("auto_squad") is True:
            members = []
        elif player.get("auto_squad") is False and player.get("team") is not None and player.get("squad_id") is not None:
            members = [other for other in players if str(other.get("uid")) != uid
                       and other.get("team") == player["team"]
                       and other.get("squad_id") == player["squad_id"]
                       and other.get("auto_squad") is False]
        else:
            result[uid] = None
            continue
        result[uid] = json.dumps([player_country(member) for member in sorted(
            members, key=lambda member: str(member.get("uid")))], ensure_ascii=False)
    return result


def party_key(player: dict) -> str | None:
    """Identify one premade squad or one solo player within a session."""
    if player.get("auto_squad") is True:
        return f"solo:{player['uid']}"
    if player.get("auto_squad") is False:
        if player.get("team") is not None and player.get("squad_id") is not None:
            return f"squad:{player['team']}:{player['squad_id']}"
    return None


def process_replay(item: dict) -> None:
    sid = str(item.get("sessionIdHex") or "").lower()
    if len(sid) != 16 or not all(char in "0123456789abcdef" for char in sid):
        return
    mode = GROUPS.get(item.get("statisticGroup"))
    if item.get("gameMode") != "realistic" or mode is None or item.get("isCompleted") is False:
        return
    with db() as connection:
        previous = connection.execute(
            "SELECT status,processed_at,error,last_part FROM replays WHERE sid=?", (sid,)).fetchone()
    if previous and (previous["status"] in ("done", "skipped") or
                     time.time() - previous["processed_at"] < (900 if previous["status"] == "pending" else 3600)):
        return
    with guard:
        active_sids.add(sid)
        runtime["active"] = len(active_sids)
    try:
        last_part = item.get("partsCount")
        if not isinstance(last_part, int):
            raise HTTPException(422, "官网未提供结算段编号")
        if previous and previous["error"] == NO_PLAYERS and isinstance(previous["last_part"], int):
            first = max(last_part, previous["last_part"] + 1)
            newer = latest_published_part(sid, first)
            if newer is None:
                record_error(sid, {**item, "partsCount": previous["last_part"]}, NO_PLAYERS, pending=True)
                return
            last_part = newer
        item = {**item, "partsCount": last_part}
        try:
            path = download_last_part(sid, last_part)
        except HTTPException as error:
            # Older listings may express partsCount as a count rather than final index.
            if error.status_code != 404 or last_part == 0:
                raise
            path = download_last_part(sid, last_part - 1)
            last_part -= 1
            item["partsCount"] = last_part
        data = parse_with_lineup_fallback(sid, last_part, path)
        if not data.get("players"):
            raise HTTPException(502, NO_PLAYERS)
        data["game_mode"] = "realistic"
        data["battle_group"] = item["statisticGroup"]
        real_players, match_br = rated_real_players(data)
        if not any(wt.has_participation_evidence(player) for player in real_players):
            record_no_participants(sid, item, len(data["players"]),
                                   previous["status"] if previous else None)
            return
        added_samples = 0
        with db() as connection:
            team_summary = json.dumps(team_br_summary(real_players, mode, match_br))
            teammate_countries = squad_countries(real_players)
            for player in real_players:
                country = player_country(player)
                squad_br = player.get("squad_br")
                cursor = connection.execute(
                    "INSERT OR IGNORE INTO samples "
                    "(sid,uid,player_name,country,squad_br,match_br,mode,squad_countries,party_key,"
                    "configured_vehicles,stats_json,team) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                    (sid, str(player["uid"]), player.get("name"), country,
                     squad_br, match_br, mode, teammate_countries[str(player["uid"])], party_key(player),
                     json.dumps(player.get("configured"), ensure_ascii=False) if player.get("configured") else None,
                     json.dumps({key: player.get(key) for key in PLAYER_STAT_FIELDS}, ensure_ascii=False),
                     player.get("team")))
                if cursor.rowcount:
                    added_samples += 1
                    squad_tick = rating_tick(squad_br, mode)
                    match_tick = rating_tick(match_br, mode)
                    if squad_tick is not None and match_tick is not None:
                        connection.execute(
                            "INSERT INTO histogram VALUES(?,?,?,?,1) "
                            "ON CONFLICT(mode,country,squad_tick,match_tick) "
                            "DO UPDATE SET count=count+1",
                            (mode, country or "", squad_tick, match_tick))
            processed_at = time.time()
            replay_start = start_time(item)
            connection.execute(
                "INSERT INTO replays(sid,start_utc,map,mode,group_name,last_part,status,error,"
                "player_count,match_br,processed_at,team_br_json) "
                "VALUES(?,?,?,?,?,?,'done',NULL,?,?,?,?) "
                "ON CONFLICT(sid) DO UPDATE SET start_utc=excluded.start_utc,map=excluded.map,"
                "mode=excluded.mode,group_name=excluded.group_name,last_part=excluded.last_part,"
                "status='done',error=NULL,player_count=excluded.player_count,"
                "match_br=excluded.match_br,processed_at=excluded.processed_at,"
                "team_br_json=excluded.team_br_json",
                (sid, replay_start, item.get("missionName"), "realistic", mode,
                 last_part, len(data["players"]), match_br, processed_at, team_summary))
            save_battle(connection, battle_values(
                sid, replay_start, item.get("missionName"), mode, len(data["players"]),
                last_part, processed_at, match_br, team_summary))
        with guard:
            if previous and previous["status"] == "error":
                runtime["errors"] = max(0, runtime["errors"] - 1)
            elif previous and previous["status"] == "pending":
                runtime["pending"] = max(0, runtime["pending"] - 1)
            runtime["processed"] += 1
            runtime["samples"] += added_samples
            runtime["revision"] += 1
            runtime["last_error"] = None
    except HTTPException as error:
        if error.status_code == 429:
            with guard:
                runtime["last_error"] = "Gaijin 返回 429，采集已停止；请稍后再开启。"
            stop_event.set()
        elif error.status_code == 404:
            record_error(sid, item, "结算段暂不可用 (HTTP 404)，稍后重试", pending=True)
        elif error.detail == NO_PLAYERS:
            record_error(sid, item, NO_PLAYERS, pending=True)
        else:
            record_error(sid, item, str(error.detail))
    except Exception as error:
        record_error(sid, item, f"{type(error).__name__}: {error}")
    finally:
        with guard:
            active_sids.discard(sid)
            runtime["active"] = len(active_sids)
        prune_files()


def process_page(items: list[dict]) -> None:
    official_rows = []
    for item in items:
        sid = str(item.get("sessionIdHex") or "").lower()
        if (len(sid) != 16 or any(char not in "0123456789abcdef" for char in sid)
                or item.get("gameMode") != "realistic"
                or item.get("gameType") not in (None, "randomBattle")
                or item.get("isCompleted") is False
                or item.get("statisticGroup") not in GROUPS):
            continue
        official_rows.append((sid, start_time(item), GROUPS[item["statisticGroup"]]))
    if official_rows:
        with db() as connection:
            connection.executemany(
                "INSERT INTO official_replays(sid,start_utc,group_name) VALUES(?,?,?) "
                "ON CONFLICT(sid) DO UPDATE SET start_utc=excluded.start_utc,"
                "group_name=excluded.group_name", official_rows)
    capture_mode = capture_mode_setting()
    candidates = [item for item in items if item.get("statisticGroup") in GROUPS
                  and (capture_mode == "all" or GROUPS[item["statisticGroup"]] == capture_mode)
                  and item.get("gameMode") == "realistic" and item.get("isCompleted") is not False]
    concurrency = setting("concurrency", 2)
    iterator = iter(candidates)
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = set()
        for _ in range(concurrency):
            item = next(iterator, None)
            if item is not None and not stop_event.is_set():
                futures.add(pool.submit(process_replay, item))
        while futures:
            finished, futures = wait(futures, return_when=FIRST_COMPLETED)
            for future in finished:
                future.result()
                if not stop_event.is_set():
                    item = next(iterator, None)
                    if item is not None:
                        futures.add(pool.submit(process_replay, item))


def retry_pending() -> None:
    cutoff = time.time() - 900
    capture_mode = capture_mode_setting()
    with db() as connection:
        rows = connection.execute(
            "SELECT sid,start_utc,map,group_name,last_part FROM replays "
            "WHERE (?='all' OR group_name=?) AND ((status='pending' AND processed_at<=?) OR "
            "(status='error' AND error IN ('结算段 HTTP 404', '结算段未包含玩家数据') "
            "AND processed_at<=?)) "
            "ORDER BY processed_at LIMIT ?", (capture_mode, capture_mode, cutoff, time.time() - 3600,
                                               PENDING_RETRY_BATCH)).fetchall()
    candidates = []
    for row in rows:
        if stop_event.is_set():
            break
        if row["group_name"] not in ("air", "ground") or not isinstance(row["last_part"], int):
            continue
        candidates.append({"sessionIdHex": row["sid"],
                           "startTime": datetime.fromisoformat(row["start_utc"]).timestamp()
                           if row["start_utc"] else None, "missionName": row["map"],
                           "partsCount": row["last_part"], "gameMode": "realistic",
                           "statisticGroup": "aircraft" if row["group_name"] == "air" else "tank"})
    if candidates and not stop_event.is_set():
        process_page(candidates)


def repair_old_missing_lineups() -> None:
    """Reparse incomplete saved battles and refresh their team BR intervals."""
    with db() as connection:
        rows = connection.execute(
            "SELECT r.sid,r.last_part,r.group_name,r.start_utc,r.map,"
            "r.player_count,r.processed_at FROM replays r "
            "JOIN battle.matches b ON b.sid=r.sid "
            "LEFT JOIN lineup_repair_attempts a ON a.sid=r.sid "
            "WHERE r.status='done' AND r.last_part IS NOT NULL "
            "AND r.group_name IN ('air','ground') "
            "AND (b.match_br_min IS NULL OR b.match_br_max IS NULL) "
            "AND (a.sid IS NULL OR (a.retryable=1 AND a.checked_at<?)) "
            "ORDER BY r.processed_at DESC LIMIT 10",
            (time.time() - 3600,)).fetchall()
    changed = False
    for row in rows:
        if stop_event.is_set():
            break
        sid, last_part = row["sid"], row["last_part"]
        try:
            final_path = download_last_part(sid, last_part)
            data = parse_with_lineup_fallback(sid, last_part, final_path)
            data["game_mode"] = "realistic"
            data["battle_group"] = "aircraft" if row["group_name"] == "air" else "tank"
            players, match_br = rated_real_players(data)
            summary = json.dumps(team_br_summary(players, row["group_name"], match_br))
            values = battle_values(
                sid, row["start_utc"], row["map"], row["group_name"],
                row["player_count"], last_part, row["processed_at"],
                match_br, summary)
            if values[7] is None or match_br is None:
                # The saved result still lacks enough data for a reliable lower bound.
                with db() as connection:
                    connection.execute(
                        "INSERT OR REPLACE INTO lineup_repair_attempts "
                        "(sid,checked_at,retryable) VALUES(?,?,?)",
                        (sid, time.time(), int(data.get("_lineup_download_incomplete", False))))
                continue
            teammate_countries = squad_countries(players)
            with db() as connection:
                connection.execute("DELETE FROM samples WHERE sid=?", (sid,))
                for player in players:
                    uid = str(player["uid"])
                    connection.execute(
                        "INSERT INTO samples(sid,uid,player_name,country,squad_br,"
                        "match_br,mode,squad_countries,party_key,configured_vehicles,"
                        "stats_json,team) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                        (sid, uid, player.get("name"), player_country(player),
                         player.get("squad_br"), match_br, row["group_name"],
                         teammate_countries[uid], party_key(player),
                         json.dumps(player.get("configured"), ensure_ascii=False)
                         if player.get("configured") else None,
                         json.dumps({key: player.get(key) for key in PLAYER_STAT_FIELDS},
                                    ensure_ascii=False), player.get("team")))
                save_battle(connection, values)
                connection.execute("UPDATE replays SET match_br=?,team_br_json=? "
                                   "WHERE sid=?", (match_br, summary, sid))
                connection.execute(
                    "INSERT OR REPLACE INTO lineup_repair_attempts "
                    "(sid,checked_at,retryable) VALUES(?,?,0)",
                    (sid, time.time()))
            changed = True
        except HTTPException as error:
            if error.status_code == 429:
                with guard:
                    runtime["last_error"] = "Gaijin 返回 429，采集已停止；请稍后再开启。"
                stop_event.set()
                break
            with db() as connection:
                connection.execute(
                    "INSERT OR REPLACE INTO lineup_repair_attempts "
                    "(sid,checked_at,retryable) VALUES(?,?,1)",
                    (sid, time.time()))
    if changed:
        with db() as connection:
            rebuild_histogram(connection)
            runtime["samples"] = connection.execute("SELECT COUNT(*) FROM samples").fetchone()[0]
        with guard:
            runtime["revision"] += 1


def scan_loop() -> None:
    try:
        pages_since_head = 20 if setting("page", 1) > 1 else 0
        while not stop_event.is_set():
            # Give settled replays a bounded, concurrent retry batch before new pages.
            retry_pending()
            if stop_event.is_set():
                break
            repair_old_missing_lineups()
            if stop_event.is_set():
                break
            page = setting("page", 1)
            if page > 1 and pages_since_head >= 20:
                listing = fetch_page(1)
                process_page(listing["items"])
                pages_since_head = 0
                if stop_event.is_set():
                    break
            listing = fetch_page(page)
            with guard:
                runtime["page"] = page
                runtime["official_total"] = listing["total_count"]
                runtime["scanned_pages"] += 1
            if not listing["items"]:
                save_setting("official_index_complete", 1)
                save_setting("page", 1)
                stop_event.wait(30)
                continue
            process_page(listing["items"])
            if stop_event.is_set():
                break
            save_setting("page", page + 1)
            pages_since_head += 1
            stop_event.wait(0.2)
    except HTTPException as error:
        with guard:
            runtime["last_error"] = f"官网列表：{error.detail}"
    except Exception as error:
        with guard:
            runtime["last_error"] = f"扫描中断：{type(error).__name__}: {error}"
    finally:
        with guard:
            runtime["running"] = False
            runtime["stopping"] = False


def status_data() -> dict:
    with guard:
        snapshot = dict(runtime)
    snapshot["concurrency"] = setting("concurrency", 2)
    snapshot["keep_files"] = setting("keep_files", 100)
    snapshot["capture_mode"] = capture_mode_setting()
    snapshot["saved_files"] = sum(1 for _ in CACHE.glob("*/*.wrpl"))
    snapshot["cookie_configured"] = bool(wt.cookie())
    snapshot["coverage"] = coverage_data()
    return snapshot


def coverage_data() -> dict:
    with db() as connection:
        saved = connection.execute(
            "SELECT value FROM settings WHERE key='capture_modes_seen'").fetchone()
        modes = {mode for mode in (saved["value"].split(",") if saved else [])
                 if mode in ("air", "ground")}
        current = connection.execute(
            "SELECT value FROM settings WHERE key='capture_mode'").fetchone()
        current_mode = current["value"] if current else "all"
        modes.update(("air", "ground") if current_mode == "all" else (current_mode,))
        modes.update(row[0] for row in connection.execute(
            "SELECT DISTINCT group_name FROM replays WHERE status='done' "
            "AND group_name IN ('air','ground')"))
        rows = connection.execute(
            "SELECT o.group_name,COUNT(*) AS official_count,"
            "SUM(CASE WHEN r.status='done' AND EXISTS ("
            "SELECT 1 FROM samples s WHERE s.sid=o.sid) THEN 1 ELSE 0 END) "
            "AS collected_count FROM official_replays o "
            "LEFT JOIN replays r ON r.sid=o.sid GROUP BY o.group_name").fetchall()
        span = connection.execute(
            "SELECT MIN(start_utc) AS earliest_utc,MAX(start_utc) AS latest_utc "
            "FROM official_replays").fetchone()
        complete = connection.execute(
            "SELECT value FROM settings WHERE key='official_index_complete'").fetchone()
    counts = {mode: {"official": 0, "collected": 0} for mode in ("air", "ground")}
    for row in rows:
        if row["group_name"] in counts:
            counts[row["group_name"]] = {
                "official": row["official_count"],
                "collected": row["collected_count"] or 0}
    official = sum(counts[mode]["official"] for mode in modes)
    collected = sum(counts[mode]["collected"] for mode in modes)
    is_complete = bool(complete and complete["value"] == "1")
    return {"modes": sorted(modes), "by_mode": counts, "official": official,
            "collected": collected,
            "ratio": collected / official if is_complete and official else None,
            "complete": is_complete,
            "earliest_utc": span["earliest_utc"], "latest_utc": span["latest_utc"]}


class Control(BaseModel):
    running: bool


class Settings(BaseModel):
    concurrency: int = Field(ge=1, le=20)
    keep_files: int = Field(ge=0, le=1000)
    capture_mode: str = Field(pattern="^(all|air|ground)$")


class DeleteBefore(BaseModel):
    before_utc: datetime


@app.get("/")
def index():
    return FileResponse(ROOT / "static" / "index.html", headers={"Cache-Control": "no-store"})


@app.get("/fonts/wt-symbols.ttf")
def wt_symbols_font():
    return FileResponse(ROOT / "static" / "fonts" / "symbols_skyquake.bfc9728c6a6bc73f209a.ttf",
                        media_type="font/ttf", headers={"Cache-Control": "public, max-age=86400"})


@app.get("/api/status")
def status():
    return status_data()


@app.post("/api/control")
def control(payload: Control):
    global worker_thread
    with guard:
        if payload.running:
            if not wt.cookie():
                raise HTTPException(401, "需要 identity_sid；请配置本目录的 config.local.json。")
            if worker_thread and worker_thread.is_alive():
                if runtime["stopping"]:
                    raise HTTPException(409, "正在停止当前任务，请稍后重新启动。")
            else:
                stop_event.clear()
                with db() as connection:
                    row = connection.execute(
                        "SELECT value FROM settings WHERE key='capture_modes_seen'").fetchone()
                    modes = set(row["value"].split(",")) if row else set()
                    current = connection.execute(
                        "SELECT value FROM settings WHERE key='capture_mode'").fetchone()["value"]
                    modes.update(("air", "ground") if current == "all" else (current,))
                    connection.execute(
                        "INSERT OR REPLACE INTO settings VALUES('capture_modes_seen',?)",
                        (",".join(sorted(modes & {"air", "ground"})),))
                runtime["running"] = True
                runtime["stopping"] = False
                runtime["last_error"] = None
                worker_thread = threading.Thread(target=scan_loop, name="wtstat-all-scan", daemon=True)
                worker_thread.start()
        else:
            stop_event.set()
            runtime["stopping"] = bool(worker_thread and worker_thread.is_alive())
            runtime["running"] = False
    return status_data()


@app.put("/api/settings")
def update_settings(payload: Settings):
    with guard:
        mode_changed = capture_mode_setting() != payload.capture_mode
        if mode_changed and (runtime["running"] or runtime["stopping"] or active_sids or
                             (worker_thread and worker_thread.is_alive())):
            raise HTTPException(409, "请先停止采集，再切换采集模式。")
        save_setting("concurrency", payload.concurrency)
        save_setting("keep_files", payload.keep_files)
        if mode_changed:
            save_setting("capture_mode", payload.capture_mode)
            save_setting("page", 1)
            runtime["page"] = 1
            runtime["official_total"] = None
    prune_files()
    return status_data()


def sample_span_data() -> dict:
    with db() as connection:
        row = connection.execute(
            "SELECT MIN(r.start_utc) AS earliest_utc, MAX(r.start_utc) AS latest_utc, "
            "COUNT(*) AS sample_count, "
            "SUM(CASE WHEN r.start_utc IS NULL THEN 1 ELSE 0 END) AS unknown_time_count "
            "FROM samples s LEFT JOIN replays r ON r.sid=s.sid").fetchone()
    return {"earliest_utc": row["earliest_utc"], "latest_utc": row["latest_utc"],
            "sample_count": row["sample_count"], "unknown_time_count": row["unknown_time_count"] or 0}


@app.get("/api/sample-span")
def sample_span():
    return sample_span_data()


@app.delete("/api/samples/before")
def delete_samples_before(payload: DeleteBefore):
    if payload.before_utc.tzinfo is None:
        raise HTTPException(422, "请提供带时区的时间，例如 2026-10-11T12:00:00Z。")
    cutoff = payload.before_utc.astimezone(timezone.utc).isoformat(timespec="seconds")
    with guard:
        if runtime["running"] or runtime["stopping"] or active_sids or (worker_thread and worker_thread.is_alive()):
            raise HTTPException(409, "请先停止采集，并等待当前下载完成。")
        with db() as connection:
            counts = connection.execute(
                "SELECT COUNT(*) AS samples, COUNT(DISTINCT s.sid) AS replays "
                "FROM samples s JOIN replays r ON r.sid=s.sid WHERE r.start_utc < ?", (cutoff,)).fetchone()
            if counts["samples"]:
                connection.execute(
                    "DELETE FROM samples WHERE sid IN (SELECT sid FROM replays WHERE start_utc < ?)", (cutoff,))
                totals: Counter[tuple[str, str, int, int]] = Counter()
                for row in connection.execute("SELECT mode,country,squad_br,match_br FROM samples"):
                    squad_tick = rating_tick(row["squad_br"], row["mode"])
                    match_tick = rating_tick(row["match_br"], row["mode"])
                    if squad_tick is not None and match_tick is not None:
                        totals[(row["mode"], row["country"] or "", squad_tick, match_tick)] += 1
                connection.execute("DELETE FROM histogram")
                connection.executemany("INSERT INTO histogram VALUES(?,?,?,?,?)",
                                       ((*key, count) for key, count in totals.items()))
            remaining = connection.execute("SELECT COUNT(*) FROM samples").fetchone()[0]
        runtime["samples"] = remaining
        if counts["samples"]:
            runtime["revision"] += 1
    return {"deleted_samples": counts["samples"], "affected_replays": counts["replays"],
            "remaining_samples": remaining, "status": status_data(), "span": sample_span_data()}


@app.delete("/api/data")
def clear_data():
    with guard:
        if runtime["running"] or runtime["stopping"] or active_sids or (worker_thread and worker_thread.is_alive()):
            raise HTTPException(409, "请先停止采集，并等待当前下载完成。")

        # Only touch this program's own cache; never follow a redirected cache directory.
        expected_cache = ROOT.resolve() / "cache" / "replays"
        if CACHE.resolve() != expected_cache:
            raise HTTPException(500, "Replay 缓存路径异常，已取消清除。")
        files = list(CACHE.glob("*/*.wrpl")) + list(CACHE.glob("*/*.part"))
        for path in files:
            if path.is_symlink() or path.parent.is_symlink() or path.resolve().parent.parent != expected_cache:
                raise HTTPException(500, "Replay 文件路径异常，已取消清除。")
        for path in files:
            path.unlink(missing_ok=True)
        for folder in {path.parent for path in files}:
            try:
                folder.rmdir()
            except OSError:
                pass

        with db() as connection:
            deleted_replays = connection.execute("SELECT COUNT(*) FROM replays").fetchone()[0]
            deleted_samples = connection.execute("SELECT COUNT(*) FROM samples").fetchone()[0]
            for table in ("histogram", "samples", "replays", "official_replays",
                          "lineup_repair_attempts"):
                connection.execute(f"DELETE FROM {table}")
            connection.execute("DELETE FROM battle.matches")
            connection.execute("UPDATE settings SET value='1' WHERE key='page'")
            connection.execute("INSERT OR REPLACE INTO settings "
                               "VALUES('official_index_complete','0')")
            connection.execute("INSERT OR REPLACE INTO settings "
                               "VALUES('capture_modes_seen','')")
        runtime.update(page=1, official_total=None, processed=0, errors=0, pending=0, samples=0,
                       last_error=None, active=0, scanned_pages=0)
        runtime["revision"] += 1
    return {"deleted_replays": deleted_replays, "deleted_samples": deleted_samples,
            "deleted_files": len(files), "status": status_data()}


def weighted_summary(counts: Counter[int]) -> dict:
    total = sum(counts.values())
    if not total:
        return {"sample_count": 0, "mean": None, "median": None, "stddev": None,
                "min": None, "max": None, "leader_rate": None, "big_leader_rate": None,
                "small_leader_rate": None, "small_underdog_rate": None,
                "big_underdog_rate": None}
    expanded = sorted(counts.items())
    weighted_sum = sum(tick * count for tick, count in expanded)
    mean_tick = weighted_sum / total
    variance_tick = sum((tick - mean_tick) ** 2 * count for tick, count in expanded) / total
    positions = [(total - 1) // 2, total // 2]
    middle = []
    cumulative = 0
    for tick, count in expanded:
        previous = cumulative
        cumulative += count
        for position in positions[len(middle):]:
            if previous <= position < cumulative:
                middle.append(tick)
            else:
                break
    return {"sample_count": total, "mean": mean_tick / 10,
            "median": sum(middle) / 20, "stddev": math.sqrt(variance_tick) / 10,
            "min": expanded[0][0] / 10, "max": expanded[-1][0] / 10}


def rate_bucket(mode: str, squad_tick: int, match_tick: int,
                minimum_tick: int | None) -> str | None:
    # Near the BR ceiling, the maximum alone cannot describe the lobby's range.
    # Compare these squads with the lowest effective squad BR in their match.
    ceiling = 147 if mode == "air" else 130
    ceiling_group = ceiling - squad_tick < 10
    if ceiling_group:
        if minimum_tick is None:
            return None
        gap = squad_tick - minimum_tick
        buckets = {0: "big_underdog", 3: "small_underdog", 4: "small_underdog",
                   6: "small_leader", 7: "small_leader"}
        return "big_leader" if gap >= 10 else buckets.get(gap)
    gap = match_tick - squad_tick
    buckets = {0: "big_leader", 3: "small_leader", 4: "small_leader",
               6: "small_underdog", 7: "small_underdog", 10: "big_underdog"}
    return buckets.get(gap)


def party_distribution(mode: str, composition_filter: tuple[str, ...] | None,
                       party_size: int | None, time_start: int = 0,
                       time_end: int = 1440, tz_offset: int = 0
                       ) -> tuple[dict[int, Counter[int]], dict[int, Counter[str]],
                                  dict[int, Counter[tuple[int | None, int]]]]:
    """Count each squad once; recover complete legacy squads from member rows."""
    time_clause = ""
    time_params: tuple = ()
    if (time_start, time_end) != (0, 1440):
        clock = "strftime('%H:%M', start_utc, ?)"
        modifier = f"{tz_offset:+d} minutes"
        start = f"{time_start // 60:02d}:{time_start % 60:02d}"
        end = f"{time_end // 60:02d}:{time_end % 60:02d}"
        operator = "AND" if time_start < time_end else "OR"
        time_clause = (f" AND s.sid IN (SELECT sid FROM replays WHERE "
                       f"({clock} >= ? {operator} {clock} < ?))")
        time_params = (modifier, start, modifier, end)
    known: dict[tuple[str, str], list] = {}
    legacy: Counter[tuple] = Counter()
    with db() as connection:
        minima = {}
        for row in connection.execute(
            "SELECT s.sid, MIN(s.squad_br) AS minimum, COUNT(*) AS retained, "
            "COUNT(s.squad_br) AS known "
            "FROM samples s WHERE s.mode=?" +
            time_clause + " GROUP BY s.sid", (mode, *time_params)
        ):
            minima[row["sid"]] = (rating_tick(row["minimum"], mode)
                                  if row["retained"] == row["known"] else None)
        rows = connection.execute(
            "SELECT s.sid,s.country,s.squad_br,s.match_br,s.squad_countries,s.party_key "
            "FROM samples s WHERE s.mode=?" + time_clause, (mode, *time_params))
        for row in rows:
            if row["squad_countries"] is None:
                continue
            try:
                teammates = json.loads(row["squad_countries"])
            except (TypeError, ValueError):
                continue
            if not isinstance(teammates, list) or len(teammates) > 3:
                continue
            nations = [row["country"], *teammates]
            if any(value is not None and value not in COUNTRIES for value in nations):
                continue
            composition = tuple(sorted(nations, key=lambda value: (value is None, value or "")))
            signature = (composition, rating_tick(row["squad_br"], mode),
                         rating_tick(row["match_br"], mode))
            if row["party_key"]:
                identity = (row["sid"], row["party_key"])
                entry = known.setdefault(identity, [signature, 0, True])
                entry[1] += 1
                entry[2] = entry[2] and entry[0] == signature
            else:
                legacy[(row["sid"], signature)] += 1

    grouped: dict[int, Counter[int]] = defaultdict(Counter)
    classified: dict[int, Counter[str]] = defaultdict(Counter)
    ranges: dict[int, Counter[tuple[int | None, int]]] = defaultdict(Counter)

    def add(sid: str, signature: tuple, count: int) -> None:
        composition, squad_tick, match_tick = signature
        if squad_tick is None or match_tick is None:
            return
        if party_size is not None and len(composition) != party_size:
            return
        if composition_filter is not None and composition != composition_filter:
            return
        grouped[squad_tick][match_tick] += count
        minimum_tick = minima.get(sid)
        ranges[squad_tick][(minimum_tick, match_tick)] += count
        bucket = rate_bucket(mode, squad_tick, match_tick, minimum_tick)
        if bucket:
            classified[squad_tick][bucket] += count

    for (sid, _), (signature, members, consistent) in known.items():
        if consistent and members == len(signature[0]):
            add(sid, signature, 1)
    for (sid, signature), members in legacy.items():
        # Old rows lack squad IDs. A complete party contributes exactly one row
        # per member; equal parties with the same lineup contribute a multiple.
        size = len(signature[0])
        if size and members % size == 0:
            add(sid, signature, members // size)
    return grouped, classified, ranges


@app.get("/api/analytics")
def analytics(mode: str = Query("air", pattern="^(air|ground)$"),
              weight: float = Query(1.0), country: str = "",
              teammate_count: str = "", teammate1: str = "",
              teammate2: str = "", teammate3: str = "",
              time_start: int = 0, time_end: int = 1440,
              tz_offset: int = 0):
    tick = rating_tick(weight, mode)
    if tick is None:
        raise HTTPException(400, "所选权重不属于该模式范围。")
    if not (0 <= time_start < 1440 and 0 < time_end <= 1440 and
            time_start % 30 == time_end % 30 == 0 and time_start != time_end and
            -720 <= tz_offset <= 840):
        raise HTTPException(400, "时间段必须使用 00:00–24:00 内的半小时间隔，且起止时间不能相同。")
    if country and country not in COUNTRIES:
        raise HTTPException(400, "未知科技树国家。")
    if teammate_count not in ("", "0", "1", "2", "3"):
        raise HTTPException(400, "组队队友数量只能为不限、单排、1、2 或 3 人。")
    choices = (teammate1, teammate2, teammate3)
    if any(value and value not in COUNTRIES for value in choices):
        raise HTTPException(400, "未知队友科技树国家。")
    if not country and any(choices):
        raise HTTPException(400, "请先选择玩家的科技树国家。")
    if country and not teammate_count:
        raise HTTPException(400, "按国家精确筛选时，请选择队友数量。")
    count = int(teammate_count) if teammate_count else None
    if country and (any(not value for value in choices[:count]) or any(choices[count:])):
        raise HTTPException(400, "请选齐指定数量的队友国家。")
    composition_filter = (tuple(sorted((country, *choices[:count]))) if country else None)
    grouped, classified, ranges = party_distribution(
        mode, composition_filter, count + 1 if count is not None else None,
        time_start, time_end, tz_offset)
    overview = []
    for group_tick in possible_ticks(mode):
        counts = grouped[group_tick]
        summary = weighted_summary(counts)
        n = summary["sample_count"]
        bucket_counts = classified[group_tick]
        big_leader_count = bucket_counts["big_leader"]
        small_leader_count = bucket_counts["small_leader"]
        rates = {"leader_rate": (big_leader_count + small_leader_count) / n if n else None,
                 "big_leader_rate": big_leader_count / n if n else None,
                 "small_leader_rate": small_leader_count / n if n else None,
                 "small_underdog_rate": bucket_counts["small_underdog"] / n if n else None,
                 "big_underdog_rate": bucket_counts["big_underdog"] / n if n else None}
        overview.append({"weight": group_tick / 10, **summary, **rates})
    chosen = next(row for row in overview if round(row["weight"] * 10) == tick)
    distribution = [{"min_br": low / 10 if low is not None else None,
                     "max_br": high / 10, "count": count}
                    for (low, high), count in sorted(
                        ranges[tick].items(), key=lambda item: (
                            item[0][1], item[0][0] if item[0][0] is not None else -1))]
    return {"mode": mode, "country": country or "不限", "selected": chosen,
            "overview": overview, "distribution": distribution,
            "stddev_type": "population", "revision": runtime["revision"]}


def parsed_player_stats(value: str | None) -> dict:
    try:
        data = json.loads(value) if value else None
    except (TypeError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def stat_number(value):
    return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) else None


@app.get("/api/repeat-players")
def repeat_players(min_appearances: int = 10, limit: int = 20,
                   sort: str = "appearances", direction: str = "desc",
                   mode: str = ""):
    if min_appearances not in (5, 10, 20, 50, 100, 200, 500, 1000):
        raise HTTPException(400, "最低出现场次只能选择 5、10、20、50、100、200、500 或 1000。")
    if limit not in (10, 20, 50, 100, 200, 500, 1000):
        raise HTTPException(400, "显示人数只能选择 10、20、50、100、200、500 或 1000。")
    if mode not in ("", "air", "ground"):
        raise HTTPException(400, "模式只能选择全部、历史空战或历史陆战。")
    sort_columns = {"player_name": "player_name COLLATE NOCASE",
                    "appearances": "appearances", "most_common_br": "most_common_br",
                    "average_score": "average_score"}
    if sort not in sort_columns or direction not in ("asc", "desc"):
        raise HTTPException(400, "排序参数无效。")
    order = sort_columns[sort]
    with db() as connection:
        rows = connection.execute(
            "WITH base AS ("
            " SELECT uid,player_name,squad_br,stats_json FROM samples "
            " WHERE uid NOT GLOB '*[^0-9]*' AND CAST(uid AS INTEGER)>0 "
            " AND (?='' OR mode=?) "
            " AND (player_name IS NULL OR player_name NOT LIKE 'coop/Bot%')"
            "), eligible AS ("
            " SELECT uid,COUNT(*) AS appearances,"
            " AVG(CASE WHEN json_valid(stats_json) THEN "
            " CASE WHEN json_type(stats_json,'$.score') IN ('integer','real') "
            " THEN json_extract(stats_json,'$.score') END END) AS average_score,"
            " SUM(CASE WHEN json_valid(stats_json) THEN "
            " CASE WHEN json_type(stats_json,'$.score') IN ('integer','real') "
            " THEN 1 ELSE 0 END ELSE 0 END) AS score_samples "
            " FROM base GROUP BY uid HAVING COUNT(*)>=?"
            "), br_counts AS ("
            " SELECT b.uid,ROUND(b.squad_br,1) AS br,COUNT(*) AS n "
            " FROM base b JOIN eligible e ON e.uid=b.uid "
            " WHERE b.squad_br IS NOT NULL GROUP BY b.uid,br"
            "), br_rank AS ("
            " SELECT uid,br,ROW_NUMBER() OVER (PARTITION BY uid ORDER BY n DESC,br DESC) AS rn "
            " FROM br_counts"
            ") SELECT e.uid,e.appearances,e.average_score,e.score_samples,"
            " br.br AS most_common_br,"
            " COALESCE((SELECT s.player_name FROM samples s "
            " LEFT JOIN replays r ON r.sid=s.sid "
            " WHERE s.uid=e.uid AND s.player_name IS NOT NULL "
            " AND s.player_name NOT LIKE 'coop/Bot%' "
            " AND (?='' OR s.mode=?) "
            " ORDER BY COALESCE(r.start_utc,'') DESC,s.sid DESC LIMIT 1),e.uid) AS player_name "
            " FROM eligible e LEFT JOIN br_rank br ON br.uid=e.uid AND br.rn=1 "
            f" ORDER BY ({order}) IS NULL,{order} {direction.upper()},"
            " e.appearances DESC,e.uid LIMIT ?",
            (mode, mode, min_appearances, mode, mode, limit)).fetchall()
    return {"items": [dict(row) for row in rows]}


@app.get("/api/repeat-players/{uid}")
def repeat_player_detail(uid: str, mode: str = ""):
    if len(uid) > 20 or not uid.isascii() or not uid.isdecimal() or int(uid) <= 0:
        raise HTTPException(400, "玩家 UID 格式无效。")
    if mode not in ("", "air", "ground"):
        raise HTTPException(400, "模式只能选择全部、历史空战或历史陆战。")
    with db() as connection:
        rows = connection.execute(
            "SELECT s.mode,s.squad_br,s.player_name,s.stats_json,s.configured_vehicles FROM samples s "
            "LEFT JOIN replays r ON r.sid=s.sid WHERE s.uid=? "
            "AND (?='' OR s.mode=?) "
            "AND (s.player_name IS NULL OR s.player_name NOT LIKE 'coop/Bot%') "
            "ORDER BY COALESCE(r.start_utc,'') DESC,s.sid DESC", (uid, mode, mode)).fetchall()
    if len(rows) < 2:
        raise HTTPException(404, "没有该玩家的重复样本。")
    def new_group(mode: str, br: float | None) -> dict:
        return {"mode": mode, "weight": br, "appearances": 0,
                "kd_samples": 0, "kd_kills": 0.0, "deaths": 0.0,
                "kr_samples": 0, "kr_kills": 0.0,
                "score_samples": 0, "score_total": 0.0,
                "vehicles": Counter()}

    def add_stats(group: dict, stats: dict) -> None:
        group["appearances"] += 1
        air = stat_number(stats.get("kills"))
        ground = stat_number(stats.get("groundKills"))
        deaths = stat_number(stats.get("deaths"))
        score = stat_number(stats.get("score"))
        if air is not None and ground is not None:
            kills = air + ground
            group["kr_samples"] += 1
            group["kr_kills"] += kills
            if deaths is not None:
                group["kd_samples"] += 1
                group["kd_kills"] += kills
                group["deaths"] += deaths
        if score is not None:
            group["score_samples"] += 1
            group["score_total"] += score

    def finish(group: dict) -> None:
        group["kd"] = group["kd_kills"] / group["deaths"] if group["deaths"] else None
        group["kd_zero_deaths"] = bool(group["kd_samples"] and not group["deaths"] and group["kd_kills"])
        group["kr"] = group["kr_kills"] / group["kr_samples"] if group["kr_samples"] else None
        group["average_score"] = group["score_total"] / group["score_samples"] if group["score_samples"] else None
        group["vehicles"] = [{"code": code, "name": name, "appearances": count}
                             for (code, name), count in sorted(
                                 group["vehicles"].items(), key=lambda item: (-item[1], item[0][1]))]
        group["top_vehicle"] = group["vehicles"][0] if group["vehicles"] else None

    groups: dict[tuple[str, float | None], dict] = {}
    vehicle_groups: dict[tuple[str, float | None, str], dict] = {}
    unattributed_matches = 0
    for row in rows:
        br = round(row["squad_br"], 1) if row["squad_br"] is not None else None
        key = (row["mode"], br)
        group = groups.setdefault(key, new_group(row["mode"], br))
        stats = parsed_player_stats(row["stats_json"])
        add_stats(group, stats)
        try:
            configured = json.loads(row["configured_vehicles"]) if row["configured_vehicles"] else []
        except (TypeError, ValueError):
            configured = []
        vehicle_names = {}
        if isinstance(configured, list):
            for vehicle in configured:
                if isinstance(vehicle, dict):
                    name = vehicle.get("name") or vehicle.get("code")
                    if name:
                        vehicle_names[str(vehicle.get("code") or name)] = str(name)
        # Air RB normally has one selected aircraft. Count only unambiguous
        # single-vehicle lineups; ground lineups cannot identify usage here.
        if row["mode"] == "air" and len(vehicle_names) == 1:
            code, name = next(iter(vehicle_names.items()))
            group["vehicles"][(code, name)] += 1
            vehicle_group = vehicle_groups.setdefault((row["mode"], br, code),
                                                     new_group(row["mode"], br))
            add_stats(vehicle_group, stats)
            vehicle_group["vehicles"][(code, name)] += 1
        elif row["mode"] == "air":
            unattributed_matches += 1
    result = []
    for group in groups.values():
        finish(group)
        result.append(group)
    result.sort(key=lambda group: (0 if group["mode"] == "air" else 1,
                                   group["weight"] is None, -(group["weight"] or 0)))
    by_vehicle = list(vehicle_groups.values())
    for group in by_vehicle:
        finish(group)
    by_vehicle.sort(key=lambda group: (0 if group["mode"] == "air" else 1,
                                       group["weight"] is None, -(group["weight"] or 0),
                                       -group["appearances"], group["top_vehicle"]["name"]))
    name = next((row["player_name"] for row in rows if row["player_name"]), uid)
    return {"uid": uid, "player_name": name, "appearances": len(rows), "groups": result,
            "vehicle_groups": by_vehicle, "unattributed_matches": unattributed_matches}


@app.get("/api/recent")
def recent():
    with db() as connection:
        rows = connection.execute("SELECT b.* FROM battle.matches b JOIN replays r ON r.sid=b.sid "
                                  "WHERE r.status='done' ORDER BY b.processed_at DESC LIMIT 100").fetchall()
    return {"items": [battle_api_row(row) for row in rows]}


@app.get("/api/recent/{sid}")
def recent_detail(sid: str):
    sid = sid.strip().lower()
    if len(sid) != 16 or any(char not in "0123456789abcdef" for char in sid):
        raise HTTPException(400, "Session ID 格式无效。")
    with db() as connection:
        replay = connection.execute(
            "SELECT b.* FROM battle.matches b JOIN replays r ON r.sid=b.sid "
            "WHERE b.sid=? AND r.status='done'", (sid,)).fetchone()
        if replay is None:
            raise HTTPException(404, "未找到已处理的对局。")
        missing_team = connection.execute(
            "SELECT 1 FROM samples WHERE sid=? AND team IS NULL LIMIT 1", (sid,)).fetchone()
    # Older samples predate the team column. Recover it only from a retained local
    # settlement segment; never fetch a replay just to open this view.
    if missing_team:
        parts = sorted((CACHE / sid).glob("*.wrpl"))
        if parts:
            try:
                parsed = wt.parse_replay(sid, [parts[-1]], include_log=False, persist=False)
                with db() as connection:
                    connection.executemany(
                        "UPDATE samples SET team=? WHERE sid=? AND uid=? AND team IS NULL",
                        [(player["team"], sid, str(player["uid"]))
                         for player in parsed.get("players", []) if player.get("team") is not None])
            except Exception:
                pass  # Existing saved data remains available if the old file is unreadable.
    with db() as connection:
        rows = connection.execute(
            "SELECT uid,player_name,country,squad_br,match_br,team,squad_countries,party_key,"
            "configured_vehicles,stats_json FROM samples WHERE sid=? "
            "ORDER BY player_name COLLATE NOCASE,uid", (sid,)).fetchall()
    players = []
    for row in rows:
        player = dict(row)
        for field in ("squad_countries", "configured_vehicles", "stats_json"):
            try:
                player[field] = json.loads(player[field]) if player[field] is not None else None
            except (TypeError, ValueError):
                player[field] = None
        players.append(player)
    match = battle_api_row(replay)
    return {"replay": match, "players": players}


def player_csv_rows(connection: sqlite3.Connection):
    yield ("session_id", "uid", "player_name", "research_country", "squad_br",
           "teammate_research_countries_json", "party_key", "configured_vehicles_json",
           "stats_json", "team")
    for row in connection.execute(
        "SELECT sid,uid,player_name,country,squad_br,squad_countries,party_key,"
        "configured_vehicles,stats_json,team FROM samples "
        f"WHERE {VALID_SAMPLE_SQL} ORDER BY sid,uid"):
        yield tuple(row)


def battle_csv_rows(connection: sqlite3.Connection):
    yield (*BATTLE_COLUMNS, "match_br_range")
    for row in connection.execute(
        f"SELECT {','.join(BATTLE_COLUMNS)} FROM battle.matches ORDER BY sid"):
        data = dict(row)
        low, high = data["match_br_min"], data["match_br_max"]
        match_range = f"[{low:.1f},{high:.1f}]" if low is not None and high is not None else ""
        yield (*row, match_range)


def csv_chunks(rows):
    output = io.StringIO()
    writer = csv.writer(output)
    for index, row in enumerate(rows):
        writer.writerow(row)
        yield output.getvalue().encode("utf-8-sig" if index == 0 else "utf-8")
        output.seek(0)
        output.truncate(0)


def export_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_UTC")


@app.get("/api/export.csv")
def export_csv():
    def generate():
        with db() as connection:
            yield from csv_chunks(player_csv_rows(connection))
    return StreamingResponse(generate(), media_type="text/csv; charset=utf-8",
                             headers={"Content-Disposition":
                                      f"attachment; filename=wtstat_all_players_{export_stamp()}.csv"})


@app.get("/api/export-battles.csv")
def export_battles_csv():
    def generate():
        with db() as connection:
            yield from csv_chunks(battle_csv_rows(connection))
    return StreamingResponse(generate(), media_type="text/csv; charset=utf-8",
                             headers={"Content-Disposition":
                                      f"attachment; filename=wtstat_all_battles_{export_stamp()}.csv"})


@app.get("/api/export-all.zip")
def export_all_zip():
    stamp = export_stamp()
    with tempfile.NamedTemporaryFile(prefix="export-", suffix=".zip", dir=DB.parent,
                                     delete=False) as temporary:
        path = Path(temporary.name)
    try:
        with db() as connection, zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED,
                                                 compresslevel=6, allowZip64=True) as archive:
            for name, rows in ((f"wtstat_all_battles_{stamp}.csv", battle_csv_rows(connection)),
                               (f"wtstat_all_players_{stamp}.csv", player_csv_rows(connection))):
                with archive.open(name, "w") as entry:
                    with io.TextIOWrapper(entry, encoding="utf-8-sig", newline="") as text_file:
                        writer = csv.writer(text_file)
                        writer.writerows(rows)
    except Exception:
        path.unlink(missing_ok=True)
        raise
    async def remove_temporary_zip():
        path.unlink(missing_ok=True)
    return FileResponse(path, filename=f"wtstat_all_{stamp}.zip", media_type="application/zip",
                        headers={"Cache-Control": "no-store"},
                        background=BackgroundTask(remove_temporary_zip))


def import_csv_file(path: Path) -> dict:
    required = {"session_id", "uid", "player_name", "research_country", "squad_br",
                "teammate_research_countries_json", "party_key",
                "configured_vehicles_json", "stats_json", "team"}
    csv.field_size_limit(4_000_000)

    def invalid(line: int, message: str):
        raise HTTPException(422, f"CSV 第 {line} 行：{message}")

    def number(value: str | None, line: int, field: str) -> float | None:
        if not value:
            return None
        try:
            result = float(value)
        except ValueError:
            invalid(line, f"{field} 不是数字。")
        if not math.isfinite(result):
            invalid(line, f"{field} 不是有限数字。")
        return result

    def integer(value: str | None, line: int, field: str, low: int, high: int) -> int | None:
        if not value:
            return None
        if not value.isascii() or not value.isdecimal() or not low <= int(value) <= high:
            invalid(line, f"{field} 无效。")
        return int(value)

    def json_field(value: str | None, line: int, field: str, expected: type) -> str | None:
        if not value:
            return None
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            invalid(line, f"{field} 不是有效 JSON。")
        if not isinstance(parsed, expected):
            invalid(line, f"{field} 的 JSON 类型无效。")
        return json.dumps(parsed, ensure_ascii=False)

    with guard:
        if runtime["running"] or runtime["stopping"] or active_sids or (worker_thread and worker_thread.is_alive()):
            raise HTTPException(409, "请先停止采集，并等待当前下载完成后再导入。")
        imported_replays = imported_samples = skipped_samples = 0
        seen_replays: dict[str, tuple] = {}
        battle_cache: dict[str, sqlite3.Row | None] = {}
        histogram_additions: Counter[tuple[str, str, int, int]] = Counter()
        try:
            with path.open("r", encoding="utf-8-sig", newline="") as source, db() as connection:
                reader = csv.DictReader(source)
                if not reader.fieldnames or len(reader.fieldnames) != len(set(reader.fieldnames)) or not required.issubset(reader.fieldnames):
                    raise HTTPException(422, "CSV 表头不符合 WTStat all 导出格式。")
                for row in reader:
                    line = reader.line_num
                    if None in row or any(value is None for value in row.values()):
                        invalid(line, "列数与表头不一致。")
                    sid = row["session_id"].strip().lower()
                    uid = row["uid"].strip()
                    if len(sid) != 16 or any(char not in "0123456789abcdef" for char in sid):
                        invalid(line, "Session ID 无效。")
                    if uid.startswith("-") and uid[1:].isascii() and uid[1:].isdecimal():
                        skipped_samples += 1
                        continue
                    if not uid or len(uid) > 20 or not uid.isascii() or not uid.isdecimal():
                        invalid(line, "玩家 UID 无效。")
                    name = row["player_name"] or None
                    if not is_real_player(uid, name):
                        skipped_samples += 1
                        continue
                    if sid not in battle_cache:
                        battle_cache[sid] = connection.execute(
                            "SELECT * FROM battle.matches WHERE sid=?", (sid,)).fetchone()
                    battle = battle_cache[sid]
                    mode = (row.get("mode") or (battle["mode"] if battle else "") or "").strip()
                    if mode not in ("air", "ground"):
                        invalid(line, "缺少战局模式；请先导入战局 CSV。")
                    country = row["research_country"] or None
                    party_key = row["party_key"] or None
                    if any(value is not None and len(value) > limit for value, limit in
                           ((name, 256), (country, 100), (party_key, 256))):
                        invalid(line, "文本字段过长。")
                    squad_br = number(row["squad_br"], line, "squad_br")
                    match_br = number(row.get("match_br"), line, "match_br")
                    if match_br is None and battle:
                        match_br = battle["match_br_max"]
                    teammates = json_field(row["teammate_research_countries_json"], line, "队友国籍", list)
                    vehicles = json_field(row["configured_vehicles_json"], line, "配置载具", list)
                    stats = json_field(row["stats_json"], line, "战绩", dict)
                    team = integer(row["team"], line, "team", 0, 2)
                    if (team == 0 and not (json.loads(vehicles) if vehicles else [])
                            and not wt.has_activity(json.loads(stats) if stats else {})):
                        skipped_samples += 1
                        continue
                    start = row.get("replay_start_utc") or (battle["start_utc"] if battle else None)
                    if start:
                        try:
                            parsed = datetime.fromisoformat(start.replace("Z", "+00:00"))
                            if parsed.tzinfo is None:
                                raise ValueError("missing timezone")
                            start = parsed.astimezone(timezone.utc).isoformat()
                        except ValueError:
                            invalid(line, "对局开始时间无效。")
                    replay_map = row.get("replay_map") or (battle["map"] if battle else None)
                    if replay_map and len(replay_map) > 256:
                        invalid(line, "地图名称过长。")
                    player_count = integer(row.get("replay_player_count"), line, "replay_player_count", 1, 256)
                    if player_count is None and battle:
                        player_count = battle["player_count"]
                    last_part = integer(row.get("replay_last_part"), line, "replay_last_part", 0, 255)
                    if last_part is None and battle:
                        last_part = battle["last_part"]
                    processed_at = number(row.get("replay_processed_at"), line, "replay_processed_at")
                    if processed_at is None and battle:
                        processed_at = battle["processed_at"]
                    if processed_at is not None and processed_at < 0:
                        invalid(line, "replay_processed_at 无效。")
                    team_br_json = json_field(row.get("replay_team_br_json"), line, "双方权重", dict)
                    if battle is None and "mode" not in row:
                        invalid(line, "找不到对应战局；请先导入战局 CSV。")
                    metadata = (mode, start, replay_map, player_count, last_part,
                                match_br, processed_at, team_br_json)
                    if sid in seen_replays and seen_replays[sid] != metadata:
                        invalid(line, "同一 Session 的对局元数据不一致。")
                    if sid not in seen_replays:
                        seen_replays[sid] = metadata
                        cursor = connection.execute(
                            "INSERT OR IGNORE INTO replays "
                            "(sid,start_utc,map,mode,group_name,last_part,status,error,"
                            "player_count,match_br,processed_at,team_br_json) "
                            "VALUES(?,?,?,'realistic',?,?,'done',NULL,?,?,?,?)",
                            (sid, start, replay_map, mode, last_part, player_count, match_br,
                             processed_at if processed_at is not None else time.time(), team_br_json))
                        if cursor.rowcount:
                            imported_replays += 1
                        else:
                            connection.execute(
                                "UPDATE replays SET start_utc=COALESCE(start_utc,?),"
                                "map=COALESCE(map,?),last_part=COALESCE(last_part,?),"
                                "player_count=COALESCE(player_count,?),match_br=COALESCE(match_br,?),"
                                "team_br_json=COALESCE(team_br_json,?),"
                                "mode=COALESCE(mode,'realistic'),group_name=COALESCE(group_name,?),"
                                "status='done',error=NULL WHERE sid=?",
                                (start, replay_map, last_part, player_count, match_br,
                                 team_br_json, mode, sid))
                        if battle is None:
                            save_battle(connection, battle_values(
                                sid, start, replay_map, mode, player_count, last_part,
                                processed_at, match_br, team_br_json), only_missing=True)
                    cursor = connection.execute(
                        "INSERT OR IGNORE INTO samples "
                        "(sid,uid,player_name,country,squad_br,match_br,mode,squad_countries,"
                        "party_key,configured_vehicles,stats_json,team) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                        (sid, uid, name, country, squad_br, match_br, mode,
                         teammates, party_key, vehicles, stats, team))
                    if cursor.rowcount:
                        imported_samples += 1
                        squad_tick = rating_tick(squad_br, mode)
                        match_tick = rating_tick(match_br, mode)
                        if squad_tick is not None and match_tick is not None:
                            histogram_additions[(mode, country or "", squad_tick, match_tick)] += 1
                    else:
                        skipped_samples += 1
                connection.executemany(
                    "INSERT INTO histogram VALUES(?,?,?,?,?) "
                    "ON CONFLICT(mode,country,squad_tick,match_tick) "
                    "DO UPDATE SET count=count+excluded.count",
                    ((*key, count) for key, count in histogram_additions.items()))
        except UnicodeError:
            raise HTTPException(422, "CSV 必须使用 UTF-8 编码。") from None
        except csv.Error:
            raise HTTPException(422, "CSV 文件格式无效。") from None
        with db() as connection:
            runtime["processed"] = connection.execute("SELECT COUNT(*) FROM replays WHERE status='done'").fetchone()[0]
            runtime["errors"] = connection.execute("SELECT COUNT(*) FROM replays WHERE status='error'").fetchone()[0]
            runtime["pending"] = connection.execute("SELECT COUNT(*) FROM replays WHERE status='pending'").fetchone()[0]
            runtime["samples"] = connection.execute("SELECT COUNT(*) FROM samples").fetchone()[0]
        runtime["revision"] += 1
        return {"imported_replays": imported_replays, "imported_samples": imported_samples,
                "skipped_samples": skipped_samples, "status": status_data()}


def import_battles_file(path: Path) -> dict:
    csv.field_size_limit(4_000_000)
    with guard:
        if runtime["running"] or runtime["stopping"] or active_sids or (worker_thread and worker_thread.is_alive()):
            raise HTTPException(409, "请先停止采集，并等待当前下载完成后再导入。")
        added = skipped = 0
        try:
            with path.open("r", encoding="utf-8-sig", newline="") as source, db() as connection:
                reader = csv.DictReader(source)
                if not reader.fieldnames or len(reader.fieldnames) != len(set(reader.fieldnames)) or not set(BATTLE_COLUMNS).issubset(reader.fieldnames):
                    raise HTTPException(422, "战局 CSV 表头不符合 WTStat all 导出格式。")
                for row in reader:
                    line = reader.line_num
                    if None in row or any(value is None for value in row.values()):
                        raise HTTPException(422, f"CSV 第 {line} 行：列数与表头不一致。")
                    sid = row["sid"].strip().lower()
                    mode = row["mode"].strip()
                    if len(sid) != 16 or any(char not in "0123456789abcdef" for char in sid):
                        raise HTTPException(422, f"CSV 第 {line} 行：Session ID 无效。")
                    if mode not in ("air", "ground"):
                        raise HTTPException(422, f"CSV 第 {line} 行：模式无效。")
                    values = [sid]
                    for name in BATTLE_COLUMNS[1:]:
                        raw = row[name].strip()
                        if name == "mode":
                            values.append(mode)
                        elif name == "start_utc":
                            if raw:
                                try:
                                    date = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                                    if date.tzinfo is None:
                                        raise ValueError()
                                    raw = date.astimezone(timezone.utc).isoformat()
                                except ValueError:
                                    raise HTTPException(422, f"CSV 第 {line} 行：开始时间无效。")
                            values.append(raw or None)
                        elif name == "map":
                            if len(raw) > 256:
                                raise HTTPException(422, f"CSV 第 {line} 行：地图名称过长。")
                            values.append(raw or None)
                        elif name in ("player_count", "last_part"):
                            if raw and (not raw.isascii() or not raw.isdecimal() or
                                        not (1 <= int(raw) <= 256 if name == "player_count" else 0 <= int(raw) <= 255)):
                                raise HTTPException(422, f"CSV 第 {line} 行：{name} 无效。")
                            values.append(int(raw) if raw else None)
                        else:
                            try:
                                value = float(raw) if raw else None
                            except ValueError:
                                raise HTTPException(422, f"CSV 第 {line} 行：{name} 无效。")
                            if value is not None and (not math.isfinite(value) or value < 0):
                                raise HTTPException(422, f"CSV 第 {line} 行：{name} 无效。")
                            values.append(value)
                    data = dict(zip(BATTLE_COLUMNS, values))
                    for prefix in ("match", "team1", "team2"):
                        low = data[f"{prefix}_br_min"]
                        high = data[f"{prefix}_br_max"]
                        if ((prefix != "match" and (low is None) != (high is None)) or
                            (high is not None and rating_tick(high, mode) is None) or
                            (low is not None and (high is None or rating_tick(low, mode) is None or low > high))):
                            raise HTTPException(422, f"CSV 第 {line} 行：{prefix} 权重区间无效。")
                    for team in (1, 2):
                        avg = data[f"team{team}_average_squad_br"]
                        low = data[f"team{team}_br_min"]
                        high = data[f"team{team}_br_max"]
                        if avg is not None and (low is None or not low <= avg <= high):
                            raise HTTPException(422, f"CSV 第 {line} 行：队伍平均组队权重无效。")
                    existing = connection.execute(
                        f"SELECT {','.join(BATTLE_COLUMNS)} FROM battle.matches WHERE sid=?", (sid,)).fetchone()
                    if existing:
                        skipped += 1
                        continue
                    save_battle(connection, tuple(values), only_missing=True)
                    added += 1
                    connection.execute(
                        "INSERT INTO replays(sid,start_utc,map,mode,group_name,last_part,status,error,"
                        "player_count,match_br,processed_at) VALUES(?,?,?,'realistic',?,?,'done',NULL,?,?,?) "
                        "ON CONFLICT(sid) DO UPDATE SET start_utc=COALESCE(replays.start_utc,excluded.start_utc),"
                        "map=COALESCE(replays.map,excluded.map),"
                        "player_count=COALESCE(replays.player_count,excluded.player_count),"
                        "match_br=COALESCE(replays.match_br,excluded.match_br),status='done',error=NULL",
                        (sid, data["start_utc"], data["map"], mode, data["last_part"],
                         data["player_count"], data["match_br_max"],
                         data["processed_at"] if data["processed_at"] is not None else time.time()))
        except (UnicodeError, csv.Error):
            raise HTTPException(422, "CSV 文件格式或编码无效。") from None
        with db() as connection:
            runtime["processed"] = connection.execute("SELECT COUNT(*) FROM replays WHERE status='done'").fetchone()[0]
            runtime["errors"] = connection.execute("SELECT COUNT(*) FROM replays WHERE status='error'").fetchone()[0]
            runtime["pending"] = connection.execute("SELECT COUNT(*) FROM replays WHERE status='pending'").fetchone()[0]
        runtime["revision"] += 1
        return {"imported_battles": added, "skipped_battles": skipped, "status": status_data()}


@app.post("/api/import.csv")
async def import_csv(request: Request):
    if request.headers.get("content-type", "").split(";", 1)[0].strip().lower() not in ("text/csv", "application/csv"):
        raise HTTPException(415, "请选择 CSV 文件。")
    with guard:
        if runtime["running"] or runtime["stopping"] or active_sids or (worker_thread and worker_thread.is_alive()):
            raise HTTPException(409, "请先停止采集，并等待当前下载完成后再导入。")
    path = None
    try:
        with tempfile.NamedTemporaryFile(prefix="import-", suffix=".csv", dir=DB.parent,
                                         delete=False) as temporary:
            path = Path(temporary.name)
            total = 0
            async for chunk in request.stream():
                total += len(chunk)
                if total > MAX_IMPORT_BYTES:
                    raise HTTPException(413, "CSV 文件超过 300 MB 上限。")
                temporary.write(chunk)
        return await asyncio.to_thread(import_csv_file, path)
    finally:
        if path is not None:
            path.unlink(missing_ok=True)


@app.post("/api/import-battles.csv")
async def import_battles_csv(request: Request):
    if request.headers.get("content-type", "").split(";", 1)[0].strip().lower() not in ("text/csv", "application/csv"):
        raise HTTPException(415, "请选择 CSV 文件。")
    with guard:
        if runtime["running"] or runtime["stopping"] or active_sids or (worker_thread and worker_thread.is_alive()):
            raise HTTPException(409, "请先停止采集，并等待当前下载完成后再导入。")
    path = None
    try:
        with tempfile.NamedTemporaryFile(prefix="import-battles-", suffix=".csv", dir=DB.parent,
                                         delete=False) as temporary:
            path = Path(temporary.name)
            total = 0
            async for chunk in request.stream():
                total += len(chunk)
                if total > MAX_IMPORT_BYTES:
                    raise HTTPException(413, "CSV 文件超过 300 MB 上限。")
                temporary.write(chunk)
        return await asyncio.to_thread(import_battles_file, path)
    finally:
        if path is not None:
            path.unlink(missing_ok=True)
