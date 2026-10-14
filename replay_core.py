"""Replay fetching, result parsing and vehicle ratings for WTStat All.

All resources are resolved beside this file so the sampler can run without
the separate WTStat application.
"""
from __future__ import annotations

import csv
import json
import re
import subprocess
import threading
import time
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path

import requests
from fastapi import HTTPException

ROOT = Path(__file__).resolve().parent
CONFIG = ROOT / "config.local.json"
PARSER = ROOT / "bin" / "wtstat_results.exe"
VEHICLES = ROOT / "data" / "wiki_vehicles.csv"
FILES = "https://wt-game-replays.warthunder.com"
MAX_REPLAY_PARTS = 256
UA = "WTStat-All/1.0 (local replay sampler)"
LOCK = threading.Lock()
LAST_REQUEST = 0.0

COUNTRIES = {"usa": "美国", "germany": "德国", "ussr": "苏联", "britain": "英国",
             "japan": "日本", "italy": "意大利", "france": "法国", "sweden": "瑞典",
             "china": "中国", "israel": "以色列"}

# Verified vehicles that may be absent from an older bundled Wiki snapshot.
VEHICLE_OVERRIDES = {
    "f_14d": ("F-14D", "usa"),
    "f_14d_vf_11": ("F-14D (VF-11)", "usa"),
    "f_16am_block_20_mlu_netherlands": ("F-16AM", "france"),
    "f_16xl": ("F-16XL", "usa"),
    "germ_boxer_swatrinf": ("sWaTrgInf", "germany"),
    "germ_schutzenpanzer_puma_vjtf_mod": ("Puma u14", "germany"),
    "mig_29_9_12": ("MiG-29 (9-12)", "ussr"),
    "mig_29kr_9_41r": ("MiG-29KR (9-41R)", "ussr"),
    "mig_29m_9_15": ("MiG-29M (9-15)", "ussr"),
    "saab_jas39d": ("JAS39D", "sweden"),
    "ussr_t_72b3_arena": ('T-72B3 "Arena"', "ussr"),
    "yak_130_early": ("Yak-130", "ussr"),
}


def cookie() -> str | None:
    if not CONFIG.exists():
        return None
    try:
        value = json.loads(CONFIG.read_text(encoding="utf-8")).get("identity_sid")
        return value.strip() if isinstance(value, str) and value.strip() else None
    except (ValueError, OSError):
        return None


def request(url, *, auth=False, stream=False, method="GET", payload=None, headers=None):
    global LAST_REQUEST
    jar = {"identity_sid": cookie()} if auth and cookie() else None
    if auth and not jar:
        raise HTTPException(401, "官网搜索需要 identity_sid。请按 README 创建 config.local.json。")
    with LOCK:
        delay = 0.5 - (time.monotonic() - LAST_REQUEST)
        if delay > 0:
            time.sleep(delay)
        LAST_REQUEST = time.monotonic()
    for attempt in range(3):
        try:
            response = requests.request(
                method, url, json=payload, cookies=jar,
                headers={"User-Agent": UA, **(headers or {})}, timeout=(8, 45),
                stream=stream, allow_redirects=True,
            )
        except requests.RequestException as error:
            if attempt == 2:
                raise HTTPException(504, f"请求超时或网络错误：{type(error).__name__}") from error
            time.sleep(attempt + 1)
            continue
        if response.status_code == 429:
            retry_after = str(response.headers.get("Retry-After", "3"))
            delay = min(int(retry_after), 10) if retry_after.isdigit() else 3
            response.close()
            if attempt == 2:
                raise HTTPException(429, "Gaijin 请求频率限制，请稍后重试。")
            time.sleep(delay)
            continue
        if response.status_code >= 500 and attempt < 2:
            response.close()
            time.sleep(attempt + 1)
            continue
        return response
    raise HTTPException(502, "Gaijin 服务暂时不可用。")


@lru_cache(maxsize=1)
def wiki_catalog() -> dict[str, dict]:
    """Read the bundled snapshot directly; no WTStat SQLite database is needed."""
    if not VEHICLES.is_file():
        return {}
    with VEHICLES.open(encoding="utf-8-sig", newline="") as source:
        rows = csv.DictReader(source)
        required = {"vehicle_id", "name", "research_country_code", "research_country",
                    "operator_country", "category", "br_ab", "br_rb", "br_sb",
                    "br_ground_rb", "br_ground_sb"}
        if not required.issubset(rows.fieldnames or []):
            return {}
        result = {}
        for row in rows:
            code = (row.get("vehicle_id") or "").casefold()
            if not code:
                continue
            for field in ("br_ab", "br_rb", "br_sb", "br_ground_rb", "br_ground_sb"):
                row[field] = float(row[field]) if row[field] else None
            result[code] = row
        return result


def vehicle(code: str) -> dict:
    wiki = wiki_catalog().get(code.casefold())
    if wiki:
        return {"name": wiki["name"], "code": code,
                "country": COUNTRIES.get(wiki["research_country_code"]) or wiki["research_country"],
                "research_country": wiki["research_country"],
                "operator_country": wiki["operator_country"]}
    if code in VEHICLE_OVERRIDES:
        name, country = VEHICLE_OVERRIDES[code]
        return {"name": name, "code": code, "country": COUNTRIES[country]}
    return {"name": code, "code": code, "country": None}


def parse_replay(sid: str, files: list[Path], *, include_log=False, persist=False) -> dict:
    """Parse the saved result segment with the bundled Windows executable."""
    if not PARSER.is_file():
        raise HTTPException(500, "缺少本目录 bin/wtstat_results.exe。请重新下载完整程序。")
    if not files:
        raise HTTPException(422, "没有 Replay 结算分段。")
    try:
        completed = subprocess.run(
            [str(PARSER), str(files[0]), str(files[-1])], capture_output=True,
            text=True, encoding="utf-8", errors="replace", timeout=40, check=True,
        )
        raw = json.loads(completed.stdout)
    except (OSError, subprocess.SubprocessError, ValueError) as error:
        raise HTTPException(502, f"Replay 结算数据解析失败：{type(error).__name__}") from error
    result = raw.get("results") or {}
    header = raw.get("header") or {}
    matching = dict(result.get("matchingInfo") or {})
    missing = {str(item.get("userId")) for item in result.get("player") or []
               if str(item.get("userId") or "").isascii()
               and str(item.get("userId") or "").isdecimal()
               and int(item["userId"]) > 0
               and not str(item.get("name") or "").lower().startswith("coop/bot")
               and has_activity(item)
               and not (matching.get(str(item["userId"])) or {}).get("crafts_info")}
    for earlier in reversed(files[:-1]):
        if not missing:
            break
        try:
            snapshot = json.loads(subprocess.run(
                [str(PARSER), str(earlier)], capture_output=True, text=True,
                encoding="utf-8", errors="replace", timeout=40, check=True).stdout)
        except (OSError, subprocess.SubprocessError, ValueError):
            continue
        if (snapshot.get("header") or {}).get("session_id") != sid:
            continue
        old_matching = ((snapshot.get("results") or {}).get("matchingInfo") or {})
        for uid in tuple(missing):
            info = old_matching.get(uid)
            if isinstance(info, dict) and info.get("crafts_info"):
                matching[uid] = info
                missing.remove(uid)
    players = []
    for item in result.get("player") or []:
        uid = str(item.get("userId") or "")
        if not uid or uid == "-1":
            continue
        configured = []
        crafts = (matching.get(uid) or {}).get("crafts_info") or {}
        for key in sorted(crafts, key=lambda value: int(value[5:]) if re.fullmatch(r"array\d+", value) else 9999):
            craft = crafts[key]
            if isinstance(craft, dict) and craft.get("name"):
                configured.append(vehicle(craft["name"]))
        players.append({
            "uid": uid, "name": item.get("name"), "team": item.get("team"),
            "squad_id": item.get("squadId"), "auto_squad": item.get("autoSquad"),
            "spawned": [], "configured": configured,
            "kills": item.get("kills"), "groundKills": item.get("groundKills"),
            "humanKills": item.get("humanKills"), "deaths": item.get("deaths"),
            "assists": item.get("assists"), "score": item.get("score"),
            "aiKills": item.get("aiKills"), "aiGroundKills": item.get("aiGroundKills"),
            "aiNavalKills": item.get("aiNavalKills"), "captureZone": item.get("captureZone"),
            "teamKills": item.get("teamKills"),
        })
    players = [player for player in players if not is_unjoined_player(player)]
    start = header.get("start_time")
    return {
        "sid": sid,
        "start_utc": datetime.fromtimestamp(start, timezone.utc).isoformat() if start else None,
        "map": (header.get("location") or "").split("\0", 1)[0] or None,
        "players": players, "source": "Replay 结算 BLK", "parts": len(files),
        "vehicle_note": "配置载具是玩家带入对局的阵容，不能证明每辆都实际出击。",
    }


def vehicle_battle_rating(code: str, mode: str, group: str) -> float | None:
    if mode not in {"arcade", "realistic", "simulation"}:
        return None
    if group not in {"aircraft", "aviation", "air", "tank", "ground", "ground_vehicles",
                     "ship", "ships", "fleet", "naval", "boat"}:
        return None
    item = wiki_catalog().get(str(code).casefold())
    if not item:
        return None
    column = {"arcade": "br_ab", "realistic": "br_rb", "simulation": "br_sb"}[mode]
    if item["category"] == "aviation" and group in {"tank", "ground", "ground_vehicles"}:
        column = {"realistic": "br_ground_rb", "simulation": "br_ground_sb"}.get(mode, column)
    return item[column]


VEHICLE_CATEGORY_ORDER = {"ground": 0, "aviation": 1, "helicopters": 2,
                          "boats": 3, "ships": 3}
ACTIVITY_FIELDS = ("kills", "groundKills", "humanKills", "deaths", "assists", "score",
                   "aiKills", "aiGroundKills", "aiNavalKills", "captureZone", "teamKills")


def has_activity(player: dict) -> bool:
    return any(player.get(field) not in (None, 0, 0.0) for field in ACTIVITY_FIELDS)


def is_unjoined_player(player: dict) -> bool:
    """A roster entry with no team, lineup, spawn, or battle activity."""
    return (player.get("team") not in (1, 2) and not player.get("configured")
            and not player.get("spawned") and not has_activity(player))


def has_participation_evidence(player: dict) -> bool:
    return bool(player.get("configured") or player.get("spawned") or has_activity(player))


def add_battle_ratings(data: dict) -> dict:
    """Calculate lineup, match and non-automatic squad BR from local data."""
    mode, group = data.get("game_mode"), data.get("battle_group")
    players = data.get("players") or []
    for player in players:
        configured = player.get("configured") or []
        configured.sort(key=lambda item: (
            VEHICLE_CATEGORY_ORDER.get(
                (wiki_catalog().get(str(item.get("code") or "").casefold()) or {}).get("category"), 4),
            vehicle_battle_rating(item.get("code"), mode, group) is None,
            -(vehicle_battle_rating(item.get("code"), mode, group) or 0),
        ))
        ratings = [vehicle_battle_rating(item.get("code"), mode, group) for item in configured]
        player["max_br"] = max(ratings) if ratings and all(value is not None for value in ratings) else None
    participants = [player for player in players if has_participation_evidence(player)]
    ratings = [player.get("max_br") for player in participants]
    data["match_br"] = (max(ratings) if ratings and all(isinstance(br, (int, float)) for br in ratings)
                        else None)
    for player in players:
        player["squad_br"] = None
        player["squad_members"] = None
        if not has_participation_evidence(player):
            continue
        if player.get("auto_squad") is True:
            members = [player]
        elif player.get("auto_squad") is False:
            squad_id, team = player.get("squad_id"), player.get("team")
            members = ([other for other in participants
                        if other.get("team") == team and other.get("squad_id") == squad_id
                        and other.get("auto_squad") is False]
                       if squad_id is not None and team is not None else [player])
        else:
            continue
        member_ratings = [member.get("max_br") for member in members]
        if member_ratings and all(isinstance(br, (int, float)) for br in member_ratings):
            player["squad_br"] = max(member_ratings)
        player["squad_members"] = [member.get("name") or member.get("uid") for member in members]
    return data
