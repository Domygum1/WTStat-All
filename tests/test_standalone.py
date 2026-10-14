"""Smoke test the sampler from a folder with no sibling WTStat checkout."""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RESOURCES = (
    "server.py",
    "replay_core.py",
    "static/index.html",
    "static/fonts/symbols_skyquake.bfc9728c6a6bc73f209a.ttf",
    "data/wiki_vehicles.csv",
    "bin/wtstat_results.exe",
)


class StandaloneTest(unittest.TestCase):
    def test_isolated_import_and_endpoints(self):
        work = ROOT / "work"
        work.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="wtstat-all-test-", dir=work) as temporary:
            isolated = Path(temporary) / "All"
            for relative in RESOURCES:
                target = isolated / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(ROOT / relative, target)
            self.assertFalse((isolated.parent / "WTStat").exists())
            script = """
import replay_core
import server
import csv
import json
import sqlite3
import zipfile
from pathlib import Path
from unittest.mock import patch

assert replay_core.wiki_catalog()
assert replay_core.cookie() is None
player = {"uid": "1", "name": "test", "team": 1, "squad_id": None,
          "auto_squad": True, "configured": [replay_core.vehicle("a-20g")]}
data = {"game_mode": "realistic", "battle_group": "aircraft", "players": [player]}
replay_core.add_battle_ratings(data)
assert data["match_br"] == player["squad_br"] == 2.7
assert server.index().path.is_file()
assert server.wt_symbols_font().path.is_file()
assert server.status()["cookie_configured"] is False
server.update_settings(server.Settings(concurrency=2, keep_files=100,
                                       capture_mode="air"))
assert server.coverage_data()["modes"] == ["air"]
server.update_settings(server.Settings(concurrency=2, keep_files=100,
                                       capture_mode="all"))
class EmptyListing:
    status_code = 200
    headers = {"Content-Type": "application/json"}
    def json(self): return {"items": [], "total_count": 0}
    def close(self): pass
with patch.object(server.wt, "request", return_value=EmptyListing()):
    try:
        server.fetch_page(1)
    except server.HTTPException as error:
        assert error.status_code == 401
    else:
        raise AssertionError("An expired login must not count as zero battles")
assert server.is_real_player("123", "notrobot_1")
assert not server.is_real_player("-12", "player")
assert not server.is_real_player("0", "player")
assert not server.is_real_player("123", "COOP/bot512")
bot = {"uid": "-1", "name": "coop/Bot512", "team": 1,
       "squad_id": 5, "auto_squad": False, "configured": []}
human = {**player, "squad_id": 5, "auto_squad": False}
real, match_br = server.rated_real_players({
    "game_mode": "realistic", "battle_group": "aircraft",
    "players": [human, bot]})
assert len(real) == 1 and match_br == 2.7 and real[0]["squad_br"] == 2.7
unspawned = {"uid": "2", "name": "unspawned", "team": 2, "auto_squad": True,
             "configured": [], "score": 0}
phantom = {"uid": "3", "name": "not joined", "team": 0, "auto_squad": False,
           "configured": [], "score": 0}
real, match_br = server.rated_real_players({
    "game_mode": "realistic", "battle_group": "aircraft",
    "players": [human, unspawned, phantom]})
assert [item["uid"] for item in real] == ["1", "2"]
assert match_br == 2.7 and unspawned["squad_br"] is None
one_human_team = server.team_br_summary(real, "air", match_br)
assert one_human_team["1"]["min"] == 2.7
assert one_human_team["2"] == {"min": 2.7, "max": 2.7,
                                "average_squad_br": 2.7}
red_only = server.team_br_summary([{**real[0], "team": 2}], "air", match_br)
assert red_only["1"] == {"min": 2.7, "max": 2.7,
                          "average_squad_br": 2.7}
assert server.battle_values("f", None, None, "air", 2, 0, 0, match_br,
                            json.dumps(one_human_team))[7:] == (
                                2.7, 2.7, 2.7, 2.7, 2.7, 2.7, 2.7, 2.7)
assert server.battle_values("f", None, None, "air", 2, 0, 0, None,
                            json.dumps(server.team_br_summary(real, "air")))[7:9] == (None, None)
assert server.team_br_summary([], "air", 2.7) == {"1": None, "2": None}
other_human = {**human, "uid": "4", "team": 2, "squad_id": None,
               "auto_squad": True}
both, both_br = server.rated_real_players({
    "game_mode": "realistic", "battle_group": "aircraft",
    "players": [human, other_human]})
assert server.team_br_summary(both, "air", both_br)["2"]["min"] == 2.7
missing_player = {**human, "configured": [], "score": 717}
resolved_player = {**missing_player, "configured": [replay_core.vehicle("ki_109")]}
def snapshot(_sid, files, **_kwargs):
    names = {path.name for path in files}
    return {"players": [resolved_player if "0001.wrpl" in names else missing_player]}
with patch.object(server, "download_last_part", side_effect=lambda _sid, n: Path(f"{n:04d}.wrpl")) as dl, \
     patch.object(server.wt, "parse_replay", side_effect=snapshot):
    found = server.parse_with_lineup_fallback("aaaaaaaaaaaaaaaa", 3, Path("0003.wrpl"))
    assert found["players"][0]["configured"][0]["code"] == "ki_109"
    assert [call.args[1] for call in dl.call_args_list] == [1]
def late_snapshot(_sid, files, **_kwargs):
    names = {path.name for path in files}
    return {"players": [resolved_player if "0002.wrpl" in names else missing_player]}
with patch.object(server, "download_last_part", side_effect=lambda _sid, n: Path(f"{n:04d}.wrpl")) as dl, \
     patch.object(server.wt, "parse_replay", side_effect=late_snapshot):
    found = server.parse_with_lineup_fallback("aaaaaaaaaaaaaaaa", 3, Path("0003.wrpl"))
    assert found["players"][0]["configured"][0]["code"] == "ki_109"
    assert [call.args[1] for call in dl.call_args_list] == [1, 0, 1, 2]
bot_only_sid = "eeeeeeeeeeeeeeee"
bot_only_item = {"sessionIdHex": bot_only_sid, "partsCount": 1,
                 "gameMode": "realistic", "statisticGroup": "aircraft",
                 "missionName": "Bot-only battle"}
processed_before = server.runtime["processed"]
with patch.object(server, "download_last_part", return_value=Path("ignored.wrpl")) as download, \
     patch.object(server.wt, "parse_replay", return_value={"players": [bot]}):
    server.process_replay(bot_only_item)
    server.process_replay(bot_only_item)
    assert download.call_count == 1
with server.db() as connection:
    assert connection.execute("SELECT status FROM replays WHERE sid=?",
                              (bot_only_sid,)).fetchone()[0] == "skipped"
    assert connection.execute("SELECT COUNT(*) FROM battle.matches WHERE sid=?",
                              (bot_only_sid,)).fetchone()[0] == 0
    assert connection.execute("SELECT COUNT(*) FROM samples WHERE sid=?",
                              (bot_only_sid,)).fetchone()[0] == 0
assert server.runtime["processed"] == processed_before
unplayed_sid = "abababababababab"
with patch.object(server, "download_last_part", return_value=Path("ignored.wrpl")), \
     patch.object(server.wt, "parse_replay", return_value={"players": [unspawned]}):
    server.process_replay({**bot_only_item, "sessionIdHex": unplayed_sid})
with server.db() as connection:
    assert connection.execute("SELECT status FROM replays WHERE sid=?",
                              (unplayed_sid,)).fetchone()[0] == "skipped"
    assert connection.execute("SELECT COUNT(*) FROM battle.matches WHERE sid=?",
                              (unplayed_sid,)).fetchone()[0] == 0
with sqlite3.connect(":memory:") as connection:
    connection.execute("CREATE TABLE samples (sid TEXT,uid TEXT,player_name TEXT,"
                       "country TEXT,squad_br REAL,squad_countries TEXT,party_key TEXT,"
                       "configured_vehicles TEXT,stats_json TEXT,team INTEGER)")
    connection.executemany("INSERT INTO samples(sid,uid,player_name) VALUES(?,?,?)", [
        ("a", "123", "real_player"),
        ("a", "124", "notrobot_1"),
        ("a", "125", None),
        ("a", "126", "coop/Bot344"),
        ("a", "127", "COOP/bot512"),
        ("a", "0", "unnamed"),
        ("a", "-1", "unnamed"),
    ])
    rows = list(server.player_csv_rows(connection))
    assert [row[1] for row in rows[1:]] == ["123", "124", "125"]
with server.db() as connection:
    connection.executemany(
        "INSERT INTO samples(sid,uid,player_name,mode,squad_br,match_br) VALUES(?,?,?,?,?,?)",
        [("a", "123", "notrobot_1", "air", 2.7, 2.7),
         ("a", "-12", "player", "air", 2.7, 2.7),
         ("a", "124", "coop/Bot56", "air", 2.7, 2.7)])
server.runtime["samples"] = 3
assert server.remove_saved_bots() == 2
assert server.runtime["samples"] == 1
with server.db() as connection:
    assert [row[0] for row in connection.execute("SELECT uid FROM samples")] == ["123"]
    assert connection.execute("SELECT SUM(count) FROM histogram").fetchone()[0] == 1

fields = ("session_id", "uid", "player_name", "research_country", "squad_br",
          "teammate_research_countries_json", "party_key", "configured_vehicles_json",
          "stats_json", "team", "mode")
path = Path("players.csv")
with path.open("w", newline="", encoding="utf-8") as file:
    writer = csv.DictWriter(file, fieldnames=fields)
    writer.writeheader()
    for uid, name in [("-13", "player"), ("0", "player"),
                      ("125", "coop/Bot57"), ("126", "real_player")]:
        writer.writerow({"session_id": "abcdef0123456789", "uid": uid,
                         "player_name": name, "mode": "air"})
result = server.import_csv_file(path)
assert result["imported_samples"] == 1
assert result["skipped_samples"] == 3
with server.db() as connection:
    assert [row[0] for row in connection.execute(
        "SELECT uid FROM samples ORDER BY uid")] == ["123", "126"]
    connection.executemany(
        "INSERT INTO replays(sid,mode,status,processed_at,player_count) "
        "VALUES(?,'air','done',0,2)",
        [("bbbbbbbbbbbbbbbb",), ("cccccccccccccccc",)])
    connection.executemany(
        "INSERT INTO samples(sid,uid,mode,squad_br,match_br,squad_countries,party_key) "
        "VALUES(?,?,?,?,?,'[]',?)",
        [("bbbbbbbbbbbbbbbb", "201", "air", 2.7, 2.7, "solo:201"),
         ("cccccccccccccccc", "202", "air", 2.7, 2.7, "solo:202"),
         ("cccccccccccccccc", "203", "air", None, 2.7, "solo:203")])
_, _, ranges = server.party_distribution("air", None, None)
assert ranges[27][(27, 27)] == 1  # A removed Bot does not hide the known lower bound.
assert ranges[27][(None, 27)] == 1  # A retained player with no BR still does.

with server.db() as connection:
    connection.execute("INSERT INTO battle.matches(sid,mode) VALUES('dddddddddddddddd','air')")
    connection.execute("INSERT INTO replays(sid,mode,group_name,status,processed_at) "
                       "VALUES('dddddddddddddddd','realistic','air','done',0)")
    connection.executemany(
        "INSERT INTO samples(sid,uid,mode,team,party_key,configured_vehicles) "
        "VALUES('dddddddddddddddd',?,'air',?,?,?)",
        [("301", 1, "solo:301", '[{"code":"a-20g"}]'),
         ("302", 2, "solo:302", '[{"code":"a-20g"}]')])
result = server.repair_missing_battle_ratings()
assert result["repaired"] == 1 and result["unresolved"] >= 1
with server.db() as connection:
    battle = connection.execute("SELECT match_br_min,match_br_max,team1_br_min,"
                                "team2_br_max FROM battle.matches "
                                "WHERE sid='dddddddddddddddd'").fetchone()
    assert tuple(battle) == (2.7, 2.7, 2.7, 2.7)
    assert connection.execute("SELECT COUNT(*) FROM samples WHERE sid='dddddddddddddddd' "
                              "AND squad_br=2.7 AND match_br=2.7").fetchone()[0] == 2
legacy_bot_sid = "5656565656565656"
legacy_human_sid = "6767676767676767"
with server.db() as connection:
    for sid in (legacy_bot_sid, legacy_human_sid):
        connection.execute(
            "INSERT INTO battle.matches(sid,mode,match_br_min,match_br_max,"
            "team2_br_min,team2_br_max,team2_average_squad_br) "
            "VALUES(?,'air',2.7,2.7,2.7,2.7,2.7)", (sid,))
        connection.execute(
            "INSERT INTO samples(sid,uid,player_name,mode,team) "
            "VALUES(?,?,'human','air',2)", (sid, sid[:8]))
    connection.execute(
        "INSERT INTO samples(sid,uid,player_name,mode,team) "
        "VALUES(?,'987654321','human','air',1)", (legacy_human_sid,))
assert server.fill_saved_bot_team_brs() == 1
with server.db() as connection:
    assert tuple(connection.execute(
        "SELECT team1_br_min,team1_br_max,team1_average_squad_br "
        "FROM battle.matches WHERE sid=?", (legacy_bot_sid,)).fetchone()) == (2.7, 2.7, 2.7)
    assert connection.execute(
        "SELECT team1_br_min FROM battle.matches WHERE sid=?",
        (legacy_human_sid,)).fetchone()[0] is None
    rows = list(server.battle_csv_rows(connection))
    assert rows[0][-1] == "match_br_range"
    assert "team1_br_range" not in rows[0] and "team2_br_range" not in rows[0]
    assert len(rows[0]) == len(rows[-1])
    assert next(row[-1] for row in rows[1:] if row[0] == legacy_bot_sid) == "[2.7,2.7]"
with server.db() as connection:
    connection.execute("INSERT INTO battle.matches(sid,mode) VALUES('ffffffffffffffff','air')")
    connection.execute("INSERT INTO replays(sid,mode,group_name,status,processed_at) "
                       "VALUES('ffffffffffffffff','realistic','air','done',0)")
    connection.executemany(
        "INSERT INTO samples(sid,uid,mode,team,party_key,configured_vehicles,stats_json) "
        "VALUES('ffffffffffffffff',?,'air',?,?,?,?)",
        [("401", 1, "solo:401", '[{"code":"a-20g"}]', '{"score":300}'),
         ("402", 2, "solo:402", None, '{"score":0}'),
         ("403", 0, None, None, '{"score":0}')])
assert server.remove_unjoined_samples() == 1
assert server.repair_missing_battle_ratings()["repaired"] == 1
with server.db() as connection:
    assert [row[0] for row in connection.execute(
        "SELECT uid FROM samples WHERE sid='ffffffffffffffff' ORDER BY uid")] == ["401", "402"]
    assert tuple(connection.execute(
        "SELECT match_br_min,match_br_max FROM battle.matches "
        "WHERE sid='ffffffffffffffff'").fetchone()) == (2.7, 2.7)
    assert connection.execute("SELECT squad_br FROM samples WHERE uid='402'").fetchone()[0] is None
assert server.repair_missing_battle_ratings()["repaired"] == 0
legacy_sid = "1212121212121212"
with server.db() as connection:
    connection.execute("INSERT INTO battle.matches(sid,mode) VALUES(?,'air')", (legacy_sid,))
    connection.execute("INSERT INTO replays(sid,mode,group_name,last_part,status,processed_at) "
                       "VALUES(?,'realistic','air',3,'done',0)", (legacy_sid,))
    connection.execute("INSERT INTO samples(sid,uid,player_name,mode,team,party_key,stats_json) "
                       "VALUES(?,'4014227','RXDimA','air',1,'solo:4014227',?)",
                       (legacy_sid, json.dumps({"score": 717})))
with patch.object(server, "download_last_part", return_value=Path("0003.wrpl")), \
     patch.object(server, "parse_with_lineup_fallback",
                  return_value={"players": [{**resolved_player, "uid": "4014227",
                                             "configured": [replay_core.vehicle("a-20g")]}]}):
    server.repair_old_missing_lineups()
with server.db() as connection:
    assert connection.execute("SELECT match_br_max FROM battle.matches WHERE sid=?",
                              (legacy_sid,)).fetchone()[0] == 2.7
    assert json.loads(connection.execute(
        "SELECT configured_vehicles FROM samples WHERE sid=?",
        (legacy_sid,)).fetchone()[0])[0]["code"] == "a-20g"
    assert connection.execute("SELECT 1 FROM lineup_repair_attempts WHERE sid=?",
                              (legacy_sid,)).fetchone() is not None
lower_only_sid = "3434343434343434"
with server.db() as connection:
    connection.execute("INSERT INTO battle.matches(sid,mode,match_br_max) "
                       "VALUES(?,'air',2.7)", (lower_only_sid,))
    connection.execute("INSERT INTO replays(sid,mode,group_name,last_part,status,processed_at) "
                       "VALUES(?,'realistic','air',3,'done',0)", (lower_only_sid,))
    connection.execute("INSERT INTO samples(sid,uid,player_name,mode,team,stats_json) "
                       "VALUES(?,'4014227','RXDimA','air',1,?)",
                       (lower_only_sid, json.dumps({"score": 717})))
with patch.object(server, "download_last_part", return_value=Path("0003.wrpl")), \
     patch.object(server, "parse_with_lineup_fallback",
                  return_value={"players": [{**resolved_player, "uid": "4014227",
                                             "configured": [replay_core.vehicle("a-20g")]}]}):
    server.repair_old_missing_lineups()
with server.db() as connection:
    assert tuple(connection.execute(
        "SELECT match_br_min,match_br_max FROM battle.matches WHERE sid=?",
        (lower_only_sid,)).fetchone()) == (2.7, 2.7)
retry_sid = "4545454545454545"
with server.db() as connection:
    connection.execute("INSERT INTO battle.matches(sid,mode) VALUES(?,'air')",
                       (retry_sid,))
    connection.execute("INSERT INTO lineup_repair_attempts "
                       "(sid,checked_at,retryable) VALUES(?,0,0)", (retry_sid,))
    connection.execute("DELETE FROM settings WHERE key='team_interval_repair_v3'")
server.init_db()
with server.db() as connection:
    assert connection.execute("SELECT 1 FROM lineup_repair_attempts WHERE sid=?",
                              (retry_sid,)).fetchone() is None
official_items = [
    {"sessionIdHex": "1010101010101010", "startTime": 1791840000,
     "gameMode": "realistic", "gameType": "randomBattle",
     "statisticGroup": "aircraft", "isCompleted": True},
    {"sessionIdHex": "2020202020202020", "startTime": 1791840001,
     "gameMode": "realistic", "gameType": "randomBattle",
     "statisticGroup": "tank", "isCompleted": True},
    {"sessionIdHex": "3030303030303030", "startTime": 1791840002,
     "gameMode": "arcade", "gameType": "randomBattle",
     "statisticGroup": "aircraft", "isCompleted": True}]
with patch.object(server, "process_replay"):
    server.process_page(official_items)
    server.process_page(official_items)
with server.db() as connection:
    connection.execute("INSERT INTO replays(sid,group_name,status,processed_at) "
                       "VALUES('1010101010101010','air','done',0)")
    connection.execute("INSERT INTO samples(sid,uid,mode) "
                       "VALUES('1010101010101010','77','air')")
server.save_setting("official_index_complete", 1)
server.save_setting("capture_mode", "air")
server.save_setting("capture_modes_seen", "air")
coverage = server.coverage_data()
assert coverage["official"] == coverage["collected"] == 1
assert coverage["ratio"] == 1 and coverage["modes"] == ["air"]
server.update_settings(server.Settings(concurrency=2, keep_files=100,
                                       capture_mode="ground"))
coverage = server.coverage_data()
assert coverage["modes"] == ["air", "ground"]
assert coverage["official"] == 2 and coverage["collected"] == 1
assert coverage["ratio"] == 0.5 and coverage["complete"]
with patch.object(zipfile, "ZIP64_LIMIT", 128):
    response = server.export_all_zip()
    with zipfile.ZipFile(response.path) as archive:
        names = archive.namelist()
        assert len(names) == 2
        players = archive.read(next(name for name in names if "players" in name))
        assert len(players) > 128 and b"session_id" in players
    Path(response.path).unlink()
"""
            environment = os.environ.copy()
            environment.pop("PYTHONPATH", None)
            try:
                completed = subprocess.run(
                    [sys.executable, "-c", script], cwd=isolated, env=environment,
                    text=True, capture_output=True, timeout=15,
                )
            except subprocess.TimeoutExpired as error:
                self.fail(f"isolated process timed out: {error.stdout!r} {error.stderr!r}")
            self.assertEqual(completed.returncode, 0, completed.stderr)


if __name__ == "__main__":
    unittest.main()
