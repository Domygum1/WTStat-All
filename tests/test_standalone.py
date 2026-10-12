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
import sqlite3
from pathlib import Path

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
assert server.is_real_player("123", "notrobot_1")
assert not server.is_real_player("-12", "player")
assert not server.is_real_player("0", "player")
assert not server.is_real_player("123", "COOP/bot512")
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
