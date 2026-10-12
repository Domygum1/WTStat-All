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
