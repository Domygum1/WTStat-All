import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

import mysql_sync


class FakeCursor:
    def __init__(self, remote):
        self.remote = remote

    def execute(self, sql, params):
        self.remote.statements.append((sql, params))
        if self.remote.fail:
            raise RuntimeError("remote unavailable")

    def executemany(self, sql, rows):
        self.remote.statements.append((sql, rows))

    def close(self):
        pass


class FakeRemote:
    def __init__(self, fail=False):
        self.statements = []
        self.commits = 0
        self.rollbacks = 0
        self.fail = fail

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


class ChangingRemote(FakeRemote):
    def __init__(self, main_db):
        super().__init__()
        self.main_db = main_db

    def commit(self):
        super().commit()
        with closing(sqlite3.connect(self.main_db)) as db:
            db.execute("UPDATE samples SET player_name='newer' WHERE sid='abc' AND uid='123'")
            db.commit()


class MySQLSyncTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent)
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.main = self.root / "main.sqlite3"
        self.battle = self.root / "battle.sqlite3"
        with closing(sqlite3.connect(self.main)) as db:
            db.execute("CREATE TABLE settings(key TEXT PRIMARY KEY,value TEXT)")
            db.execute("CREATE TABLE replays(sid TEXT PRIMARY KEY," +
                       ",".join(f"{name} TEXT" for name in mysql_sync.REPLAY_COLUMNS[1:]) + ")")
            db.execute("CREATE TABLE samples(sid TEXT,uid TEXT," +
                       ",".join(f"{name} TEXT" for name in mysql_sync.SAMPLE_COLUMNS[2:]) +
                       ",PRIMARY KEY(sid,uid))")
            db.execute("INSERT INTO replays(sid,status,processed_at) VALUES('abc','done',1)")
            db.execute("INSERT INTO samples(sid,uid,mode) VALUES('abc','123','air')")
            db.commit()
        with closing(sqlite3.connect(self.battle)) as db:
            db.execute("CREATE TABLE matches(sid TEXT PRIMARY KEY," +
                       ",".join(f"{name} TEXT" for name in mysql_sync.MATCH_COLUMNS[1:]) + ")")
            db.execute("INSERT INTO matches(sid,mode) VALUES('abc','air')")
            db.commit()
        self.config = {"host": "26.48.27.164", "port": 3306,
                       "database": "wtstat_all", "user": "wt_collector", "password": "secret"}

    def test_blank_host_disables_upload(self):
        (self.root / mysql_sync.CONFIG_NAME).write_text(json.dumps({"host": ""}), encoding="utf-8")
        self.assertIsNone(mysql_sync.load_config(self.root))
        mysql_sync.start(self.root, self.main, self.battle)
        self.assertFalse(mysql_sync.status()["enabled"])
        with closing(sqlite3.connect(self.main)) as db:
            tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertNotIn("mysql_sync_outbox", tables)

    def test_backfill_changes_and_acknowledgement(self):
        mysql_sync.install_outbox(self.main, self.battle, self.config)
        self.assertEqual(mysql_sync._pending_count(self.main, self.battle), 2)
        remote = FakeRemote()
        self.assertEqual(mysql_sync.sync_batch(self.main, self.battle, remote), 1)
        self.assertEqual(remote.commits, 1)
        self.assertEqual(mysql_sync._pending_count(self.main, self.battle), 0)
        mysql_sync.install_outbox(self.main, self.battle, self.config)
        self.assertEqual(mysql_sync._pending_count(self.main, self.battle), 0)
        with closing(sqlite3.connect(self.main)) as db:
            db.execute("UPDATE samples SET player_name='changed' WHERE sid='abc' AND uid='123'")
            db.commit()
        self.assertEqual(mysql_sync._pending_count(self.main, self.battle), 1)
        self.assertEqual(mysql_sync.sync_batch(self.main, self.battle, remote), 1)
        self.assertEqual(mysql_sync._pending_count(self.main, self.battle), 0)
        with closing(sqlite3.connect(self.main)) as db:
            db.execute("DELETE FROM samples WHERE sid='abc' AND uid='123'")
            db.commit()
        self.assertEqual(mysql_sync.sync_batch(self.main, self.battle, remote), 1)
        self.assertEqual(mysql_sync._pending_count(self.main, self.battle), 0)

    def test_remote_failure_keeps_outbox(self):
        mysql_sync.install_outbox(self.main, self.battle, self.config)
        remote = FakeRemote(fail=True)
        with self.assertRaisesRegex(RuntimeError, "remote unavailable"):
            mysql_sync.sync_batch(self.main, self.battle, remote)
        self.assertEqual(remote.rollbacks, 1)
        self.assertEqual(mysql_sync._pending_count(self.main, self.battle), 2)

    def test_change_during_upload_remains_queued(self):
        mysql_sync.install_outbox(self.main, self.battle, self.config)
        remote = ChangingRemote(self.main)
        self.assertEqual(mysql_sync.sync_batch(self.main, self.battle, remote), 1)
        self.assertEqual(mysql_sync._pending_count(self.main, self.battle), 1)


if __name__ == "__main__":
    unittest.main()
