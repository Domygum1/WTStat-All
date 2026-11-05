"""Requeue all local rows after replacing or clearing the remote database."""
from __future__ import annotations

import sqlite3
from contextlib import closing
from pathlib import Path

import mysql_sync


def main() -> None:
    root = Path(__file__).resolve().parent
    config = mysql_sync.load_config(root)
    if config is None:
        raise SystemExit("先在 mysql_sync.local.json 填写 PC 的 Radmin VPN IP。")
    main_db = root / "data" / "wtstat_all.sqlite3"
    battle_db = root / "data" / "battles.sqlite3"
    if not main_db.is_file() or not battle_db.is_file():
        raise SystemExit("未找到本机采集数据库。")
    with closing(sqlite3.connect(main_db)) as connection:
        connection.execute("DELETE FROM settings WHERE key='mysql_sync_target_v1'")
        connection.commit()
    mysql_sync.install_outbox(main_db, battle_db, config)
    print("本机数据已重新排队。启动 start.bat 后会逐步补传。")


if __name__ == "__main__":
    main()
