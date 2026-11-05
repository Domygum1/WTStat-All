"""Install the optional MySQL driver only when remote upload is configured."""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from mysql_sync import load_config


def main() -> None:
    root = Path(__file__).resolve().parent
    try:
        enabled = load_config(root) is not None
    except (OSError, ValueError) as error:
        print(f"MySQL 同步配置无效，本地采集仍可运行：{error}")
        return
    if not enabled:
        return
    try:
        import mysql.connector
        if int(mysql.connector.__version__.split(".", 1)[0]) >= 26:
            return
    except (ImportError, AttributeError, ValueError):
        pass
    print("正在安装可选的 MySQL Python 驱动…")
    result = subprocess.run([sys.executable, "-m", "pip", "install", "-r",
                             str(root / "requirements-mysql.txt")], check=False)
    if result.returncode:
        print("MySQL 驱动安装失败；本地采集仍可运行，下次启动会重试。")


if __name__ == "__main__":
    main()
