"""One-time interactive setup on the PC that runs MySQL Server."""
from __future__ import annotations

import getpass
import ipaddress

import mysql.connector

from mysql_schema import SCHEMA


def main() -> None:
    print("在运行 MySQL Server 的 PC 上执行此程序。输入的密码只用于本次连接。")
    server_port = input("MySQL 端口 [3306]：").strip() or "3306"
    if not server_port.isdecimal() or not 1 <= int(server_port) <= 65535:
        raise ValueError("端口无效。")
    laptop_ip = str(ipaddress.ip_address(input("笔记本的 Radmin VPN IP：").strip()))
    root_password = getpass.getpass("PC 上 MySQL root 密码：")
    collector_password = getpass.getpass("为 wt_collector 设置密码：")
    if not collector_password:
        raise ValueError("采集账号密码不能为空。")
    connection = mysql.connector.connect(host="127.0.0.1", port=int(server_port),
                                         user="root", password=root_password,
                                         connection_timeout=10)
    try:
        cursor = connection.cursor()
        try:
            cursor.execute("CREATE DATABASE IF NOT EXISTS `wtstat_all` CHARACTER SET utf8mb4")
            cursor.execute("USE `wtstat_all`")
            for statement in SCHEMA:
                cursor.execute(statement)
            cursor.execute("SHOW COLUMNS FROM `samples`")
            columns = {row[0] for row in cursor.fetchall()}
            for name, field_type in (("country_code", "CHAR(2)"),
                                     ("country_name", "VARCHAR(64)"),
                                     ("squad_country_codes", "LONGTEXT"),
                                     ("squad_country_names", "LONGTEXT")):
                if name not in columns:
                    cursor.execute(f"ALTER TABLE `samples` ADD COLUMN `{name}` {field_type}")
            cursor.execute("CREATE USER IF NOT EXISTS %s@%s IDENTIFIED BY %s",
                           ("wt_collector", laptop_ip, collector_password))
            cursor.execute("ALTER USER %s@%s IDENTIFIED BY %s",
                           ("wt_collector", laptop_ip, collector_password))
            cursor.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON `wtstat_all`.* TO %s@%s",
                           ("wt_collector", laptop_ip))
            connection.commit()
        finally:
            cursor.close()
    finally:
        connection.close()
    print("已建好 wtstat_all 数据库、三张表和采集账号。")
    print("在笔记本的 mysql_sync.local.json 填 PC 的 Radmin IP、用户 wt_collector 和刚设置的密码。")


if __name__ == "__main__":
    main()
