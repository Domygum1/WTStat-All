"""Refresh WTStat all's War Thunder cookie using an isolated Edge profile."""
from __future__ import annotations

import json
import os
import re
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
LOGIN_CONFIG = ROOT / "login.local.json"
COOKIE_CONFIG = ROOT / "config.local.json"
PROFILE = ROOT / "work" / "gaijin_edge_profile"
REPLAY_URL = "https://warthunder.com/en/tournament/replay/"
RETRY_SECONDS = 600
_LOCK = threading.Lock()
_last_attempt = 0.0
_last_forced_attempt = 0.0


class LoginError(RuntimeError):
    """A safe-to-display login failure without account details."""


def configured() -> bool:
    return LOGIN_CONFIG.is_file()


def _credentials() -> tuple[str, str]:
    try:
        raw = json.loads(LOGIN_CONFIG.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise LoginError("无法读取 login.local.json。") from error
    if not isinstance(raw, dict):
        raise LoginError("login.local.json 格式无效。")
    email, password = raw.get("email"), raw.get("password")
    if not isinstance(email, str) or not email.strip() or not isinstance(password, str) or not password:
        raise LoginError("请在 login.local.json 填写小号的邮箱和密码。")
    return email.strip(), password


def _site_cookie(context) -> str | None:
    for entry in context.cookies(["https://warthunder.com"]):
        if entry["name"] == "identity_sid" and entry.get("value"):
            return entry["value"]
    return None


def _save_cookie(value: str) -> None:
    try:
        data = json.loads(COOKIE_CONFIG.read_text(encoding="utf-8")) if COOKIE_CONFIG.exists() else {}
        if not isinstance(data, dict):
            data = {}
    except (OSError, ValueError):
        data = {}
    data["identity_sid"] = value
    temporary = COOKIE_CONFIG.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(data, ensure_ascii=False) + "\n", encoding="utf-8")
    os.replace(temporary, COOKIE_CONFIG)


def refresh_cookie(*, interactive: bool = False, force: bool = False) -> str:
    """Login only after cookie expiry; never automate CAPTCHA or 2FA."""
    global _last_attempt, _last_forced_attempt
    with _LOCK:
        now = time.monotonic()
        allow_forced_retry = force and _last_forced_attempt < _last_attempt
        if not interactive and now - _last_attempt < RETRY_SECONDS and not allow_forced_retry:
            raise LoginError("自动登录暂时失败，10 分钟后才会重试。可运行 python gaijin_login.py 手动完成验证。")
        email, password = _credentials()
        _last_attempt = now
        if force:
            _last_forced_attempt = now
        try:
            from playwright.sync_api import sync_playwright
        except ImportError as error:
            raise LoginError("缺少 Playwright。请重新运行 start.bat 安装依赖。") from error

        PROFILE.parent.mkdir(parents=True, exist_ok=True)
        try:
            with sync_playwright() as playwright:
                context = playwright.chromium.launch_persistent_context(
                    str(PROFILE), channel="msedge", headless=not interactive,
                    accept_downloads=False, timeout=30000)
                try:
                    if force:
                        # A persistent profile can retain the same expired identity_sid.
                        context.clear_cookies()
                    page = context.new_page()
                    page.goto(REPLAY_URL, wait_until="domcontentloaded", timeout=45000)
                    value = _site_cookie(context)
                    if value and page.url.startswith("https://warthunder.com/"):
                        _save_cookie(value)
                        return value

                    email_field = page.locator('input[placeholder*="mail" i]').first
                    password_field = page.locator('input[type="password"]').first
                    email_field.wait_for(timeout=15000)
                    password_field.wait_for(timeout=15000)
                    email_field.fill(email)
                    password_field.fill(password)
                    page.get_by_role("button", name=re.compile(r"Log in|Sign in", re.I)).first.click()

                    deadline = time.monotonic() + (180 if interactive else 45)
                    while time.monotonic() < deadline:
                        value = _site_cookie(context)
                        if value and page.url.startswith("https://warthunder.com/"):
                            _save_cookie(value)
                            return value
                        page.wait_for_timeout(1000)
                    raise LoginError("未取得网站登录 Cookie。若出现验证码或两步验证，请运行 python gaijin_login.py 完成一次人工验证。")
                finally:
                    context.close()
        except LoginError:
            raise
        except Exception as error:
            raise LoginError(f"自动登录失败：{type(error).__name__}。请检查 Edge 是否已安装、网络是否正常。") from error


if __name__ == "__main__":
    try:
        refresh_cookie(interactive=True)
        print("登录成功，WTStat all 的 Cookie 已更新。")
    except LoginError as error:
        raise SystemExit(str(error)) from error
