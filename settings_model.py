"""AppSettings dataclass + чтение/запись JSON. Pure - без Tk."""
import json
import os
from dataclasses import dataclass, field, asdict
from typing import List, Optional

from config import DEFAULT_MAX_ITEMS
from logger_setup import logger


@dataclass
class AppSettings:
    telegram_token: str = ""
    telegram_chat_id: str = ""

    proxy_scheme: str = "http"
    proxy_host: str = ""
    proxy_port: str = ""
    proxy_user: str = ""
    proxy_pass: str = ""

    tg_proxy_scheme: str = "http"
    tg_proxy_host: str = ""
    tg_proxy_port: str = ""
    tg_proxy_user: str = ""
    tg_proxy_pass: str = ""

    tg_notify_status: bool = True

    schedule_enabled: bool = False
    schedule_start: str = "09:00"
    schedule_end: str = "21:00"
    schedule_days: List[bool] = field(default_factory=lambda: [True] * 7)

    max_items: int = DEFAULT_MAX_ITEMS
    show_browser: bool = False

    @classmethod
    def from_dict(cls, raw: dict) -> "AppSettings":
        s = cls()
        s.telegram_token = raw.get("telegram_token", s.telegram_token)
        s.telegram_chat_id = raw.get("telegram_chat_id", s.telegram_chat_id)

        s.proxy_scheme = raw.get("proxy_scheme", s.proxy_scheme)
        s.proxy_host = raw.get("proxy_host", s.proxy_host)
        s.proxy_port = raw.get("proxy_port", s.proxy_port)
        s.proxy_user = raw.get("proxy_user", s.proxy_user)
        s.proxy_pass = raw.get("proxy_pass", s.proxy_pass)

        s.tg_proxy_scheme = raw.get("tg_proxy_scheme", s.tg_proxy_scheme)
        s.tg_proxy_host = raw.get("tg_proxy_host", s.tg_proxy_host)
        s.tg_proxy_port = raw.get("tg_proxy_port", s.tg_proxy_port)
        s.tg_proxy_user = raw.get("tg_proxy_user", s.tg_proxy_user)
        s.tg_proxy_pass = raw.get("tg_proxy_pass", s.tg_proxy_pass)

        s.tg_notify_status = bool(raw.get("tg_notify_status", s.tg_notify_status))

        s.schedule_enabled = bool(raw.get("schedule_enabled", s.schedule_enabled))
        s.schedule_start = raw.get("schedule_start", s.schedule_start)
        s.schedule_end = raw.get("schedule_end", s.schedule_end)
        days = raw.get("schedule_days")
        if isinstance(days, list) and len(days) == 7:
            s.schedule_days = [bool(v) for v in days]

        try:
            saved_max = int(raw.get("max_items", s.max_items))
            if saved_max <= 50:
                saved_max = DEFAULT_MAX_ITEMS
            s.max_items = saved_max
        except (TypeError, ValueError):
            pass

        s.show_browser = bool(raw.get("show_browser", s.show_browser))
        return s

    def to_dict(self) -> dict:
        return asdict(self)

    # ---------- Helpers ----------
    def avito_proxy_dict(self) -> Optional[dict]:
        return _build_proxy_dict(
            self.proxy_scheme, self.proxy_host, self.proxy_port,
            self.proxy_user, self.proxy_pass,
        )

    def tg_proxy_dict(self) -> Optional[dict]:
        return _build_proxy_dict(
            self.tg_proxy_scheme, self.tg_proxy_host, self.tg_proxy_port,
            self.tg_proxy_user, self.tg_proxy_pass,
        )


def _build_proxy_dict(scheme, host, port, user, pwd) -> Optional[dict]:
    host = (host or "").strip()
    port = (port or "").strip()
    if not host or not port:
        return None
    user = (user or "").strip()
    pwd = (pwd or "").strip()
    if user and pwd:
        url = f"{scheme}://{user}:{pwd}@{host}:{port}"
    else:
        url = f"{scheme}://{host}:{port}"
    return {"http": url, "https": url}


def load_settings(path) -> AppSettings:
    """Читает JSON. Возвращает AppSettings (с дефолтами если файла нет/битый)."""
    if not os.path.exists(path):
        return AppSettings()
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        return AppSettings.from_dict(raw)
    except Exception as e:
        logger.warning(f"Ошибка чтения {path}: {e}")
        return AppSettings()


def save_settings(settings: AppSettings, path) -> bool:
    """Пишет JSON. Возвращает True при успехе."""
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(settings.to_dict(), f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        logger.error(f"Ошибка записи {path}: {e}")
        return False
