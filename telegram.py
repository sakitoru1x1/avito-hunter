import os
import json
import requests

from config import SETTINGS_FILE
from utils import sanitize_error_for_telegram
from logger_setup import logger


def build_proxies_dict(settings):
    """Строит словарь proxies для requests из полей tg_proxy_* настроек.

    Возвращает None, если не хватает хоста/порта.
    """
    scheme = (settings.get("tg_proxy_scheme") or "").strip()
    host = (settings.get("tg_proxy_host") or "").strip()
    port = (settings.get("tg_proxy_port") or "").strip()
    user = (settings.get("tg_proxy_user") or "").strip()
    pwd = (settings.get("tg_proxy_pass") or "").strip()
    if not host or not port:
        return None
    if not scheme:
        scheme = "http"
    if user and pwd:
        proxy_url = f"{scheme}://{user}:{pwd}@{host}:{port}"
    else:
        proxy_url = f"{scheme}://{host}:{port}"
    return {"http": proxy_url, "https": proxy_url}


class TelegramNotifier:
    def __init__(self, token=None, chat_id=None, proxies=None):
        self.token = token
        self.chat_id = chat_id
        self.enabled = bool(token and chat_id)
        self.base_url = f"https://api.telegram.org/bot{token}" if token else ""
        self.proxies = proxies

    def send_message(self, text, parse_mode='HTML'):
        if not self.enabled:
            return False
        try:
            url = f"{self.base_url}/sendMessage"
            payload = {
                'chat_id': self.chat_id,
                'text': text,
                'parse_mode': parse_mode,
                'disable_web_page_preview': False
            }
            response = requests.post(url, data=payload, timeout=10, proxies=self.proxies)
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Ошибка отправки Telegram: {e}")
            return False

    def test_connection(self):
        if not self.token:
            return False, "Токен не указан"
        try:
            url = f"https://api.telegram.org/bot{self.token}/getMe"
            response = requests.get(url, timeout=10, proxies=self.proxies)
            if response.status_code == 200:
                return True, "Подключение успешно"
            else:
                return False, f"Ошибка {response.status_code}: {response.text}"
        except Exception as e:
            return False, str(e)


def send_crash_report_to_telegram(error_text):
    """Отправляет критическую ошибку в Telegram (используется глобальным обработчиком)."""
    if not os.path.exists(SETTINGS_FILE):
        return
    try:
        with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
            settings = json.load(f)
        token = settings.get("telegram_token")
        chat_id = settings.get("telegram_chat_id")
        if not token or not chat_id:
            return
        proxies = build_proxies_dict(settings)
        error_text = sanitize_error_for_telegram(error_text)
        if len(error_text) > 3500:
            error_text = error_text[:3500] + "\n... (обрезано)"
        message = f"⚠️ *Критическая ошибка в парсере Avito*\n```\n{error_text}\n```"
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data={'chat_id': chat_id, 'text': message, 'parse_mode': 'Markdown'},
            timeout=10,
            proxies=proxies,
        )
    except Exception as e:
        logger.error(f"Не удалось отправить краш-репорт: {e}")
