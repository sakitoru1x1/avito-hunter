"""NotificationService - Telegram + звук + кеш картинок.

Синхронный режим: отправки идут из того же треда, где вызывается
enqueue_*. UI и save_data уже успели отработать до этого (см. run_parser),
так что даже если TG висит - история программы не теряется.

Без очередей, без фоновых потоков. Публичный API сохранён из предыдущей
асинхронной версии, чтобы gui.py не переписывать.
"""
import base64
import os
import sys
import threading
import time
from datetime import datetime

import requests

from telegram import TelegramNotifier
from utils import sanitize_error_for_telegram
from logger_setup import logger


# Пауза между успешными отправками - TG лимит ~1 msg/sec на чат.
# Меньше - ловим 429 flood_control.
_PACE_AFTER_SUCCESS = 0.7

# Порог, при котором описание ещё влезает в caption фото (TG-лимит 1024).
_DESC_IN_CAPTION_LIMIT = 700

# Таймауты HTTP.
_SEND_TIMEOUT = 15
_PHOTO_TIMEOUT = 30

# Максимум ретраев при 429 на один ряд. Больше не нужно - пользователь
# явно попросил "упало так упало".
_MAX_429_RETRIES = 2


def _esc(s):
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


class NotificationService:
    """Фасад над TelegramNotifier + кеш картинок. Все отправки синхронны."""

    def __init__(self, log):
        self.log = log
        self._notifier = TelegramNotifier()
        self._img_cache = {}
        self._img_cache_order = []
        self._img_cache_max = 256
        self._img_cache_lock = threading.Lock()
        self._last_send_ok = True  # для индикатора в UI

    # ---------- Конфигурация ----------
    def configure(self, token, chat_id, proxies=None):
        """Пересоздаёт TelegramNotifier. Возвращает True если включён."""
        self._notifier = TelegramNotifier(token, chat_id, proxies=proxies)
        return self._notifier.enabled

    @property
    def enabled(self):
        return self._notifier.enabled

    @property
    def notifier(self):
        return self._notifier

    @property
    def tg_online(self):
        return self._last_send_ok if self._notifier.enabled else False

    def test_connection(self, token, chat_id, proxies=None):
        notifier = TelegramNotifier(token, chat_id, proxies=proxies)
        return notifier, notifier.test_connection()

    # ---------- Фоновые потоки - no-op в sync-режиме ----------
    def start_background(self):
        pass

    def stop_background(self, timeout=3):
        pass

    # ---------- Статистика для UI ----------
    def get_stats(self):
        return {
            "pending": 0,
            "online": self.tg_online,
            "enabled": self._notifier.enabled,
        }

    # ---------- Публичные методы отправки ----------
    # Имена enqueue_* сохранены ради совместимости с gui.py, но отправка синхронная.

    def send_status(self, text, status_enabled=True):
        return self.enqueue_status(text, status_enabled=status_enabled)

    def send_error(self, error_text):
        return self.enqueue_error(error_text)

    def send_raw(self, text):
        return self.enqueue_raw(text)

    def enqueue_status(self, text, status_enabled=True):
        if not status_enabled or not text or not self._notifier.enabled:
            return False
        return self._send_text_retrying(text)[0]

    def enqueue_error(self, error_text):
        if not error_text or not self._notifier.enabled:
            return False
        clean = sanitize_error_for_telegram(error_text)
        if len(clean) > 3500:
            clean = clean[:3500] + "..."
        msg = f"<b>❌ Ошибка в программе</b>\n<pre>{clean}</pre>"
        return self._send_text_retrying(msg)[0]

    def enqueue_raw(self, text):
        if not text or not self._notifier.enabled:
            return False
        return self._send_text_retrying(text)[0]

    def enqueue_new_items(self, new_items, img_session):
        """Шлёт заголовок + по сообщению на каждое новое объявление.

        Качает картинки синхронно через Selenium-сессию (куки Avito иначе
        в другом треде не достать). Блокирует вызывающий тред до конца
        отправки - это и есть "как раньше, только встроено в новую архитектуру".
        """
        if not new_items or not self._notifier.enabled:
            return 0
        items = sorted(new_items, key=lambda x: x.get("pub_date_timestamp", 0) or 0)
        sent = 0
        ok, _ = self._send_text_retrying(
            f"<b>🔔 Найдено новых объявлений: {len(items)}</b>"
        )
        if ok:
            sent += 1
            self._pace()

        for item in items:
            desc = item.get("description") or ""
            if desc == "Н/Д":
                desc = ""
            fold_in_caption = bool(desc) and len(desc) <= _DESC_IN_CAPTION_LIMIT

            caption = self._build_new_item_caption(item, desc if fold_in_caption else "")
            img_b64 = None
            img_url = item.get("image_url")
            if img_session and img_url and img_url != "Н/Д" and img_url.startswith("http"):
                data = self.fetch_image_bytes(img_session, img_url, max_attempts=2)
                if data:
                    img_b64 = base64.b64encode(data).decode("ascii")

            ok, _ = self._send_item_with_photo(caption, img_b64)
            if ok:
                sent += 1
                self._pace()

            if desc and not fold_in_caption:
                ok2, _ = self._send_description(item.get("title") or "", desc)
                if ok2:
                    sent += 1
                    self._pace()
        return sent

    def enqueue_disappeared(self, disappeared):
        """Шлёт пачкой в одном или нескольких сообщениях (4096-char лимит TG)."""
        if not disappeared or not self._notifier.enabled:
            return 0
        count = len(disappeared)
        self.log(f"🗑️ TG: шлю пачку 'исчезли' на {count} объявлений")
        MAX_LEN = 4000
        header = f"<b>🗑️ Объявления сняты: {count}</b>\n\n"
        current = header
        parts = []
        for it in disappeared:
            price = it.get("price")
            price_str = f"{price} руб." if price else "цена не указана"
            block = f"• <s>{_esc(it.get('title', 'Н/Д'))}</s> - было {price_str}\n\n"
            if len(current) + len(block) > MAX_LEN:
                parts.append(current)
                current = "🔹 Продолжение:\n\n" + block
            else:
                current += block
        if current:
            parts.append(current)

        sent = 0
        for idx, text in enumerate(parts):
            ok, _ = self._send_text_retrying(text)
            if ok:
                sent += 1
                if idx < len(parts) - 1:
                    self._pace()
            else:
                break  # дальше бессмысленно - оборвалось
        return sent

    # ---------- Картинки ----------
    def fetch_image_bytes(self, session, image_url, max_attempts=3):
        """Кэшированное скачивание картинки. Thread-safe."""
        with self._img_cache_lock:
            cached = self._img_cache.get(image_url)
        if cached is not None:
            return cached

        last_err = None
        for attempt in range(max_attempts):
            try:
                resp = session.get(image_url, timeout=20)
                if resp.status_code == 200 and resp.content:
                    data = resp.content
                    with self._img_cache_lock:
                        if image_url not in self._img_cache:
                            self._img_cache[image_url] = data
                            self._img_cache_order.append(image_url)
                            while len(self._img_cache_order) > self._img_cache_max:
                                old = self._img_cache_order.pop(0)
                                self._img_cache.pop(old, None)
                    return data
                last_err = f"HTTP {resp.status_code}"
            except Exception as e:
                last_err = str(e)
                time.sleep(0.5 * (attempt + 1))

        logger.warning(f"Не скачалась картинка {image_url[:80]}: {last_err}")
        return None

    # ---------- Звук ----------
    @staticmethod
    def play_sound():
        try:
            if sys.platform == 'win32':
                import winsound
                winsound.Beep(440, 200)
            elif sys.platform == 'darwin':
                os.system('afplay /System/Library/Sounds/Glass.aiff &')
            else:
                os.system('paplay /usr/share/sounds/freedesktop/stereo/message.oga 2>/dev/null &')
        except Exception:
            print('\a')

    # ---------- Диагностика ----------
    def ping_direct_vs_proxy(self):
        """Пробует getMe напрямую и через прокси. Для UI-кнопки диагностики."""
        out = {"direct": None, "proxy": None}
        if not self._notifier.enabled:
            return out
        url = f"https://api.telegram.org/bot{self._notifier.token}/getMe"

        s = requests.Session()
        s.trust_env = False
        try:
            r = s.get(url, timeout=8)
            out["direct"] = {"ok": r.status_code == 200, "code": r.status_code}
        except Exception as e:
            out["direct"] = {"ok": False, "err": f"{type(e).__name__}: {e}"}

        if self._notifier.proxies:
            try:
                r = s.get(url, timeout=8, proxies=self._notifier.proxies)
                out["proxy"] = {"ok": r.status_code == 200, "code": r.status_code}
            except Exception as e:
                out["proxy"] = {"ok": False, "err": f"{type(e).__name__}: {e}"}
        return out

    # ---------- Внутренние отправки ----------
    def _pace(self):
        time.sleep(_PACE_AFTER_SUCCESS)

    @staticmethod
    def _parse_retry_after(resp):
        try:
            return int(resp.json().get("parameters", {}).get("retry_after", 1))
        except Exception:
            return 1

    def _send_text_once(self, text, parse_mode="HTML"):
        """Одна попытка отправки текста. (ok, err, retry_after)."""
        try:
            url = f"{self._notifier.base_url}/sendMessage"
            resp = self._notifier.session.post(
                url,
                data={
                    "chat_id": self._notifier.chat_id,
                    "text": text,
                    "parse_mode": parse_mode,
                    "disable_web_page_preview": False,
                },
                timeout=_SEND_TIMEOUT,
                proxies=self._notifier.proxies,
            )
            if resp.status_code == 200:
                return True, "", 0
            if resp.status_code == 429:
                return False, "429", self._parse_retry_after(resp)
            return False, f"HTTP {resp.status_code}: {resp.text[:180]}", 0
        except Exception as e:
            return False, f"{type(e).__name__}: {e}", 0

    def _send_photo_once(self, caption, photo_bytes, parse_mode="HTML"):
        """Одна попытка отправки фото. (ok, err, retry_after)."""
        try:
            url = f"{self._notifier.base_url}/sendPhoto"
            data = {"chat_id": self._notifier.chat_id, "parse_mode": parse_mode}
            if caption:
                if len(caption) > 1024:
                    caption = caption[:1020] + "..."
                data["caption"] = caption
            files = {"photo": ("image.jpg", photo_bytes, "image/jpeg")}
            resp = self._notifier.session.post(
                url, data=data, files=files,
                timeout=_PHOTO_TIMEOUT,
                proxies=self._notifier.proxies,
            )
            if resp.status_code == 200:
                return True, "", 0
            if resp.status_code == 429:
                return False, "429", self._parse_retry_after(resp)
            return False, f"HTTP {resp.status_code}: {resp.text[:180]}", 0
        except Exception as e:
            return False, f"{type(e).__name__}: {e}", 0

    def _send_text_retrying(self, text):
        """До _MAX_429_RETRIES попыток при 429. Остальные ошибки - сразу return False."""
        for attempt in range(_MAX_429_RETRIES + 1):
            ok, err, ra = self._send_text_once(text)
            if ok:
                self._last_send_ok = True
                return True, ""
            if ra > 0 and attempt < _MAX_429_RETRIES:
                delay = ra + 1
                self.log(f"📉 TG 429: жду {delay}с и пробую ещё раз")
                time.sleep(delay)
                continue
            self._last_send_ok = False
            self.log(f"📭 TG: отправка не прошла - {err}")
            return False, err
        self._last_send_ok = False
        return False, "max retries"

    def _send_photo_retrying(self, caption, photo_bytes):
        for attempt in range(_MAX_429_RETRIES + 1):
            ok, err, ra = self._send_photo_once(caption, photo_bytes)
            if ok:
                self._last_send_ok = True
                return True, ""
            if ra > 0 and attempt < _MAX_429_RETRIES:
                delay = ra + 1
                self.log(f"📉 TG 429 (photo): жду {delay}с")
                time.sleep(delay)
                continue
            self._last_send_ok = False
            return False, err
        self._last_send_ok = False
        return False, "max retries"

    def _build_new_item_caption(self, item, description):
        caption = f"<a href='{_esc(item.get('link') or '')}'>{_esc(item.get('title') or '')}</a>\n"
        caption += f"💰 {_esc(item.get('price') or '—')} руб.\n"
        pub_ts = item.get("pub_date_timestamp", 0) or 0
        if pub_ts > 0:
            pub_str = datetime.fromtimestamp(pub_ts).strftime("%d.%m.%Y %H:%M")
        else:
            pub_str = item.get("date") or "Н/Д"
        caption += f"🕐 На Авито: {_esc(pub_str)}\n"
        caption += f"📥 В программе: {_esc(item.get('first_seen') or 'Н/Д')}"

        if description and description != "Н/Д":
            candidate = caption + f"\n\n<blockquote>{_esc(description)}</blockquote>"
            if len(candidate) <= 1024:
                caption = candidate
        return caption

    def _send_item_with_photo(self, caption, img_b64):
        """Отправка объявления. Если картинки нет или фото упало непрозрачно -
        пробуем текстом (чтобы уведомление дошло хоть как-то)."""
        if img_b64:
            try:
                data = base64.b64decode(img_b64)
            except Exception:
                data = None
            if data:
                ok, err = self._send_photo_retrying(caption, data)
                if ok:
                    return True, ""
                # Фото сфейлилось по непонятным причинам - пробуем текстом.
                self.log(f"📭 TG: фото не прошло ({err}), шлю текстом")
                return self._send_text_retrying(caption)
        return self._send_text_retrying(caption)

    def _send_description(self, title, description):
        """Отдельное сообщение с описанием - для длинных, не влезших в caption."""
        if not description:
            return True, ""
        if len(description) > 3500:
            description = description[:3500] + "..."
        if title:
            text = f"<b>{_esc(title)}</b>\n<blockquote>{_esc(description)}</blockquote>"
        else:
            text = f"<blockquote>{_esc(description)}</blockquote>"
        return self._send_text_retrying(text)
