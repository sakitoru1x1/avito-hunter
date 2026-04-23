"""NotificationService - Telegram + звук + кеш картинок для TG."""
import os
import sys
import threading
import time
from datetime import datetime

import requests

from telegram import TelegramNotifier
from utils import sanitize_error_for_telegram
from logger_setup import logger


class NotificationService:
    """Одно место для всех уведомлений. Thread-safe кеш картинок."""

    def __init__(self, log):
        self.log = log
        self._notifier = TelegramNotifier()
        self._img_cache = {}
        self._img_cache_order = []
        self._img_cache_max = 256
        self._img_cache_lock = threading.Lock()

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

    def test_connection(self, token, chat_id, proxies=None):
        notifier = TelegramNotifier(token, chat_id, proxies=proxies)
        return notifier, notifier.test_connection()

    # ---------- Отправка ----------
    def send_status(self, text, status_enabled=True):
        if not status_enabled or not self._notifier.enabled:
            return False
        return self._notifier.send_message(text)

    def send_error(self, error_text):
        if not self._notifier.enabled:
            return False
        error_text = sanitize_error_for_telegram(error_text)
        if len(error_text) > 3500:
            error_text = error_text[:3500] + "..."
        msg = f"<b>❌ Ошибка в программе</b>\n<pre>{error_text}</pre>"
        return self._notifier.send_message(msg)

    def send_raw(self, text):
        """Прямая отправка текста (для тестов и ручных вызовов)."""
        if not self._notifier.enabled:
            return False
        return self._notifier.send_message(text)

    def send_new_items(self, new_items):
        """Шлёт пачку новых объявлений из кэша.

        Фото берутся ТОЛЬКО из _img_cache (GUI уже скачал). Никаких сетевых
        запросов к Avito CDN из этого метода. Если фото нет в кэше - шлём
        текстом без фото, лучше так чем зависнуть.

        Между отправками - умная пауза: 3с минимум между API-вызовами,
        но если send_photo через VPN уже заняло 5с, дополнительный sleep
        не нужен.
        """
        if not self._notifier.enabled or not new_items:
            return

        TG_MIN_INTERVAL = 1.2
        CAPTION_LIMIT = 1024

        new_items = sorted(new_items, key=lambda x: x.get("pub_date_timestamp", 0) or 0)

        self._notifier.send_message(
            f"<b>🔔 Найдено новых объявлений: {len(new_items)}</b>"
        )
        last_send = time.monotonic()

        def _esc(s):
            return (str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))

        def _smart_sleep():
            nonlocal last_send
            elapsed = time.monotonic() - last_send
            remaining = TG_MIN_INTERVAL - elapsed
            if remaining > 0:
                time.sleep(remaining)
            last_send = time.monotonic()

        sent_with_photo = 0
        sent_text_only = 0

        for item in new_items:
            _smart_sleep()

            title = item.get('title', '') or ''
            link = item.get('link', '') or ''
            if title and link and link != 'Н/Д' and link.startswith('http'):
                header = f"📌 <a href='{_esc(link)}'>{_esc(title)}</a>\n"
            elif title:
                header = f"📌 <b>{_esc(title)}</b>\n"
            else:
                header = ""
            header += f"💰 {_esc(item['price'])} руб.\n"
            if link and link != 'Н/Д' and link.startswith('http'):
                header += f"🔗 {_esc(link)}\n"
            pub_ts = item.get("pub_date_timestamp", 0) or 0
            if pub_ts > 0:
                pub_str = datetime.fromtimestamp(pub_ts).strftime("%d.%m.%Y %H:%M")
            else:
                pub_str = item.get("date", "Н/Д")
            header += f"🕐 На Авито: {_esc(pub_str)}\n"
            header += f"📥 В программе: {_esc(item.get('first_seen', 'Н/Д'))}"

            desc = item.get('description', '') or ''
            caption = header
            if desc and desc != "Н/Д":
                budget = CAPTION_LIMIT - len(header) - len("\n\n")
                if budget > 40:
                    desc_text = desc
                    if len(desc_text) > budget:
                        desc_text = desc_text[: budget - 3] + "..."
                    caption = header + "\n\n" + _esc(desc_text)

            img = item.get('image_url')
            photo_bytes = None
            if img and img != "Н/Д" and img.startswith("http"):
                photo_bytes = self.get_cached_bytes(img)

            if photo_bytes:
                self._notifier.send_photo(caption=caption, photo_bytes=photo_bytes)
                sent_with_photo += 1
            else:
                if len(caption) > 4000:
                    caption = caption[:4000] + "..."
                self._notifier.send_message(caption)
                sent_text_only += 1

        self.log(f"📨 TG: отправлено {sent_with_photo} с фото, {sent_text_only} текстом")

    def send_disappeared(self, disappeared):
        if not self._notifier.enabled or not disappeared:
            return
        self.log(f"🗑️ Пропало объявлений: {len(disappeared)}")
        MAX_LEN = 4000
        header = f"<b>🗑️ Объявления сняты: {len(disappeared)}</b>\n\n"
        current_msg = header
        messages = []
        for item in disappeared:
            price = item.get("price")
            price_str = f"{price} руб." if price else "цена не указана"
            block = f"• <s>{item.get('title', 'Н/Д')}</s> - было {price_str}\n\n"
            if len(current_msg) + len(block) > MAX_LEN:
                messages.append(current_msg)
                current_msg = "🔹 Продолжение:\n\n" + block
            else:
                current_msg += block
        if current_msg:
            messages.append(current_msg)
        for msg in messages:
            self._notifier.send_message(msg)

    # ---------- Картинки ----------
    def get_cached_bytes(self, image_url):
        """Возвращает байты картинки ТОЛЬКО из кэша. Никогда не лезет в сеть."""
        with self._img_cache_lock:
            return self._img_cache.get(image_url)

    def has_cached(self, image_url):
        """True если картинка уже в кэше."""
        with self._img_cache_lock:
            return image_url in self._img_cache

    def fetch_image_bytes(self, session, image_url, max_attempts=3):
        """Кэшированное скачивание картинки. Используется и для TG, и для UI."""
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
