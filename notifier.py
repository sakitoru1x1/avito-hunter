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


_SHOP_KEYWORDS = (
    "магазин", "shop", "store", "ооо", "ип ", "маркет", "market",
    "trade", "трейд", "салон", "центр", "компания", "group", "груп", "оптом",
    "ltd", "inc", "торг", "сервис", "service", "студия", "studio",
    "дисконт", "outlet", "склад", "warehouse", "бутик", "boutique",
    "plaza", "плаза", "базар", "mall", "молл", "ритейл", "retail",
)

_COMMERCIAL_KEYWORDS = (
    "трейд-ин", "трейд ин", "trade-in", "trade in",
    "кредит", "рассрочк", "обмен вашего", "обмен старого",
    "выкуп", "скупка", "принимаем старые", "сдай старый",
)


def _classify_seller(item):
    name = (item.get("seller_name") or "").lower()
    ads = item.get("seller_ads")
    reviews = item.get("seller_reviews")
    since = item.get("seller_since")
    desc = (item.get("description") or "").lower()

    if ads is None and reviews is None and not since and not name:
        return None

    ads = ads or 0
    reviews = reviews or 0
    has_commercial = any(kw in desc for kw in _COMMERCIAL_KEYWORDS)

    rating = item.get("seller_rating")
    ratings_count = int(rating) if rating and rating > 0 else 0

    if name in ("пользователь", "") and ads <= 3:
        return "👤 частник"
    if any(kw in name for kw in _SHOP_KEYWORDS):
        return "🏪 магазин"
    if ads >= 50:
        return "🏪 магазин"
    if ads >= 20 and reviews >= 30 and has_commercial:
        return "🏪 магазин"
    if ads >= 20 and has_commercial:
        return "🏬 скорее магазин или перекупщик"
    if ads >= 20:
        return "🏬 скорее магазин или перекупщик"
    if ads >= 10 and has_commercial:
        return "🏬 скорее магазин или перекупщик"
    if ads >= 10:
        return "🏬 скорее магазин или перекупщик"
    if ads >= 5 and has_commercial:
        return "🏬 скорее магазин или перекупщик"
    if reviews >= 30 or ratings_count >= 20:
        return "🏬 скорее магазин или перекупщик"

    return "👤 частник"


class NotificationService:
    """Одно место для всех уведомлений. Thread-safe кеш картинок."""

    def __init__(self, log):
        self.log = log
        self._notifier = TelegramNotifier()
        self._img_cache = {}
        self._img_cache_order = []
        self._img_cache_max = 256
        self._img_cache_lock = threading.Lock()
        self._last_error_msg = None
        self._last_error_time = 0
        self._error_cooldown = 300

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
        now = time.monotonic()
        if error_text == self._last_error_msg and (now - self._last_error_time) < self._error_cooldown:
            return False
        self._last_error_msg = error_text
        self._last_error_time = now
        error_text = sanitize_error_for_telegram(error_text)
        if len(error_text) > 500:
            error_text = error_text[:500] + "..."
        return self._notifier.send_message(f"<b>⚠️ {error_text}</b>")

    def send_raw(self, text):
        """Прямая отправка текста (для тестов и ручных вызовов)."""
        if not self._notifier.enabled:
            return False
        return self._notifier.send_message(text)

    def send_new_items(self, new_items, fast=False):
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

        TG_MIN_INTERVAL = 1.0 if fast else 1.2
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
            if title:
                header = f"📌 <b>{_esc(title)}</b>\n"
            else:
                header = ""
            header += f"💰 {_esc(item['price'])} руб.\n"
            if link and link != 'Н/Д' and link.startswith('http'):
                header += f"🔗 <a href='{_esc(link)}'>Ссылка</a>\n"
            pub_ts = item.get("pub_date_timestamp", 0) or 0
            if pub_ts > 0:
                pub_str = datetime.fromtimestamp(pub_ts).strftime("%d.%m.%Y %H:%M")
            else:
                pub_str = item.get("date", "Н/Д")
            header += f"🕐 {_esc(pub_str)}\n"

            seller_parts = []
            sn = item.get("seller_name")
            if sn:
                seller_parts.append(_esc(sn))
            sr = item.get("seller_rating")
            if sr:
                rev = item.get("seller_reviews")
                seller_parts.append(f"★{sr}" + (f"({rev})" if rev else ""))
            ss = item.get("seller_since")
            if ss:
                seller_parts.append(f"с {ss}")
            sa = item.get("seller_ads")
            if sa:
                seller_parts.append(f"{sa} объявл.")
            if seller_parts:
                header += f"👤 {' · '.join(seller_parts)}\n"

            seller_label = _classify_seller(item)
            if seller_label:
                header += f"{seller_label}\n"

            loc = item.get("location")
            vc = item.get("view_count")
            if loc or vc:
                loc_parts = []
                if loc:
                    loc_parts.append(_esc(loc))
                if vc:
                    loc_parts.append(f"👁{vc}")
                header += f"📍 {' · '.join(loc_parts)}"
            else:
                header = header.rstrip("\n")

            desc = item.get('description', '') or ''
            caption = header
            if desc and desc != "Н/Д":
                quote_prefix = "\n\n<blockquote>"
                quote_suffix = "</blockquote>"
                budget = CAPTION_LIMIT - len(header) - len(quote_prefix) - len(quote_suffix)
                if budget > 40:
                    desc_text = desc
                    if len(desc_text) > budget:
                        desc_text = desc_text[: budget - 3] + "..."
                    caption = header + quote_prefix + _esc(desc_text) + quote_suffix

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
