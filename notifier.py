"""NotificationService - Telegram + звук + кеш картинок + асинхронная очередь.

Очередь переживает сбои сети и разрывы VPN: enqueue_* атомарно кладёт
payload в SQLite, sender-поток пробует доставку с экспоненциальной выдержкой,
health-поток держит флаг _tg_online и будит sender при восстановлении связи.

Parser-flow НЕ блокируется на сети - save_data/display_results успевают
пройти прежде, чем уведомления покинут процесс."""
import base64
import json
import os
import sys
import threading
import time
from datetime import datetime

import requests

import database
from telegram import TelegramNotifier
from utils import sanitize_error_for_telegram
from logger_setup import logger


# Лестница задержек между попытками (секунды). После последнего значения - повтор этого же.
_BACKOFF = [5, 15, 30, 60, 120, 300, 600, 1200, 1800, 3600]

# Пороги max_attempts для разных типов сообщений.
_MAX_ATTEMPTS = {
    "new_item": 20,
    "new_items_header": 10,
    "disappeared_batch": 10,
    "status": 3,
    "error": 3,
    "raw": 3,
}

# Timeout одной попытки из sender-потока. Короткий - пусть очередь ретраит,
# чем мы зависаем на 30 секунд на каждом ряду.
_SENDER_TIMEOUT = 10
_SENDER_PHOTO_TIMEOUT = 20

# Health-check частит и дешёвый, но всё же на один поток выделенный.
_HEALTH_INTERVAL = 30
_HEALTH_TIMEOUT = 8


def _esc(s):
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


class NotificationService:
    """Фасад над TelegramNotifier + асинхронная очередь + кеш картинок."""

    def __init__(self, log):
        self.log = log
        self._notifier = TelegramNotifier()
        self._img_cache = {}
        self._img_cache_order = []
        self._img_cache_max = 256
        self._img_cache_lock = threading.Lock()

        # Фоновые потоки
        self._stop = threading.Event()
        self._wake = threading.Event()  # пинок sender-потоку
        self._sender_thread = None
        self._health_thread = None
        self._tg_online = True  # оптимистично, health-check уточнит

    # ---------- Конфигурация ----------
    def configure(self, token, chat_id, proxies=None):
        """Пересоздаёт TelegramNotifier. Возвращает True если включён."""
        self._notifier = TelegramNotifier(token, chat_id, proxies=proxies)
        # При смене конфига - будим sender и реестимулируем health-check
        self._wake.set()
        return self._notifier.enabled

    @property
    def enabled(self):
        return self._notifier.enabled

    @property
    def notifier(self):
        return self._notifier

    @property
    def tg_online(self):
        return self._tg_online

    def test_connection(self, token, chat_id, proxies=None):
        notifier = TelegramNotifier(token, chat_id, proxies=proxies)
        return notifier, notifier.test_connection()

    # ---------- Синхронные отправки (для тестов, краш-репортов) ----------
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
        if not self._notifier.enabled:
            return False
        return self._notifier.send_message(text)

    # ---------- Асинхронная очередь: enqueue ----------
    def enqueue_status(self, text, status_enabled=True):
        """Ставит статусное сообщение в очередь. Если статусы выключены - no-op."""
        if not status_enabled or not text:
            return False
        self._enqueue("status", {"text": text})
        return True

    def enqueue_error(self, error_text):
        if not error_text:
            return False
        clean = sanitize_error_for_telegram(error_text)
        if len(clean) > 3500:
            clean = clean[:3500] + "..."
        self._enqueue("error", {"text": clean})
        return True

    def enqueue_raw(self, text):
        if not text:
            return False
        self._enqueue("raw", {"text": text})
        return True

    def enqueue_new_items(self, new_items, img_session):
        """Ставит в очередь заголовок + отдельное сообщение на каждое объявление.

        Картинки предзагружаются прямо сейчас (пока живы куки Selenium) и идут
        в payload_json как base64 - sender их только декодирует и аплоадит в TG.
        """
        if not new_items:
            return 0
        items = sorted(new_items, key=lambda x: x.get("pub_date_timestamp", 0) or 0)
        self._enqueue(
            "new_items_header",
            {"text": f"<b>🔔 Найдено новых объявлений: {len(items)}</b>"},
        )
        enqueued = 0
        for item in items:
            img_b64 = None
            img_url = item.get("image_url")
            if img_session and img_url and img_url != "Н/Д" and img_url.startswith("http"):
                data = self.fetch_image_bytes(img_session, img_url, max_attempts=2)
                if data:
                    img_b64 = base64.b64encode(data).decode("ascii")
            payload = {
                "item": {
                    "id": item.get("id"),
                    "title": item.get("title"),
                    "price": item.get("price"),
                    "link": item.get("link"),
                    "description": item.get("description"),
                    "date": item.get("date"),
                    "pub_date_timestamp": item.get("pub_date_timestamp", 0) or 0,
                    "first_seen": item.get("first_seen"),
                },
                "image_b64": img_b64,
            }
            self._enqueue("new_item", payload)
            enqueued += 1
        return enqueued + 1  # + заголовок

    def enqueue_disappeared(self, disappeared):
        """Пачкой, как и было. Сообщения режутся на куски в sender-е."""
        if not disappeared:
            return 0
        slim = [
            {"title": it.get("title", "Н/Д"), "price": it.get("price")}
            for it in disappeared
        ]
        self._enqueue("disappeared_batch", {"items": slim})
        return len(slim)

    def _enqueue(self, type_, payload):
        max_att = _MAX_ATTEMPTS.get(type_, 3)
        try:
            database.queue_enqueue(
                type_,
                json.dumps(payload, ensure_ascii=False),
                max_attempts=max_att,
            )
            self._wake.set()  # пинаем sender
        except Exception as e:
            logger.error(f"Не удалось положить {type_} в очередь TG: {e}")

    # ---------- Статистика для UI ----------
    def get_stats(self):
        try:
            pending = database.queue_count_pending()
        except Exception:
            pending = -1
        return {
            "pending": pending,
            "online": self._tg_online,
            "enabled": self._notifier.enabled,
        }

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

    # ---------- Фоновые потоки ----------
    def start_background(self):
        """Стартует sender + health-потоки. Вызывать один раз на старте приложения."""
        if self._sender_thread and self._sender_thread.is_alive():
            return
        self._stop.clear()
        # БД могла ещё не быть инициализирована (init_db ленивый в storage.py).
        # Делаем idempotent-ный init, иначе queue_count_pending упадёт.
        try:
            database.init_db()
        except Exception as e:
            logger.error(f"notifier: не удалось init_db перед стартом: {e}")
        # Выгрузим старое висевшее при старте, если сессия падала
        try:
            pending = database.queue_count_pending()
            if pending > 0:
                self.log(f"📨 В очереди TG с прошлого запуска: {pending}")
        except Exception:
            pass
        self._sender_thread = threading.Thread(
            target=self._sender_loop, daemon=True, name="tg-sender"
        )
        self._health_thread = threading.Thread(
            target=self._health_loop, daemon=True, name="tg-health"
        )
        self._sender_thread.start()
        self._health_thread.start()

    def stop_background(self, timeout=3):
        self._stop.set()
        self._wake.set()
        for t in (self._sender_thread, self._health_thread):
            if t and t.is_alive():
                t.join(timeout=timeout)

    # ---------- Sender loop ----------
    def _sender_loop(self):
        while not self._stop.is_set():
            try:
                self._wake.clear()
                # Нет конфига TG - просто подождём
                if not self._notifier.enabled:
                    self._wake.wait(10)
                    continue
                # Офлайн по health-check - ждём восстановления или таймаута
                if not self._tg_online:
                    self._wake.wait(5)
                    continue

                now = time.time()
                rows = database.queue_fetch_ready(now, limit=1)
                if not rows:
                    next_at = database.queue_next_scheduled_at()
                    if next_at is None:
                        # очередь пуста
                        self._wake.wait(60)
                    else:
                        wait = max(0.5, min(30, next_at - now))
                        self._wake.wait(wait)
                    continue

                row = rows[0]
                success, err = self._deliver(row)
                if success:
                    database.queue_delete(row["id"])
                else:
                    next_attempt = row["attempts"] + 1
                    if next_attempt >= row["max_attempts"]:
                        self.log(
                            f"📭 TG: {row['type']} выброшен после "
                            f"{next_attempt} попыток: {err}"
                        )
                        database.queue_delete(row["id"])
                    else:
                        delay = _BACKOFF[min(next_attempt - 1, len(_BACKOFF) - 1)]
                        database.queue_mark_failed(
                            row["id"], err, time.time() + delay
                        )
                        if self._looks_like_disconnect(err):
                            self._tg_online = False
            except Exception as e:
                logger.error(f"Sender-поток упал на итерации: {e}")
                self._wake.wait(3)

    def _deliver(self, row):
        """Одна попытка отправки одной строки. Возвращает (ok, err_text)."""
        type_ = row["type"]
        try:
            payload = json.loads(row["payload_json"])
        except Exception as e:
            return False, f"bad payload: {e}"

        try:
            if type_ == "status":
                return self._send_text(payload["text"])
            if type_ == "error":
                text = payload["text"]
                msg = f"<b>❌ Ошибка в программе</b>\n<pre>{text}</pre>"
                return self._send_text(msg)
            if type_ == "raw":
                return self._send_text(payload["text"])
            if type_ == "new_items_header":
                return self._send_text(payload["text"])
            if type_ == "new_item":
                return self._send_new_item(payload)
            if type_ == "disappeared_batch":
                return self._send_disappeared(payload["items"])
        except Exception as e:
            return False, f"{type(e).__name__}: {e}"
        return False, f"unknown type: {type_}"

    def _send_text(self, text, parse_mode="HTML"):
        if not self._notifier.enabled:
            return False, "not enabled"
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
                timeout=_SENDER_TIMEOUT,
                proxies=self._notifier.proxies,
            )
            if resp.status_code == 200:
                return True, ""
            return False, f"HTTP {resp.status_code}: {resp.text[:180]}"
        except Exception as e:
            return False, f"{type(e).__name__}: {e}"

    def _send_photo(self, caption, photo_bytes, parse_mode="HTML"):
        if not self._notifier.enabled:
            return False, "not enabled"
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
                timeout=_SENDER_PHOTO_TIMEOUT,
                proxies=self._notifier.proxies,
            )
            if resp.status_code == 200:
                return True, ""
            return False, f"HTTP {resp.status_code}: {resp.text[:180]}"
        except Exception as e:
            return False, f"{type(e).__name__}: {e}"

    def _send_new_item(self, payload):
        item = payload["item"]
        img_b64 = payload.get("image_b64")

        header = f"<a href='{_esc(item['link'])}'>{_esc(item['title'])}</a>\n"
        header += f"💰 {_esc(item['price'])} руб.\n"
        pub_ts = item.get("pub_date_timestamp", 0) or 0
        if pub_ts > 0:
            pub_str = datetime.fromtimestamp(pub_ts).strftime("%d.%m.%Y %H:%M")
        else:
            pub_str = item.get("date") or "Н/Д"
        header += f"🕐 На Авито: {_esc(pub_str)}\n"
        header += f"📥 В программе: {_esc(item.get('first_seen') or 'Н/Д')}"

        if img_b64:
            try:
                data = base64.b64decode(img_b64)
            except Exception:
                data = None
            if data:
                ok, err = self._send_photo(caption=header, photo_bytes=data)
                if not ok:
                    # fallback на текст, но только если дело не в сети
                    if self._looks_like_disconnect(err):
                        return False, err
                    ok2, err2 = self._send_text(header)
                    if not ok2:
                        return False, f"photo: {err}; text: {err2}"
            else:
                ok, err = self._send_text(header)
                if not ok:
                    return False, err
        else:
            ok, err = self._send_text(header)
            if not ok:
                return False, err

        desc = item.get("description") or ""
        if desc and desc != "Н/Д":
            if len(desc) > 3500:
                desc = desc[:3500] + "..."
            desc_msg = f"<blockquote>{_esc(desc)}</blockquote>"
            # Описание - best-effort: даже если упадёт, карточку не ретраим.
            # Иначе пользователь получит дубли при повторе.
            self._send_text(desc_msg)

        return True, ""

    def _send_disappeared(self, items):
        count = len(items)
        self.log(f"🗑️ TG: шлю пачку 'исчезли' на {count} объявлений")
        MAX_LEN = 4000
        header = f"<b>🗑️ Объявления сняты: {count}</b>\n\n"
        current_msg = header
        messages = []
        for it in items:
            price = it.get("price")
            price_str = f"{price} руб." if price else "цена не указана"
            block = f"• <s>{_esc(it.get('title', 'Н/Д'))}</s> - было {price_str}\n\n"
            if len(current_msg) + len(block) > MAX_LEN:
                messages.append(current_msg)
                current_msg = "🔹 Продолжение:\n\n" + block
            else:
                current_msg += block
        if current_msg:
            messages.append(current_msg)
        # Если любая часть упала - возвращаем False, всю пачку ретраим.
        # Чтобы не задублить - пользователь просто получит полный список ещё раз.
        for m in messages:
            ok, err = self._send_text(m)
            if not ok:
                return False, err
        return True, ""

    # ---------- Health-check loop ----------
    def _health_loop(self):
        while not self._stop.is_set():
            try:
                prev = self._tg_online
                self._tg_online = self._ping_ok()
                if not prev and self._tg_online:
                    self._wake.set()  # связь вернулась, дёрнем sender
            except Exception as e:
                logger.debug(f"Health-check exception: {e}")
                self._tg_online = False
            self._stop.wait(_HEALTH_INTERVAL)

    def _ping_ok(self):
        if not self._notifier.enabled:
            return False
        try:
            url = f"https://api.telegram.org/bot{self._notifier.token}/getMe"
            resp = self._notifier.session.get(
                url, timeout=_HEALTH_TIMEOUT, proxies=self._notifier.proxies,
            )
            return resp.status_code == 200
        except Exception:
            return False

    def ping_direct_vs_proxy(self):
        """Диагностика: пробуем getMe напрямую (без прокси) и через прокси.
        Возвращает dict с результатами. Блокирующая - вызывать из UI-обработчика."""
        out = {"direct": None, "proxy": None}
        if not self._notifier.enabled:
            return out
        url = f"https://api.telegram.org/bot{self._notifier.token}/getMe"

        # Прямой (без прокси), но сессия не trust_env чтоб не подхватить VPN-env
        s = requests.Session()
        s.trust_env = False
        try:
            r = s.get(url, timeout=_HEALTH_TIMEOUT)
            out["direct"] = {"ok": r.status_code == 200, "code": r.status_code}
        except Exception as e:
            out["direct"] = {"ok": False, "err": f"{type(e).__name__}: {e}"}

        # Через настроенный прокси (если задан)
        if self._notifier.proxies:
            try:
                r = s.get(url, timeout=_HEALTH_TIMEOUT, proxies=self._notifier.proxies)
                out["proxy"] = {"ok": r.status_code == 200, "code": r.status_code}
            except Exception as e:
                out["proxy"] = {"ok": False, "err": f"{type(e).__name__}: {e}"}
        return out

    @staticmethod
    def _looks_like_disconnect(err_text):
        if not err_text:
            return False
        el = err_text.lower()
        return any(s in el for s in (
            "timeout", "connectionerror", "connectionreseterror",
            "proxyerror", "nameresolutionerror", "gaierror",
            "max retries exceeded", "socket", "ssl",
        ))
