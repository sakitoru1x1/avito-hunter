import os
import json
import sqlite3
from datetime import datetime
from contextlib import contextmanager

from config import DB_FILE, DATA_FILE, QUEUE_DB_FILE
from logger_setup import logger


SCHEMA = """
CREATE TABLE IF NOT EXISTS ads (
    id TEXT PRIMARY KEY,
    title TEXT,
    price INTEGER,
    link TEXT,
    image_url TEXT,
    description TEXT,
    date TEXT,
    pub_date_timestamp INTEGER,
    search_query TEXT,
    first_seen TEXT,
    last_seen TEXT,
    is_active INTEGER DEFAULT 1,
    is_favorite INTEGER DEFAULT 0,
    seller_rating REAL
);

CREATE TABLE IF NOT EXISTS search_profiles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE,
    url TEXT,
    city TEXT,
    filters TEXT,
    interval INTEGER,
    is_active INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS price_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ad_id TEXT NOT NULL,
    price INTEGER NOT NULL,
    timestamp TEXT NOT NULL,
    FOREIGN KEY (ad_id) REFERENCES ads(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_ads_pub_date ON ads(pub_date_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_ads_search_query ON ads(search_query);
CREATE INDEX IF NOT EXISTS idx_price_history_ad_id ON price_history(ad_id);
"""


QUEUE_SCHEMA = """
CREATE TABLE IF NOT EXISTS notifications_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    type TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    attempts INTEGER DEFAULT 0,
    next_retry_at REAL NOT NULL,
    created_at REAL NOT NULL,
    max_attempts INTEGER DEFAULT 3,
    last_error TEXT
);

CREATE INDEX IF NOT EXISTS idx_notif_queue_next_retry ON notifications_queue(next_retry_at);
"""


# Таймаут ожидания writer-lock. Сколько секунд ждать другой writer перед
# sqlite3.OperationalError("database is locked"). Дефолт Python - 5с,
# поставим 30 чтоб точно не нарваться на busy-исключения при пиковых бурстах.
_BUSY_TIMEOUT_MS = 30000


def _configure(conn):
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout = {_BUSY_TIMEOUT_MS}")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")


@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_FILE, timeout=_BUSY_TIMEOUT_MS / 1000)
    _configure(conn)
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@contextmanager
def get_queue_conn():
    """Отдельная база под очередь уведомлений - чтоб writer-lock парсера
    (save_ads на сотню строк) не блокировал sender, делающий queue_delete
    после каждой отправки."""
    conn = sqlite3.connect(QUEUE_DB_FILE, timeout=_BUSY_TIMEOUT_MS / 1000)
    _configure(conn)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    with get_conn() as conn:
        conn.executescript(SCHEMA)
        # Миграции: добавление новых колонок если их ещё нет
        cur = conn.execute("PRAGMA table_info(ads)")
        cols = {row["name"] for row in cur.fetchall()}
        if "is_favorite" not in cols:
            conn.execute("ALTER TABLE ads ADD COLUMN is_favorite INTEGER DEFAULT 0")
        if "seller_rating" not in cols:
            conn.execute("ALTER TABLE ads ADD COLUMN seller_rating REAL")
    init_queue_db()


def init_queue_db():
    """Создаёт схему очереди + переносит старые висяки из основной БД, если есть."""
    with get_queue_conn() as qconn:
        qconn.executescript(QUEUE_SCHEMA)

    _migrate_queue_from_main_db()


def _migrate_queue_from_main_db():
    """Одноразовый перенос notifications_queue из основной БД в отдельную.
    Безопасно выполняется многократно: старую таблицу удаляем только после
    успешного переноса, если в ней вообще что-то было."""
    try:
        with get_conn() as conn:
            cur = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='notifications_queue'"
            )
            if cur.fetchone() is None:
                return
            cur = conn.execute(
                "SELECT type, payload_json, attempts, next_retry_at, created_at, "
                "max_attempts, last_error FROM notifications_queue"
            )
            rows = cur.fetchall()
    except Exception as e:
        logger.warning(f"Миграция очереди: не смогла прочитать старую таблицу: {e}")
        return

    if rows:
        try:
            with get_queue_conn() as qconn:
                qconn.executemany(
                    "INSERT INTO notifications_queue "
                    "(type, payload_json, attempts, next_retry_at, created_at, "
                    "max_attempts, last_error) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    [
                        (r["type"], r["payload_json"], r["attempts"], r["next_retry_at"],
                         r["created_at"], r["max_attempts"], r["last_error"])
                        for r in rows
                    ],
                )
            logger.info(f"Миграция очереди: перенесено {len(rows)} рядов в {QUEUE_DB_FILE}")
        except Exception as e:
            logger.error(f"Миграция очереди: не удалось записать в {QUEUE_DB_FILE}: {e}")
            return

    try:
        with get_conn() as conn:
            conn.execute("DROP TABLE IF EXISTS notifications_queue")
    except Exception as e:
        logger.warning(f"Миграция очереди: не удалось удалить старую таблицу: {e}")


def _row_to_item(row):
    try:
        is_fav = bool(row["is_favorite"])
    except (IndexError, KeyError):
        is_fav = False
    try:
        rating = row["seller_rating"]
    except (IndexError, KeyError):
        rating = None
    return {
        "id": row["id"],
        "title": row["title"],
        "price": row["price"],
        "link": row["link"],
        "image_url": row["image_url"],
        "description": row["description"],
        "date": row["date"],
        "pub_date_timestamp": row["pub_date_timestamp"] or 0,
        "search_query": row["search_query"],
        "first_seen": row["first_seen"],
        "last_seen": row["last_seen"],
        "is_active": bool(row["is_active"]),
        "is_favorite": is_fav,
        "seller_rating": rating,
        "is_new": False,
    }


def load_all_ads(max_items):
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT * FROM ads ORDER BY pub_date_timestamp DESC, first_seen DESC LIMIT ?",
            (max_items,),
        )
        return [_row_to_item(r) for r in cur.fetchall()]


def count_ads():
    with get_conn() as conn:
        cur = conn.execute("SELECT COUNT(*) FROM ads")
        return cur.fetchone()[0]


def _upsert_ad_conn(conn, item, search_query=None):
    """Вставляет/обновляет объявление в рамках переданного соединения. True = новое."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cur = conn.execute("SELECT price, first_seen FROM ads WHERE id = ?", (item["id"],))
    existing = cur.fetchone()

    if existing is None:
        first_seen = item.get("first_seen") or now
        conn.execute(
            """INSERT INTO ads (id, title, price, link, image_url, description, date,
                                pub_date_timestamp, search_query, first_seen, last_seen, is_active,
                                seller_rating)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)""",
            (
                item["id"], item.get("title"), item.get("price"), item.get("link"),
                item.get("image_url"), item.get("description"), item.get("date"),
                item.get("pub_date_timestamp") or 0,
                item.get("search_query") or search_query,
                first_seen, now,
                item.get("seller_rating"),
            ),
        )
        if item.get("price") is not None:
            conn.execute(
                "INSERT INTO price_history (ad_id, price, timestamp) VALUES (?, ?, ?)",
                (item["id"], item["price"], now),
            )
        return True
    else:
        old_price = existing["price"]
        conn.execute(
            """UPDATE ads SET title=?, price=?, link=?, image_url=?, description=?,
                              date=?, pub_date_timestamp=?, search_query=COALESCE(?, search_query),
                              last_seen=?, is_active=1,
                              seller_rating=COALESCE(?, seller_rating)
               WHERE id=?""",
            (
                item.get("title"), item.get("price"), item.get("link"),
                item.get("image_url"), item.get("description"), item.get("date"),
                item.get("pub_date_timestamp") or 0,
                item.get("search_query") or search_query,
                now, item.get("seller_rating"),
                item["id"],
            ),
        )
        if item.get("price") is not None and item["price"] != old_price:
            conn.execute(
                "INSERT INTO price_history (ad_id, price, timestamp) VALUES (?, ?, ?)",
                (item["id"], item["price"], now),
            )
        return False


def upsert_ad(item, search_query=None):
    """Публичный API: открывает своё соединение и делает upsert."""
    with get_conn() as conn:
        return _upsert_ad_conn(conn, item, search_query)


def save_ads(items, search_query=None):
    """Массовая запись в одной транзакции. Возвращает кол-во вставленных (новых)."""
    if not items:
        return 0
    inserted = 0
    with get_conn() as conn:
        for item in items:
            if _upsert_ad_conn(conn, item, search_query):
                inserted += 1
    return inserted


def trim_ads(max_items):
    """Удаляет самые старые объявления, если их больше max_items."""
    with get_conn() as conn:
        cur = conn.execute("SELECT COUNT(*) FROM ads")
        total = cur.fetchone()[0]
        if total <= max_items:
            return 0
        conn.execute(
            """DELETE FROM ads WHERE id IN (
                   SELECT id FROM ads ORDER BY pub_date_timestamp ASC LIMIT ?
               )""",
            (total - max_items,),
        )
        return total - max_items


def clear_all():
    with get_conn() as conn:
        conn.execute("DELETE FROM price_history")
        conn.execute("DELETE FROM ads")


def set_favorite(ad_id, is_favorite):
    """Помечает/снимает пометку 'избранное' для объявления."""
    with get_conn() as conn:
        conn.execute(
            "UPDATE ads SET is_favorite = ? WHERE id = ?",
            (1 if is_favorite else 0, ad_id),
        )


def mark_inactive(ad_ids):
    """Помечает список объявлений как неактивные (is_active=0)."""
    if not ad_ids:
        return
    with get_conn() as conn:
        placeholders = ",".join("?" for _ in ad_ids)
        conn.execute(
            f"UPDATE ads SET is_active = 0 WHERE id IN ({placeholders})",
            tuple(ad_ids),
        )


# ---------- Поисковые профили ----------

def _row_to_profile(row):
    return {
        "id": row["id"],
        "name": row["name"],
        "city": row["city"],
        "filters": json.loads(row["filters"]) if row["filters"] else {},
        "interval": row["interval"] or 0,
        "is_active": bool(row["is_active"]),
    }


def list_search_profiles():
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT id, name, city, filters, interval, is_active FROM search_profiles ORDER BY name"
        )
        return [_row_to_profile(r) for r in cur.fetchall()]


def get_search_profile(profile_id):
    with get_conn() as conn:
        cur = conn.execute("SELECT * FROM search_profiles WHERE id = ?", (profile_id,))
        row = cur.fetchone()
        return _row_to_profile(row) if row else None


def get_active_profile():
    with get_conn() as conn:
        cur = conn.execute("SELECT * FROM search_profiles WHERE is_active = 1 LIMIT 1")
        row = cur.fetchone()
        return _row_to_profile(row) if row else None


def create_search_profile(name, city, filters, interval=0):
    filters_json = json.dumps(filters or {}, ensure_ascii=False)
    with get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO search_profiles (name, url, city, filters, interval, is_active) "
            "VALUES (?, ?, ?, ?, ?, 0)",
            (name, "", city, filters_json, interval or 0),
        )
        return cur.lastrowid


def update_search_profile(profile_id, name=None, city=None, filters=None, interval=None):
    with get_conn() as conn:
        cur = conn.execute("SELECT * FROM search_profiles WHERE id = ?", (profile_id,))
        row = cur.fetchone()
        if row is None:
            return False
        new_name = name if name is not None else row["name"]
        new_city = city if city is not None else row["city"]
        if filters is not None:
            new_filters = json.dumps(filters, ensure_ascii=False)
        else:
            new_filters = row["filters"]
        new_interval = interval if interval is not None else row["interval"]
        conn.execute(
            "UPDATE search_profiles SET name=?, city=?, filters=?, interval=? WHERE id=?",
            (new_name, new_city, new_filters, new_interval, profile_id),
        )
        return True


def delete_search_profile(profile_id):
    with get_conn() as conn:
        conn.execute("DELETE FROM search_profiles WHERE id = ?", (profile_id,))


def set_active_profile(profile_id):
    """Делает указанный профиль активным, остальные - нет. profile_id=None снимает активность со всех."""
    with get_conn() as conn:
        conn.execute("UPDATE search_profiles SET is_active = 0")
        if profile_id is not None:
            conn.execute("UPDATE search_profiles SET is_active = 1 WHERE id = ?", (profile_id,))


# ---------- Миграция ----------

def migrate_from_json():
    """Импортирует avito_history.json в БД, если БД пуста и файл существует."""
    if not os.path.exists(DATA_FILE):
        return 0
    if count_ads() > 0:
        return 0

    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            items = json.load(f)
    except Exception as e:
        logger.error(f"Миграция: не удалось прочитать {DATA_FILE}: {e}")
        return 0

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    migrated = 0
    with get_conn() as conn:
        for item in items:
            try:
                conn.execute(
                    """INSERT OR IGNORE INTO ads
                       (id, title, price, link, image_url, description, date,
                        pub_date_timestamp, search_query, first_seen, last_seen, is_active)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)""",
                    (
                        item.get("id"), item.get("title"), item.get("price"),
                        item.get("link"), item.get("image_url"), item.get("description"),
                        item.get("date"), item.get("pub_date_timestamp") or 0,
                        None,
                        item.get("first_seen") or now, now,
                    ),
                )
                if item.get("id") and item.get("price") is not None:
                    conn.execute(
                        "INSERT INTO price_history (ad_id, price, timestamp) VALUES (?, ?, ?)",
                        (item["id"], item["price"], item.get("first_seen") or now),
                    )
                migrated += 1
            except Exception as e:
                logger.error(f"Миграция: ошибка для id={item.get('id')}: {e}")
                continue

    logger.info(f"Миграция из JSON завершена: {migrated} объявлений")
    return migrated


# ---------- Очередь уведомлений ----------

def queue_enqueue(type_, payload_json, max_attempts=3):
    """Кладёт строку в очередь, готовую к немедленной отправке."""
    import time as _t
    now = _t.time()
    with get_queue_conn() as conn:
        cur = conn.execute(
            "INSERT INTO notifications_queue "
            "(type, payload_json, attempts, next_retry_at, created_at, max_attempts) "
            "VALUES (?, ?, 0, ?, ?, ?)",
            (type_, payload_json, now, now, max_attempts),
        )
        return cur.lastrowid


def queue_fetch_ready(now_ts, limit=1):
    """Возвращает готовые к отправке строки (next_retry_at <= now), по возрасту."""
    with get_queue_conn() as conn:
        cur = conn.execute(
            "SELECT id, type, payload_json, attempts, max_attempts, created_at "
            "FROM notifications_queue "
            "WHERE next_retry_at <= ? "
            "ORDER BY next_retry_at ASC LIMIT ?",
            (now_ts, limit),
        )
        return [dict(r) for r in cur.fetchall()]


def queue_next_scheduled_at():
    """Время ближайшего next_retry_at (или None если очередь пуста)."""
    with get_queue_conn() as conn:
        cur = conn.execute(
            "SELECT MIN(next_retry_at) AS t FROM notifications_queue"
        )
        row = cur.fetchone()
        return row["t"] if row and row["t"] is not None else None


def queue_mark_failed(row_id, err, next_retry_at):
    """Инкрементирует attempts и откладывает попытку."""
    with get_queue_conn() as conn:
        conn.execute(
            "UPDATE notifications_queue "
            "SET attempts = attempts + 1, next_retry_at = ?, last_error = ? "
            "WHERE id = ?",
            (next_retry_at, (err or "")[:500], row_id),
        )


def queue_delete(row_id):
    with get_queue_conn() as conn:
        conn.execute("DELETE FROM notifications_queue WHERE id = ?", (row_id,))


def queue_count_pending():
    with get_queue_conn() as conn:
        cur = conn.execute("SELECT COUNT(*) AS c FROM notifications_queue")
        row = cur.fetchone()
        return row["c"] if row else 0


def queue_purge_old(max_age_days=7):
    """Чистит висяки старше N дней (последняя защита от роста БД)."""
    import time as _t
    cutoff = _t.time() - max_age_days * 86400
    with get_queue_conn() as conn:
        cur = conn.execute(
            "DELETE FROM notifications_queue WHERE created_at < ?",
            (cutoff,),
        )
        return cur.rowcount
