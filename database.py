import os
import json
import sqlite3
from datetime import datetime
from contextlib import contextmanager

from config import DB_FILE, DATA_FILE
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
    is_favorite INTEGER DEFAULT 0
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


@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
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
        # Миграция: добавление is_favorite если её ещё нет
        cur = conn.execute("PRAGMA table_info(ads)")
        cols = {row["name"] for row in cur.fetchall()}
        if "is_favorite" not in cols:
            conn.execute("ALTER TABLE ads ADD COLUMN is_favorite INTEGER DEFAULT 0")


def _row_to_item(row):
    try:
        is_fav = bool(row["is_favorite"])
    except (IndexError, KeyError):
        is_fav = False
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
        "is_new": False,
    }


def load_all_ads(max_items):
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT * FROM ads ORDER BY pub_date_timestamp DESC LIMIT ?",
            (max_items,),
        )
        return [_row_to_item(r) for r in cur.fetchall()]


def count_ads():
    with get_conn() as conn:
        cur = conn.execute("SELECT COUNT(*) FROM ads")
        return cur.fetchone()[0]


def upsert_ad(item, search_query=None):
    """Вставляет или обновляет объявление. Пишет в price_history при изменении цены."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with get_conn() as conn:
        cur = conn.execute("SELECT price, first_seen FROM ads WHERE id = ?", (item["id"],))
        existing = cur.fetchone()

        if existing is None:
            first_seen = item.get("first_seen") or now
            conn.execute(
                """INSERT INTO ads (id, title, price, link, image_url, description, date,
                                    pub_date_timestamp, search_query, first_seen, last_seen, is_active)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)""",
                (
                    item["id"], item.get("title"), item.get("price"), item.get("link"),
                    item.get("image_url"), item.get("description"), item.get("date"),
                    item.get("pub_date_timestamp") or 0,
                    item.get("search_query") or search_query,
                    first_seen, now,
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
                                  last_seen=?, is_active=1
                   WHERE id=?""",
                (
                    item.get("title"), item.get("price"), item.get("link"),
                    item.get("image_url"), item.get("description"), item.get("date"),
                    item.get("pub_date_timestamp") or 0,
                    item.get("search_query") or search_query,
                    now, item["id"],
                ),
            )
            if item.get("price") is not None and item["price"] != old_price:
                conn.execute(
                    "INSERT INTO price_history (ad_id, price, timestamp) VALUES (?, ?, ?)",
                    (item["id"], item["price"], now),
                )
            return False


def save_ads(items, search_query=None):
    """Массовая запись. Возвращает кол-во вставленных (новых)."""
    inserted = 0
    for item in items:
        if upsert_ad(item, search_query):
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
