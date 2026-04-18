from datetime import datetime

import database
from logger_setup import logger


_initialized = False


def _ensure_initialized(log_callback=None):
    global _initialized
    if _initialized:
        return
    try:
        database.init_db()
        migrated = database.migrate_from_json()
        if migrated > 0 and log_callback:
            log_callback(f"Миграция из JSON: импортировано {migrated} объявлений")
        _initialized = True
    except Exception as e:
        logger.error(f"Ошибка инициализации БД: {e}")
        if log_callback:
            log_callback(f"Ошибка инициализации БД: {e}")
        raise


def save_data(all_items, log_callback=None, search_query=None):
    """Сохраняет список объявлений в SQLite."""
    try:
        _ensure_initialized(log_callback)
        database.save_ads(all_items, search_query=search_query)
        if log_callback:
            log_callback(f"История сохранена ({len(all_items)} объявлений)")
    except Exception as e:
        if log_callback:
            log_callback(f"Ошибка сохранения истории: {e}")
        logger.error(f"Ошибка сохранения истории: {e}")


def load_data(max_items, log_callback=None):
    """Загружает список объявлений из SQLite."""
    try:
        _ensure_initialized(log_callback)
        items = database.load_all_ads(max_items)
        for item in items:
            item["is_new"] = False
        if items and log_callback:
            log_callback(f"Загружена история ({len(items)} объявлений)")
        elif not items and log_callback:
            log_callback("История пуста, начинаем с нуля")
        return items
    except Exception as e:
        if log_callback:
            log_callback(f"Ошибка загрузки истории: {e}")
        logger.error(f"Ошибка загрузки истории: {e}")
        return []


def clear_history_files():
    """Очищает все объявления из БД."""
    try:
        _ensure_initialized()
        database.clear_all()
    except Exception as e:
        logger.error(f"Ошибка очистки БД: {e}")


def update_all_items(all_items, new_items, max_items, log_callback=None):
    """Обновляет список объявлений новыми данными. Возвращает (обновлённый список, кол-во новых)."""
    if not new_items:
        if log_callback:
            log_callback("⚠️ Предупреждение: new_items пуст, обновление списка пропущено")
        return all_items, 0

    existing_ids = {item["id"] for item in all_items}
    new_ids = {item["id"] for item in new_items}

    new_ones = []
    for item in new_items:
        if item["id"] not in existing_ids:
            item["is_new"] = True
            item["first_seen"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            item.setdefault("is_favorite", False)
            new_ones.append(item)
        else:
            for old in all_items:
                if old["id"] == item["id"]:
                    item["first_seen"] = old.get("first_seen", "Н/Д")
                    item["is_favorite"] = old.get("is_favorite", False)
                    if item.get("seller_rating") is None:
                        item["seller_rating"] = old.get("seller_rating")
                    break
            item["is_new"] = False

    old_remaining = [item for item in all_items if item["id"] not in new_ids]
    for item in old_remaining:
        item["is_new"] = False

    updated_old = [item for item in new_items if item["id"] in existing_ids]
    for item in updated_old:
        item["is_new"] = False

    combined = new_ones + updated_old + old_remaining
    combined.sort(
        key=lambda x: (x.get('pub_date_timestamp', 0), x.get('first_seen', '')),
        reverse=True,
    )

    if len(combined) > max_items:
        removed = len(combined) - max_items
        combined = combined[:max_items]
        if log_callback:
            log_callback(f"📦 Лимит истории {max_items}: удалено {removed} самых старых объявлений")

    return combined, len(new_ones)
