"""HistoryService - владелец списка объявлений. RLock защищает all_items и фильтр-кеш."""
import json
import threading
from datetime import datetime

from storage import save_data, clear_history_files, update_all_items
from logger_setup import logger


class HistoryService:
    """Хранит список объявлений и кеш отбракованных ID. Потокобезопасен через RLock."""

    def __init__(self, max_items, log):
        self._lock = threading.RLock()
        self._all_items = []
        self._filtered_ids = set()
        self._last_filter_key = None
        self.max_items = max_items
        self.log = log

    # ---------- Чтение ----------
    def get_all(self):
        """Снимок списка (неглубокая копия)."""
        with self._lock:
            return list(self._all_items)

    def count(self):
        with self._lock:
            return len(self._all_items)

    def known_ids(self):
        """ID всего, что уже видели (включая отбракованных)."""
        with self._lock:
            return {it["id"] for it in self._all_items} | set(self._filtered_ids)

    def iter_new(self):
        """Список объявлений с is_new=True."""
        with self._lock:
            return [it for it in self._all_items if it.get("is_new", False)]

    def items_by_id(self):
        with self._lock:
            return {it["id"]: it for it in self._all_items}

    def get_filtered_ids_snapshot(self):
        with self._lock:
            return set(self._filtered_ids)

    # ---------- Фильтр-кеш ----------
    def reset_filter_cache_if_changed(self, filter_key):
        """Возвращает (changed: bool, prev_count: int). Если ключ сменился - сбрасывает _filtered_ids."""
        with self._lock:
            if filter_key == self._last_filter_key:
                return False, 0
            prev_count = len(self._filtered_ids)
            self._filtered_ids = set()
            self._last_filter_key = filter_key
            return True, prev_count

    def get_filtered_ids(self):
        """Возвращает САМ set (не копию). Для передачи в parser.parse_items, который мутирует его."""
        with self._lock:
            return self._filtered_ids

    # ---------- Мутации ----------
    def update_with_new(self, new_results):
        """Сливает new_results через storage.update_all_items. Возвращает added."""
        with self._lock:
            self._all_items, added = update_all_items(
                self._all_items, new_results, self.max_items, self.log
            )
            return added

    def apply_retry_image_updates(self, page_summary_by_id):
        """Догружает image_url у старых, у кого было 'Н/Д'. Возвращает список обновлённых."""
        updated = []
        with self._lock:
            for existing in self._all_items:
                if existing.get("image_url") in (None, "", "Н/Д"):
                    ps = page_summary_by_id.get(existing["id"])
                    if ps and ps.get("image_url") not in (None, "", "Н/Д"):
                        existing["image_url"] = ps["image_url"]
                        updated.append(existing)
        return updated

    def clear(self):
        with self._lock:
            self._all_items = []
            self._filtered_ids = set()
            self._last_filter_key = None
        try:
            clear_history_files()
        except Exception as e:
            logger.error(f"Не удалось очистить БД: {e}")

    def replace_all(self, items):
        """Полная замена списка (для загрузки из файла)."""
        for it in items:
            it["is_new"] = False
        with self._lock:
            self._all_items = items

    # ---------- БД / файлы ----------
    def save_dirty(self, dirty_items):
        """Пишет только указанные объявления в БД."""
        if dirty_items:
            save_data(dirty_items, self.log)

    def persist_all(self):
        with self._lock:
            snapshot = list(self._all_items)
        save_data(snapshot, self.log)

    def export_to_file(self, path):
        """Дамп всего списка в JSON. Возвращает кол-во сохранённых."""
        with self._lock:
            snapshot = list(self._all_items)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(snapshot, f, ensure_ascii=False, indent=2, default=str)
        logger.info(f"История сохранена: {path} ({len(snapshot)} объявлений)")
        return len(snapshot)

    def import_from_file(self, path):
        """Читает JSON, заменяет текущий список. Возвращает кол-во загруженных."""
        with open(path, 'r', encoding='utf-8') as f:
            items = json.load(f)
        if not isinstance(items, list):
            raise ValueError("В файле ожидался JSON-список объявлений")
        self.replace_all(items)
        self.persist_all()
        return len(items)
