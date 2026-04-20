"""
Автотесты для errors.py и интеграции в driver/telegram/storage.

Запуск: python test_errors.py
Возвращает 0 если всё зелёное, 1 если хоть один тест упал.
"""

import json
import socket
import sqlite3
import sys
import traceback

# Чтобы не тянуть tkinter, gui.py не импортируем - только errors.py и лёгкие модули
from errors import format_user_error, should_retry, backoff_seconds

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

try:
    from selenium.common.exceptions import (
        WebDriverException,
        TimeoutException,
        NoSuchElementException,
        SessionNotCreatedException,
        InvalidSessionIdException,
    )
    HAS_SELENIUM = True
except ImportError:
    HAS_SELENIUM = False


PASSED = 0
FAILED = 0
FAILS = []


def check(name, expected_substring, actual, negate=False):
    global PASSED, FAILED
    ok = (expected_substring in actual) if not negate else (expected_substring not in actual)
    if ok:
        PASSED += 1
        print(f"  ✅ {name}")
    else:
        FAILED += 1
        FAILS.append((name, expected_substring, actual))
        print(f"  ❌ {name}")
        print(f"     Ожидалось {'НЕ ' if negate else ''}найти: {expected_substring!r}")
        print(f"     В строке:  {actual!r}")


def check_eq(name, expected, actual):
    global PASSED, FAILED
    if expected == actual:
        PASSED += 1
        print(f"  ✅ {name}")
    else:
        FAILED += 1
        FAILS.append((name, expected, actual))
        print(f"  ❌ {name}")
        print(f"     Ожидалось: {expected!r}")
        print(f"     Получено:  {actual!r}")


def check_true(name, cond):
    global PASSED, FAILED
    if cond:
        PASSED += 1
        print(f"  ✅ {name}")
    else:
        FAILED += 1
        FAILS.append((name, True, cond))
        print(f"  ❌ {name}")


# --------- Тесты format_user_error ---------

print("\n=== format_user_error: сеть ===")
if HAS_REQUESTS:
    try:
        raise requests.exceptions.ConnectionError("Max retries exceeded")
    except Exception as e:
        check("ConnectionError → про нет соединения", "Нет соединения", format_user_error(e))
        check("ConnectionError → без traceback", "Traceback", format_user_error(e), negate=True)

    try:
        raise requests.exceptions.Timeout("read timed out")
    except Exception as e:
        check("Timeout → про сервер не ответил", "не ответил", format_user_error(e))

    try:
        raise requests.exceptions.ProxyError("proxy refused")
    except Exception as e:
        check("ProxyError → про прокси", "Прокси", format_user_error(e))

    class FakeResp:
        def __init__(self, status):
            self.status_code = status

    try:
        err = requests.exceptions.HTTPError("401 Unauthorized")
        err.response = FakeResp(401)
        raise err
    except Exception as e:
        msg = format_user_error(e, context="telegram")
        check("TG 401 → про неверный токен", "Неверный", msg)
        check("TG 401 → про BotFather", "BotFather", msg)

    try:
        err = requests.exceptions.HTTPError("400 Bad Request")
        err.response = FakeResp(400)
        raise err
    except Exception as e:
        check("TG 400 → про chat_id", "chat_id", format_user_error(e, context="telegram"))

    try:
        err = requests.exceptions.HTTPError("429 Too Many")
        err.response = FakeResp(429)
        raise err
    except Exception as e:
        check("HTTP 429 → про ограничение доступа", "ограничил", format_user_error(e))

print("\n=== format_user_error: SQLite ===")
try:
    raise sqlite3.OperationalError("database is locked")
except Exception as e:
    msg = format_user_error(e, context="db")
    check("DB locked → про заблокированную БД", "заблокирована", msg)
    check("DB locked → есть инструкция", "Закройте все копии", msg)

try:
    raise sqlite3.OperationalError("no such table: ads")
except Exception as e:
    check("DB no table → инструкция удалить ads.db", "удалите файл", format_user_error(e).lower())

try:
    raise sqlite3.DatabaseError("file is not a database")
except Exception as e:
    check("DB corrupt → про повреждение", "повреждена", format_user_error(e))

print("\n=== format_user_error: JSON/файлы ===")
try:
    json.loads("{bad json")
except Exception as e:
    check("Settings JSON → инструкция удалить", "settings.json", format_user_error(e, context="settings"))

try:
    raise FileNotFoundError(2, "No such file", "chrome.exe")
except Exception as e:
    check("FileNotFound chrome → про Chrome", "Chrome", format_user_error(e))

try:
    raise PermissionError(13, "Permission denied", "ads.db")
except Exception as e:
    check("PermissionError → про права", "прав доступа", format_user_error(e))

print("\n=== format_user_error: Selenium ===")
if HAS_SELENIUM:
    try:
        raise WebDriverException("cannot find Chrome binary")
    except Exception as e:
        check("WebDriverException без Chrome → инструкция", "Chrome не найден", format_user_error(e))

    try:
        raise InvalidSessionIdException("invalid session id")
    except Exception as e:
        check("InvalidSessionIdException → про отвалился", "отвалился", format_user_error(e))

    try:
        raise WebDriverException("Received error 429 from server")
    except Exception as e:
        check("WebDriverException 429 → про блокировку", "блокирует", format_user_error(e))

    try:
        raise TimeoutException("wait timeout")
    except Exception as e:
        check("TimeoutException в parser → про карточки", "карточки",
              format_user_error(e, context="parser"))

    try:
        raise NoSuchElementException("no such element")
    except Exception as e:
        check("NoSuchElement в parser → про вёрстку",
              "вёрстку", format_user_error(e, context="parser"))

print("\n=== format_user_error: socket ===")
try:
    raise socket.timeout("timed out")
except Exception as e:
    check("socket.timeout → про соединение", "соединения", format_user_error(e))

try:
    raise ConnectionResetError("connection reset")
except Exception as e:
    check("ConnectionResetError → про соединение", "соединения", format_user_error(e))

print("\n=== format_user_error: fallback ===")
try:
    raise ValueError("что-то странное")
except Exception as e:
    check("ValueError → общий fallback", "Ошибка", format_user_error(e))
    check("ValueError → содержит текст", "странное", format_user_error(e))

# --------- Тесты should_retry ---------

print("\n=== should_retry ===")
if HAS_REQUESTS:
    try:
        raise requests.exceptions.ConnectionError()
    except Exception as e:
        check_true("retry при ConnectionError", should_retry(e))

    try:
        raise requests.exceptions.Timeout()
    except Exception as e:
        check_true("retry при Timeout", should_retry(e))

    try:
        err = requests.exceptions.HTTPError()
        err.response = FakeResp(503)
        raise err
    except Exception as e:
        check_true("retry при HTTP 503", should_retry(e))

    try:
        err = requests.exceptions.HTTPError()
        err.response = FakeResp(404)
        raise err
    except Exception as e:
        check_true("НЕ retry при HTTP 404", not should_retry(e))

if HAS_SELENIUM:
    try:
        raise InvalidSessionIdException()
    except Exception as e:
        check_true("retry при протухшей сессии", should_retry(e))

try:
    raise ValueError()
except Exception as e:
    check_true("НЕ retry при ValueError", not should_retry(e))

# --------- Тесты backoff_seconds ---------

print("\n=== backoff_seconds ===")
check_eq("attempt 0 → 5 сек", 5, backoff_seconds(0))
check_eq("attempt 1 → 10 сек", 10, backoff_seconds(1))
check_eq("attempt 2 → 20 сек", 20, backoff_seconds(2))
check_eq("attempt 3 → 40 сек", 40, backoff_seconds(3))
check_eq("attempt 4 → 80 сек", 80, backoff_seconds(4))
check_eq("attempt 5 → 120 сек (капнуло по максимуму)", 120, backoff_seconds(5))
check_eq("attempt 10 → тоже 120 (максимум держит)", 120, backoff_seconds(10))
check_eq("отрицательный attempt → 5 (base)", 5, backoff_seconds(-3))

# --------- Проверка что модули проекта импортируются без ошибок ---------

print("\n=== Импорт модулей ===")
REQUIRE_SELENIUM = {"driver"}
for modname in ["errors", "driver", "telegram", "storage", "utils", "database", "config"]:
    try:
        __import__(modname)
        PASSED += 1
        print(f"  ✅ import {modname}")
    except ModuleNotFoundError as e:
        if modname in REQUIRE_SELENIUM and not HAS_SELENIUM:
            print(f"  ⏭  import {modname} (selenium не установлен - пропущено)")
            continue
        FAILED += 1
        FAILS.append((f"import {modname}", "OK", str(e)))
        print(f"  ❌ import {modname} -> {e}")
    except Exception as e:
        FAILED += 1
        FAILS.append((f"import {modname}", "OK", str(e)))
        print(f"  ❌ import {modname} -> {e}")
        traceback.print_exc()

# --------- Итог ---------

print("\n" + "=" * 50)
print(f"Прошло: {PASSED}, Упало: {FAILED}")
if FAILED:
    print("\nФейлы:")
    for name, exp, got in FAILS:
        print(f"  - {name}")
    sys.exit(1)
else:
    print("\nВсё зелёное 👌")
    sys.exit(0)
