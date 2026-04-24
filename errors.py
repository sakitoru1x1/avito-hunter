"""
Human-readable error formatter.

Превращает сырые исключения и traceback в понятные для пользователя сообщения
с рекомендациями. Полный трейс при этом остаётся в logger.error для отладки.

Использование:
    from errors import format_user_error
    try:
        ...
    except Exception as e:
        self.log(format_user_error(e, context="parser"))
        logger.error(traceback.format_exc())
"""

import json
import socket
import sqlite3

try:
    import requests
    _REQUESTS_AVAILABLE = True
except ImportError:
    _REQUESTS_AVAILABLE = False

try:
    from selenium.common.exceptions import (
        WebDriverException,
        TimeoutException,
        NoSuchElementException,
        SessionNotCreatedException,
        InvalidSessionIdException,
    )
    _SELENIUM_AVAILABLE = True
except ImportError:
    _SELENIUM_AVAILABLE = False


CHROME_INSTALL_HINT = (
    "Убедитесь что установлен Google Chrome (последняя версия). "
    "Скачать: https://www.google.com/chrome/"
)

NETWORK_HINT = (
    "Проверьте интернет-соединение. Если используете прокси - проверьте его настройки "
    "на вкладке 'Настройки'."
)

AVITO_BLOCK_HINT = (
    "Авито временно ограничил доступ. Парсер подождёт и продолжит. "
    "Если ошибка повторяется - увеличьте интервал между запросами или подключите прокси."
)

TG_TOKEN_HINT = (
    "Неверный Telegram-токен. Создайте бота через @BotFather и скопируйте токен в настройки."
)

TG_CHAT_HINT = (
    "Неверный chat_id. Напишите боту /start, затем откройте "
    "https://api.telegram.org/bot<ТОКЕН>/getUpdates и скопируйте chat.id."
)

DB_LOCKED_HINT = (
    "База данных заблокирована (возможно, открыта в другой программе или предыдущий процесс "
    "не завершился). Закройте все копии программы и попробуйте снова."
)

DB_CORRUPT_HINT = (
    "База данных повреждена. Сделайте резервную копию файла ads.db и удалите его - "
    "программа создаст новую БД при следующем запуске."
)

SETTINGS_CORRUPT_HINT = (
    "Файл настроек settings.json повреждён. Удалите его - программа создаст новый "
    "со значениями по умолчанию."
)

LAYOUT_CHANGED_HINT = (
    "Возможно, Авито обновил вёрстку сайта. Проверьте наличие обновления парсера "
    "или сообщите об ошибке."
)


def _is_avito_block(exc):
    """Эвристика: 429/403 от авито в тексте ошибки."""
    msg = str(exc).lower()
    return any(s in msg for s in ("429", "403", "too many requests", "access denied", "rate limit"))


def _is_chrome_missing(exc):
    """Chrome не установлен / не запустился."""
    msg = str(exc).lower()
    markers = (
        "cannot find chrome",
        "chrome not reachable",
        "chrome failed to start",
        "chrome binary",
        "no such file or directory: 'chrome'",
        "'chrome' executable needs to be in path",
    )
    return any(m in msg for m in markers)


def _is_session_dead(exc):
    """Браузер упал / сессия протухла."""
    if _SELENIUM_AVAILABLE and isinstance(exc, (InvalidSessionIdException, SessionNotCreatedException)):
        return True
    msg = str(exc).lower()
    return any(m in msg for m in (
        "invalid session id",
        "session deleted",
        "no such window",
        "disconnected",
        "target window already closed",
    ))


def format_user_error(exc, context=None):
    """Формирует понятное пользователю сообщение об ошибке.

    Args:
        exc: Исключение.
        context: Контекст: "driver", "parser", "telegram", "db", "settings", "save".

    Returns:
        str - многострочное сообщение для self.log или статуса.
    """
    # --- Selenium / Chrome ---
    # ВАЖНО: TimeoutException и NoSuchElementException - подклассы WebDriverException,
    # поэтому их проверяем первыми.
    if _SELENIUM_AVAILABLE:
        if isinstance(exc, TimeoutException):
            if context == "driver":
                return "⏱ Страница не загрузилась вовремя. Проверьте интернет или попробуйте позже."
            if context == "parser":
                return "⏱ Какая-то абракадабра с капчей, перезапускаемся"
            return "⏱ Превышено время ожидания."
        if isinstance(exc, NoSuchElementException):
            if context == "parser":
                return f"⚠️ Не найден ожидаемый элемент на странице. {LAYOUT_CHANGED_HINT}"
            return f"⚠️ Элемент не найден: {_first_line(exc)}"
        if isinstance(exc, WebDriverException):
            if _is_chrome_missing(exc):
                return f"❌ Chrome не найден. {CHROME_INSTALL_HINT}"
            if _is_session_dead(exc):
                return "⚠️ Браузер отвалился - перезапускаем и пробуем снова."
            if _is_avito_block(exc):
                return f"⚠️ Авито блокирует запросы. {AVITO_BLOCK_HINT}"
            return (
                f"❌ Ошибка браузера: {_first_line(exc)}. "
                f"{CHROME_INSTALL_HINT}"
            )

    # --- Network ---
    if _REQUESTS_AVAILABLE:
        if isinstance(exc, requests.exceptions.HTTPError):
            status = getattr(getattr(exc, "response", None), "status_code", None)
            if context == "telegram":
                if status == 401:
                    return f"❌ Telegram: {TG_TOKEN_HINT}"
                if status == 400:
                    return f"❌ Telegram: {TG_CHAT_HINT}"
                if status == 429:
                    return "⚠️ Telegram: слишком много сообщений, ждём."
                return f"❌ Telegram вернул HTTP {status}."
            if status in (429, 403):
                return f"⚠️ Сервер ответил {status}. {AVITO_BLOCK_HINT}"
            return f"❌ HTTP {status}: {_first_line(exc)}"
        if isinstance(exc, requests.exceptions.ProxyError):
            return "❌ Прокси не отвечает. Проверьте настройки прокси на вкладке 'Настройки'."
        if isinstance(exc, requests.exceptions.ConnectionError):
            return f"❌ Нет соединения. {NETWORK_HINT}"
        if isinstance(exc, requests.exceptions.Timeout):
            return f"⏱ Сервер не ответил вовремя. {NETWORK_HINT}"
        if isinstance(exc, requests.exceptions.RequestException):
            return f"❌ Сетевая ошибка: {_first_line(exc)}"

    if isinstance(exc, (socket.timeout, socket.gaierror, ConnectionError, ConnectionResetError)):
        return f"❌ Нет соединения. {NETWORK_HINT}"

    # --- SQLite ---
    if isinstance(exc, sqlite3.OperationalError):
        msg = str(exc).lower()
        if "locked" in msg:
            return f"❌ БД: {DB_LOCKED_HINT}"
        if "no such table" in msg:
            return "❌ БД: структура таблиц повреждена. Удалите файл ads.db - программа создаст БД заново."
        return f"❌ Ошибка БД: {_first_line(exc)}"
    if isinstance(exc, sqlite3.DatabaseError):
        return f"❌ БД повреждена: {DB_CORRUPT_HINT}"

    # --- Settings / JSON ---
    if isinstance(exc, json.JSONDecodeError):
        if context == "settings":
            return f"❌ {SETTINGS_CORRUPT_HINT}"
        return f"❌ Повреждён JSON: {_first_line(exc)}"

    if isinstance(exc, FileNotFoundError):
        if "chrome" in str(exc).lower():
            return f"❌ Chrome не найден. {CHROME_INSTALL_HINT}"
        return f"❌ Файл не найден: {exc.filename or _first_line(exc)}"

    if isinstance(exc, PermissionError):
        return (
            f"❌ Нет прав доступа к файлу: {exc.filename or _first_line(exc)}. "
            "Закройте файл в других программах или запустите от имени администратора."
        )

    # --- Fallback ---
    return f"❌ Ошибка: {_first_line(exc)}"


def _first_line(exc):
    """Первая строка текста исключения без 'Class: ...'."""
    txt = str(exc).strip()
    if not txt:
        return exc.__class__.__name__
    return txt.splitlines()[0][:200]


def should_retry(exc):
    """Стоит ли повторить операцию при этой ошибке."""
    if _SELENIUM_AVAILABLE and isinstance(exc, (TimeoutException,)):
        return True
    if _SELENIUM_AVAILABLE and isinstance(exc, WebDriverException):
        return _is_session_dead(exc) or _is_avito_block(exc)
    if _REQUESTS_AVAILABLE:
        if isinstance(exc, (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.ProxyError,
        )):
            return True
        if isinstance(exc, requests.exceptions.HTTPError):
            status = getattr(getattr(exc, "response", None), "status_code", None)
            return status in (429, 502, 503, 504)
    if isinstance(exc, (socket.timeout, socket.gaierror, ConnectionError, ConnectionResetError)):
        return True
    return False


def backoff_seconds(attempt, base=5, maximum=120):
    """Экспоненциальный backoff: 5, 10, 20, 40, 80, 120..."""
    delay = base * (2 ** max(0, attempt))
    return min(delay, maximum)
