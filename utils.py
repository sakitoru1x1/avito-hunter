import re
import random
import time
from datetime import datetime, timedelta


def random_sleep(min_s, max_s):
    """Случайная пауза в диапазоне [min_s, max_s] секунд."""
    t = random.uniform(min_s, max_s)
    time.sleep(t)
    return t


def is_within_schedule(schedule_enabled, start_hm, end_hm, days_mask, now=None):
    """Проверяет, попадает ли текущий момент в рабочее окно расписания.

    schedule_enabled: bool - расписание включено
    start_hm, end_hm: строки "HH:MM"
    days_mask: list[bool] длины 7 (Пн=0..Вс=6), True = день активен
    now: datetime для тестов, по умолчанию now()

    Возвращает (ok: bool, reason: str). Если расписание выключено - (True, '').
    """
    if not schedule_enabled:
        return True, ""
    if now is None:
        now = datetime.now()

    if days_mask and len(days_mask) == 7:
        if not days_mask[now.weekday()]:
            return False, f"День недели отключён в расписании"

    try:
        sh, sm = [int(x) for x in start_hm.split(":")]
        eh, em = [int(x) for x in end_hm.split(":")]
    except Exception:
        return True, ""

    start_min = sh * 60 + sm
    end_min = eh * 60 + em
    cur_min = now.hour * 60 + now.minute

    if start_min == end_min:
        return True, ""

    if start_min < end_min:
        ok = start_min <= cur_min < end_min
    else:
        ok = cur_min >= start_min or cur_min < end_min

    if ok:
        return True, ""
    return False, f"Сейчас вне рабочего окна {start_hm}-{end_hm}"


def transliterate(text):
    mapping = {
        'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'e',
        'ж': 'zh', 'з': 'z', 'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm',
        'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u',
        'ф': 'f', 'х': 'kh', 'ц': 'ts', 'ч': 'ch', 'ш': 'sh', 'щ': 'shch',
        'ъ': '', 'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu', 'я': 'ya',
        'А': 'A', 'Б': 'B', 'В': 'V', 'Г': 'G', 'Д': 'D', 'Е': 'E', 'Ё': 'E',
        'Ж': 'Zh', 'З': 'Z', 'И': 'I', 'Й': 'Y', 'К': 'K', 'Л': 'L', 'М': 'M',
        'Н': 'N', 'О': 'O', 'П': 'P', 'Р': 'R', 'С': 'S', 'Т': 'T', 'У': 'U',
        'Ф': 'F', 'Х': 'Kh', 'Ц': 'Ts', 'Ч': 'Ch', 'Ш': 'Sh', 'Щ': 'Shch',
        'Ъ': '', 'Ы': 'Y', 'Ь': '', 'Э': 'E', 'Ю': 'Yu', 'Я': 'Ya'
    }
    result = ''
    for ch in text:
        result += mapping.get(ch, ch)
    result = result.replace(' ', '-').replace('--', '-')
    result = result.lower()
    return result


def parse_date_to_timestamp(date_str):
    if not date_str or date_str == "Н/Д":
        return 0
    now = datetime.now()
    date_str = date_str.lower().strip()

    if date_str.startswith("сегодня"):
        parts = date_str.split()
        if len(parts) > 1 and ':' in parts[1]:
            time_part = parts[1]
            try:
                t = datetime.strptime(time_part, "%H:%M")
                dt = datetime(now.year, now.month, now.day, t.hour, t.minute)
                return int(dt.timestamp())
            except ValueError:
                pass
        return int(now.timestamp())

    if date_str.startswith("вчера"):
        parts = date_str.split()
        yesterday = now - timedelta(days=1)
        if len(parts) > 1 and ':' in parts[1]:
            time_part = parts[1]
            try:
                t = datetime.strptime(time_part, "%H:%M")
                dt = datetime(yesterday.year, yesterday.month, yesterday.day, t.hour, t.minute)
                return int(dt.timestamp())
            except ValueError:
                pass
        return int(yesterday.timestamp())

    months = {
        'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4, 'мая': 5, 'июня': 6,
        'июля': 7, 'августа': 8, 'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12
    }
    pattern = r'(\d+)\s+(' + '|'.join(months.keys()) + r')(?:\s+(\d+))?'
    match = re.search(pattern, date_str)
    if match:
        day = int(match.group(1))
        month = months[match.group(2)]
        year = int(match.group(3)) if match.group(3) else now.year
        dt = datetime(year, month, day)
        if dt > now:
            dt = dt.replace(year=year - 1)
        return int(dt.timestamp())
    return 0


def sanitize_error_for_telegram(error_text):
    """Убирает потенциальные креды/токены из текста ошибки перед отправкой."""
    patterns = [
        r'(password["\s:=]+)[^\s,\}]+',
        r'(token["\s:=]+)[^\s,\}]+',
        r'(username["\s:=]+)[^\s,\}]+',
        r'(bot)\d+:[A-Za-z0-9_-]+',
    ]
    sanitized = error_text
    for pattern in patterns:
        sanitized = re.sub(pattern, r'\1***СКРЫТО***', sanitized, flags=re.IGNORECASE)
    return sanitized
