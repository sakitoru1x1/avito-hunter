"""ParseParams - снимок UI-состояния для передачи в worker-поток.

Собирается в UI-потоке в start_parsing. Worker-поток не должен трогать Tk-переменные:
все значения читаются один раз на UI-boundary и передаются дальше через этот dataclass.
"""
from dataclasses import dataclass
from typing import List, Optional


@dataclass(frozen=True)
class ParseParams:
    # Поисковый запрос
    query: str
    min_price: int
    max_price: int
    city: Optional[str]

    # Фильтры
    filter_services: bool
    ignore_words: List[str]
    delivery: bool

    # Браузер / прокси
    show_browser: bool
    proxy_settings: Optional[dict]

    # Расписание
    schedule_enabled: bool
    schedule_start: str
    schedule_end: str
    schedule_days: List[bool]

    # Уведомления
    notify_sound: bool
    tg_notify_status: bool

    # Антикапча
    captcha_api_key: str = ""
    captcha_service: str = "rucaptcha"

    # Режим скорости: без batch fetch деталей и без загрузки фото
    speed_mode: bool = False
