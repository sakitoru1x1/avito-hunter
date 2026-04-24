"""ParserOrchestrator - парсинг-цикл, извлечённый из gui.py.

Содержит всю логику взаимодействия с браузером: навигация, капча,
прокрутка, вызов AvitoParser, скачивание фото. Не импортирует tkinter.
Общается с GUI через колбэки log/set_status/stop_check.
"""
import random
import time
import traceback
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

import requests
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

from config import USER_AGENTS
from errors import format_user_error, should_retry, backoff_seconds
from logger_setup import logger
from notifier import NotificationService
from params import ParseParams
from parser import (
    AvitoParser,
    is_captcha_page as _parser_is_captcha_page,
    detect_disappeared as _parser_detect_disappeared,
)
from storage import save_data
from utils import transliterate, random_sleep, is_within_schedule
import database


@dataclass
class CycleResult:
    added: int = 0
    error: bool = False
    skipped_schedule: bool = False
    driver_failed: bool = False
    rate_limit_restart: bool = False
    items_changed: bool = False
    tg_items: list = field(default_factory=list)


class ParserOrchestrator:
    """Управляет одним циклом парсинга. Не знает про Tk."""

    def __init__(self, driver_manager, history, notifier, avito_parser,
                 image_executor, chrome_profile_dir, tg_queue, *,
                 log, set_status, stop_check):
        self.driver_manager = driver_manager
        self.history = history
        self.notifier: NotificationService = notifier
        self.avito_parser: AvitoParser = avito_parser
        self.image_executor: ThreadPoolExecutor = image_executor
        self._chrome_profile_dir = chrome_profile_dir
        self.tg_queue = tg_queue

        self.log = log
        self.set_status = set_status
        self.stop_check = stop_check

        self.cached_search_url = None
        self.cached_search_key = None
        self._captcha_recovery_in_progress = False
        self._avito_block_attempts = 0

    # ------------------------------------------------------------------ #
    #  Капча                                                               #
    # ------------------------------------------------------------------ #

    def _is_captcha_page(self, driver):
        try:
            src = (driver.page_source or "").lower()
        except Exception:
            return False
        markers = [
            "captcha", "firewall", "доступ ограничен",
            "подтвердите, что вы не робот", "access-confirm", "are you a robot",
        ]
        return any(m in src for m in markers)

    def _try_auto_solve_captcha(self, driver, params: ParseParams):
        api_key = params.captcha_api_key
        if not api_key:
            return False
        try:
            from captcha_solver import CaptchaSolver
            solver = CaptchaSolver(api_key, service=params.captcha_service, log_func=self.log)
            self.log("🤖 Пробую автоматическое решение капчи...")
            self.set_status("🤖 Решаю капчу автоматически...")
            self.notifier.send_status("⚠️ Программа словила капчу. Подождите, сейчас индус её решит.")
            return solver.solve(driver)
        except Exception as e:
            self.log(f"⚠️ Ошибка авто-решения: {e}")
            return False

    def _recover_from_captcha(self, params: ParseParams):
        if self._captcha_recovery_in_progress:
            self.log("🚧 Восстановление после капчи уже идёт - пропускаю")
            return False
        self._captcha_recovery_in_progress = True
        proxy_settings = params.proxy_settings
        show_browser = params.show_browser
        try:
            driver = self.driver_manager.driver
            max_attempts = 3
            for attempt in range(1, max_attempts + 1):
                if driver and self._try_auto_solve_captcha(driver, params):
                    self.log("✅ Капча решена автоматически")
                    self.set_status("✅ Капча решена")
                    time.sleep(3)
                    return True
                if attempt < max_attempts:
                    try:
                        page_src = driver.page_source.lower() if driver else ""
                    except Exception:
                        page_src = ""
                    if "доступ ограничен" in page_src or "проблема с ip" in page_src:
                        self.log("🚫 Блокировка IP, ретраи бесполезны")
                        break
                    self.log(f"⏸ Попытка {attempt}/{max_attempts} не удалась, пробую ещё...")
                    time.sleep(2)
                    driver = self.driver_manager.driver

            self.log("🚧 Открываю видимый браузер для сброса сессии (5 сек)...")
            self.set_status("🚧 Сброс сессии через видимый браузер")

            try:
                self.driver_manager.hard_kill()
            except Exception:
                pass

            if not self.driver_manager.ensure_driver(
                proxy_settings, self.log,
                show_browser=True,
                user_data_dir=self._chrome_profile_dir,
            ):
                self.log("Не удалось открыть видимый браузер")
                return False

            try:
                self.driver_manager.driver.get("https://www.avito.ru/")
            except Exception as e:
                self.log(f"Не удалось открыть avito.ru: {e}")

            time.sleep(5)
            self.log("✓ Сессия сброшена, возвращаюсь в рабочий режим")

            try:
                self.driver_manager.hard_kill()
            except Exception:
                pass

            if not self.driver_manager.ensure_driver(
                proxy_settings, self.log,
                show_browser=show_browser,
                user_data_dir=self._chrome_profile_dir,
            ):
                self.log("Не удалось перезапустить драйвер после сброса")
                return False

            self.set_status("✓ Сессия сброшена")
            return True
        finally:
            self._captcha_recovery_in_progress = False

    # ------------------------------------------------------------------ #
    #  Парсинг элементов                                                   #
    # ------------------------------------------------------------------ #

    def _parse_items(self, items, params: ParseParams):
        return self.avito_parser.parse_items(
            driver=self.driver_manager.driver,
            items=items,
            min_price=params.min_price,
            max_price=params.max_price,
            search_query=params.query,
            filter_services=params.filter_services,
            ignore_words=params.ignore_words,
            known_ids=self.history.known_ids(),
            filtered_ids=self.history.get_filtered_ids(),
            stop_check=self.stop_check,
            captcha_callback=lambda: self._recover_from_captcha(params),
            get_driver=lambda: self.driver_manager.driver,
            skip_batch=params.speed_mode,
        )

    # ------------------------------------------------------------------ #
    #  Картинки                                                            #
    # ------------------------------------------------------------------ #

    def _build_image_session(self):
        session = requests.Session()
        if self.driver_manager.driver:
            try:
                for cookie in self.driver_manager.driver.get_cookies():
                    session.cookies.set(cookie['name'], cookie['value'])
            except Exception:
                pass
        session.headers.update({
            'User-Agent': random.choice(USER_AGENTS),
            'Referer': 'https://www.avito.ru/',
        })
        return session

    def _prefetch_images(self, items):
        session = self._build_image_session()
        for item in items:
            url = item.get("image_url")
            if url and url != "Н/Д" and url.startswith("http"):
                if not self.notifier.has_cached(url):
                    self.image_executor.submit(self.notifier.fetch_image_bytes, session, url)

    # ------------------------------------------------------------------ #
    #  Основной цикл                                                       #
    # ------------------------------------------------------------------ #

    def run_cycle(self, params: ParseParams) -> CycleResult:
        result = CycleResult()
        query = params.query
        min_price = params.min_price
        max_price = params.max_price
        city = params.city
        proxy_settings = params.proxy_settings
        fast = params.speed_mode

        filter_key = (
            query, min_price, max_price,
            int(params.filter_services),
            tuple(sorted(params.ignore_words)),
        )
        changed, prev_count = self.history.reset_filter_cache_if_changed(filter_key)
        if changed and prev_count:
            self.log(f"🔄 Фильтры изменились - сброс кэша отбракованных ({prev_count})")

        ok, reason = is_within_schedule(
            params.schedule_enabled, params.schedule_start,
            params.schedule_end, params.schedule_days,
        )
        if not ok:
            self.log(f"⏸ {reason} - парсинг пропущен")
            if params.tg_notify_status:
                self.notifier.send_status(f"⏸ {reason}", status_enabled=True)
            result.skipped_schedule = True
            return result

        if not self.driver_manager.ensure_driver(
            proxy_settings, self.log,
            show_browser=params.show_browser,
            user_data_dir=self._chrome_profile_dir,
        ):
            self.log("Не удалось создать драйвер. Парсинг невозможен.")
            result.driver_failed = True
            return result

        driver = self.driver_manager.driver

        try:
            encoded_query = urllib.parse.quote_plus(query)
            search_key = f"{query}|{city}|{int(params.delivery)}"
            use_cached = (self.cached_search_url
                          and self.cached_search_key == search_key)

            if use_cached:
                self.log("Открываем сохранённый URL (быстрый путь)")
                driver.get(self.cached_search_url)
                random_sleep(0.8, 1.5) if fast else random_sleep(2.0, 3.5)
                try:
                    WebDriverWait(driver, 15).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "[data-marker='item']"))
                    )
                    self.log("Карточки загружены")
                except TimeoutException:
                    if _parser_is_captcha_page(driver):
                        solved = self._recover_from_captcha(params)
                        if not solved:
                            return result
                        driver = self.driver_manager.driver
                        try:
                            WebDriverWait(driver, 10).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, "[data-marker='item']"))
                            )
                            self.log("Карточки загружены после решения капчи")
                        except TimeoutException:
                            self.cached_search_url = None
                            use_cached = False
                    else:
                        self.log("Кеш URL не сработал, идём долгим путём")
                        self.cached_search_url = None
                        use_cached = False

            if not use_cached:
                if city and city != "Вся Россия":
                    city_slug = transliterate(city)
                    url = f"https://www.avito.ru/{city_slug}?q={encoded_query}&s=104"
                    self.log(f"Открываем URL для города {city}: {url}")
                else:
                    url = f"https://www.avito.ru/rossiya?q={encoded_query}&s=104"
                    self.log(f"Открываем URL для всей России: {url}")
                driver.get(url)
                random_sleep(1.5, 2.5) if fast else random_sleep(4.0, 7.0)

            if self.stop_check():
                return result

            if not use_cached:
                self._handle_first_visit(driver, query, params, fast)
                if self.stop_check():
                    return result

                try:
                    self.cached_search_url = driver.current_url
                    self.cached_search_key = search_key
                    self.log("URL сохранён для быстрых перезапросов")
                except Exception:
                    pass

            self._scroll_page(driver, fast)
            if self.stop_check():
                return result

            items = driver.find_elements(By.CSS_SELECTOR, "[data-marker='item']")
            self.log(f"Найдено карточек: {len(items)}")
            self.set_status(f"📋 Обработка карточек: {len(items)}")
            new_results, page_summary = self._parse_items(items, params)

            if getattr(self.avito_parser, 'had_rate_limit', False):
                self.cached_search_url = None
                result.rate_limit_restart = True
                self.log("🔄 Rate-limit обнаружен, быстрый перезапуск для сброса капчи")

            self.log(f"Новых после фильтров: {len(new_results)}")

            ps_by_id = {p["id"]: p for p in page_summary}
            retry_updated_items = self.history.apply_retry_image_updates(ps_by_id)
            if retry_updated_items:
                self.log(f"🖼 Догружено фото у {len(retry_updated_items)} старых объявлений")

            disappeared = _parser_detect_disappeared(self.history.get_all(), page_summary, query)
            if disappeared:
                database.mark_inactive([it["id"] for it in disappeared])
                if self.notifier.enabled:
                    self.notifier.send_disappeared(disappeared)

            added = self.history.update_with_new(new_results)
            result.added = added
            if added > 0:
                self.log(f"Добавлено новых объявлений: {added}")
            else:
                self.log("Новых объявлений не найдено")

            if added > 0 and params.notify_sound:
                NotificationService.play_sound()

            dirty = list(new_results)
            if retry_updated_items:
                dirty_ids = {it["id"] for it in dirty}
                for it in retry_updated_items:
                    if it["id"] not in dirty_ids:
                        dirty.append(it)
            if dirty:
                save_data(dirty, self.log)

            if added > 0 or retry_updated_items:
                result.items_changed = True

            all_items = self.history.get_all()
            if not params.speed_mode:
                self._prefetch_images(all_items)

            if added > 0:
                result.tg_items = list(self.history.iter_new())

            self.set_status(
                f"✅ Готово. Новых: {added}",
                counter=f"Всего в БД: {self.history.count()}",
            )

        except Exception as e:
            if self.stop_check():
                logger.info(f"Парсер остановлен (жёсткий стоп): {type(e).__name__}")
                return result
            result.error = True
            error_trace = traceback.format_exc()
            user_msg = format_user_error(e, context="parser")
            self.log(user_msg)
            logger.error(f"Ошибка парсинга: {error_trace}")
            self.notifier.send_error(user_msg)
            self.set_status(user_msg[:80])

            if should_retry(e):
                try:
                    from selenium.common.exceptions import WebDriverException
                    if isinstance(e, WebDriverException):
                        self.log("🔄 Перезапускаем браузер...")
                        self.driver_manager.cleanup()
                    msg_l = str(e).lower()
                    if any(s in msg_l for s in ("429", "403", "too many", "rate limit")):
                        wait = backoff_seconds(self._avito_block_attempts)
                        self._avito_block_attempts += 1
                        self.set_status(f"⏸ Авито блокирует. Жду {wait} сек перед повтором...")
                        self.log(f"⏸ Backoff {wait} сек (попытка {self._avito_block_attempts})")
                        time.sleep(wait)
                    else:
                        self._avito_block_attempts = 0
                except Exception:
                    pass
            else:
                self._avito_block_attempts = 0

        return result

    # ------------------------------------------------------------------ #
    #  Вспомогательные методы навигации                                    #
    # ------------------------------------------------------------------ #

    def _handle_first_visit(self, driver, query, params, fast):
        try:
            cookie_btn = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, '//button[contains(text(),"Принять")]'))
            )
            cookie_btn.click()
            self.log("Куки приняты")
            random_sleep(0.3, 0.6) if fast else random_sleep(0.7, 1.8)
            if self.stop_check():
                return
        except TimeoutException:
            self.log("Куки уже приняты")

        try:
            city_btn = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, '//button[contains(text(),"Да")]'))
            )
            city_btn.click()
            self.log("Город подтверждён")
            random_sleep(0.5, 1.0) if fast else random_sleep(1.5, 3.0)
            if self.stop_check():
                return
        except TimeoutException:
            pass

        try:
            search_input = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "input[data-marker='search-form/suggest']"))
            )
            search_input.clear()
            search_input.send_keys(query)
            search_button = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button[data-marker='search-form/submit-button']"))
            )
            search_button.click()
            self.log("Поиск выполнен")
            if self.stop_check():
                return
        except (TimeoutException, NoSuchElementException):
            self.log("URL уже содержит запрос")

        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "[data-marker='item']"))
            )
        except TimeoutException:
            is_captcha = _parser_is_captcha_page(driver)
            if not is_captcha:
                src = (driver.page_source or "")[:500].lower()
                has_items = "data-marker" in src and "item" in src
                if not has_items:
                    self.log("⚠️ Нет карточек и нет капчи - возможно блокировка без капчи")
                    is_captcha = True
            if is_captcha:
                pre_captcha_url = driver.current_url
                solved = self._recover_from_captcha(params)
                if not solved:
                    raise TimeoutException("Капча не решена")
                driver = self.driver_manager.driver
                search_url = self.cached_search_url or pre_captcha_url
                self.log(f"🔄 Перезагружаю поиск после капчи: {search_url}")
                driver.get(search_url)
                random_sleep(1.5, 2.5) if fast else random_sleep(3.0, 5.0)
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "[data-marker='item']"))
                )
            else:
                raise

        self.log("Карточки загружены")
        random_sleep(0.5, 1.0) if fast else random_sleep(1.5, 3.0)
        if self.stop_check():
            return

        if params.delivery:
            self._apply_delivery_filter(driver, fast)

    def _apply_delivery_filter(self, driver, fast):
        try:
            self.log("Применяем фильтр 'Авито Доставка'...")
            driver.execute_script("window.scrollBy(0, 300);")
            random_sleep(0.3, 0.6) if fast else random_sleep(0.7, 1.6)

            delivery_element = None
            selectors = [
                (By.XPATH, "//span[contains(text(),'С Авито Доставкой')]"),
                (By.XPATH, "//label[contains(.,'С Авито Доставкой')]"),
            ]

            for by, selector in selectors:
                try:
                    elem = WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((by, selector))
                    )
                    driver.execute_script("arguments[0].scrollIntoView();", elem)
                    random_sleep(0.2, 0.4) if fast else random_sleep(0.4, 0.9)
                    elem.click()
                    delivery_element = elem
                    break
                except (TimeoutException, NoSuchElementException) as e:
                    self.log(f"Не удалось по селектору {selector}: {e}")
                    continue

            if delivery_element is None:
                self.log("Не удалось найти элемент 'Авито Доставка'")
            else:
                random_sleep(0.5, 1.0) if fast else random_sleep(1.5, 2.8)
                try:
                    show_span = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located(
                            (By.XPATH, "//span[starts-with(text(),'Показать')]"))
                    )
                    parent_button = show_span.find_element(By.XPATH, "ancestor::button")
                    parent_button.click()
                except (TimeoutException, NoSuchElementException) as e:
                    self.log(f"Кнопка применения не найдена - возможно, фильтр применился сразу: {e}")

                random_sleep(1.0, 1.5) if fast else random_sleep(2.5, 4.0)
                try:
                    WebDriverWait(driver, 25).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "[data-marker='item']"))
                    )
                except TimeoutException:
                    self.log("После фильтра доставки карточки не появились за 25с - продолжаем с текущей страницей")

        except Exception as e:
            self.log(f"Не удалось применить фильтр доставки (пропускаем): {e}")
            logger.error(f"Ошибка при фильтре доставки: {traceback.format_exc()}")

    def _scroll_page(self, driver, fast):
        self.log("Прокручиваем страницу...")
        target_cards = 50
        last_height = driver.execute_script("return document.body.scrollHeight")
        current_position = 0
        max_scroll_attempts = 15
        attempts = 0

        while attempts < max_scroll_attempts:
            if self.stop_check():
                self.log("Прокрутка прервана")
                return

            cards_in_dom = driver.execute_script(
                "return document.querySelectorAll(\"[data-marker='item']\").length;"
            )
            if cards_in_dom >= target_cards:
                self.log(f"Набрано {cards_in_dom} карточек, прокрутка не нужна")
                break

            scroll_step = random.randint(600, 1200)
            current_position += scroll_step
            if current_position > last_height:
                current_position = last_height
            driver.execute_script(f"window.scrollTo(0, {current_position});")
            time.sleep(random.uniform(0.1, 0.2) if fast else random.uniform(0.2, 0.6))
            if self.stop_check():
                return

            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height > last_height:
                last_height = new_height
                attempts = 0
            else:
                attempts += 1

            if current_position >= last_height - 100:
                self.log("Достигнут конец страницы.")
                break

        self.log("Прокрутка завершена")

        try:
            total_h = driver.execute_script("return document.body.scrollHeight") or 0
            step = 600
            y = 0
            while y < total_h:
                if self.stop_check():
                    return
                driver.execute_script(f"window.scrollTo(0, {y});")
                time.sleep(0.15 if fast else 0.35)
                y += step
            driver.execute_script(f"window.scrollTo(0, {total_h});")
            time.sleep(0.15 if fast else 0.3)
            driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(0.15 if fast else 0.3)
        except Exception:
            pass
