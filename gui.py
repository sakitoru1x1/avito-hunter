import tkinter as tk
from tkinter import messagebox, simpledialog, filedialog
import customtkinter as ctk
import threading
import time
import random
import requests
import urllib.parse
import webbrowser
import os
import sys
import json
import traceback
from datetime import datetime
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor
from PIL import Image

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

from config import CITIES, USER_AGENTS, SETTINGS_FILE, DEFAULT_MAX_ITEMS
from utils import transliterate, parse_date_to_timestamp, sanitize_error_for_telegram, random_sleep, is_within_schedule
from logger_setup import logger
from telegram import TelegramNotifier
from driver import DriverManager
from storage import save_data, clear_history_files
from errors import format_user_error, should_retry, backoff_seconds
import database

from parser import (
    AvitoParser,
    is_captcha_page as _parser_is_captcha_page,
    detect_disappeared as _parser_detect_disappeared,
)
from history import HistoryService
from notifier import NotificationService
from settings_model import load_settings as load_app_settings, save_settings as save_app_settings, AppSettings
from params import ParseParams

ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("blue")

class ParserApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Avito Hunter v.1.1")

        screen_w = root.winfo_screenwidth()
        screen_h = root.winfo_screenheight()
        win_w = min(1600, int(screen_w * 0.8))
        win_h = min(1000, int(screen_h * 0.8))
        x = (screen_w - win_w) // 2
        y = (screen_h - win_h) // 2
        self.root.geometry(f"{win_w}x{win_h}+{x}+{y}")
        self.root.minsize(800, 600)

        self.images = []
        self.auto_update = False
        self.driver_manager = DriverManager()
        # Постоянный профиль Chrome - куки (в т.ч. решённая капча) переживают
        # перезапуск драйвера между headless и видимым режимами.
        self._chrome_profile_dir = os.path.join(os.path.expanduser("~"), ".avito-hunter", "chrome-profile")
        self._captcha_recovery_in_progress = False
        self.previous_ids = set()
        self.stop_parsing = False
        self.max_items = DEFAULT_MAX_ITEMS
        self.image_executor = ThreadPoolExecutor(max_workers=4)

        self.history = HistoryService(self.max_items, self.log)
        self.notifier = NotificationService(self.log)
        self.avito_parser = AvitoParser(self.log)

        self.notify_var = tk.BooleanVar(value=True)
        self.filter_services_var = tk.BooleanVar(value=False)
        self.delivery_var = tk.BooleanVar(value=False)

        self.create_widgets()
        self.load_settings()
        self.history.max_items = self.max_items
        self._load_data()
        self.refresh_profiles_list()
        self._apply_active_profile_on_startup()

    def create_widgets(self):
        # Статусбар (создаём первым, чтобы он пришпилился к низу)
        statusbar = ctk.CTkFrame(self.root, border_width=1)
        statusbar.pack(side="bottom", fill="x")
        self.status_var = tk.StringVar(value="⏸ Ожидание")
        self.status_label = ctk.CTkLabel(statusbar, textvariable=self.status_var, anchor="w")
        self.status_label.pack(side="left", padx=5, pady=2)
        self.status_counter_var = tk.StringVar(value="")
        self.status_counter_label = ctk.CTkLabel(statusbar, textvariable=self.status_counter_var, anchor="e")
        self.status_counter_label.pack(side="right", padx=5, pady=2)
        self.progress = ctk.CTkProgressBar(statusbar, mode='indeterminate')
        self.progress.pack(side="left", fill="x", expand=True, padx=10, pady=4)

        main_container = ctk.CTkFrame(self.root)
        main_container.pack(fill="both", expand=True, padx=10, pady=5)

        self.notebook = ctk.CTkTabview(main_container)
        self.notebook.pack(fill="both", expand=True)

        # ========== Вкладка "Результаты поиска" ==========
        tab_results = self.notebook.add("Результаты поиска")

        top_half = ctk.CTkFrame(tab_results, fg_color="transparent")
        top_half.pack(fill="x", pady=(0, 5))

        # Левая колонка - критерии (город, запрос, игнор, цена)
        search_left = ctk.CTkFrame(top_half, border_width=1)
        search_left.pack(side="left", fill="both", expand=True, padx=(0, 3))
        ctk.CTkLabel(search_left, text="Критерии поиска", font=ctk.CTkFont(weight="bold")).pack(pady=(5, 0))

        row1 = ctk.CTkFrame(search_left)
        row1.pack(fill="x", pady=2, padx=8)
        ctk.CTkLabel(row1, text="Город:").pack(side="left", padx=2)
        self.city_var = tk.StringVar(value="Москва")
        self.city_combo = ctk.CTkComboBox(row1, variable=self.city_var, values=CITIES, state="readonly")
        self.city_combo.pack(side="left", padx=2)
        self.city_combo.configure(command=lambda _: self.on_city_change(None))

        self.all_russia_var = tk.BooleanVar()
        self.all_russia_cb = ctk.CTkCheckBox(row1, text="Вся Россия", variable=self.all_russia_var,
                                              command=self.on_all_russia)
        self.all_russia_cb.pack(side="left", padx=10)

        row2 = ctk.CTkFrame(search_left)
        row2.pack(fill="x", pady=2, padx=8)
        ctk.CTkLabel(row2, text="Запрос:").pack(side="left", padx=2)
        self.query_entry = ctk.CTkEntry(row2, width=30*8)
        self.query_entry.pack(side="left", padx=2, fill="x", expand=True)
        self.query_entry.insert(0, "")

        row2b = ctk.CTkFrame(search_left)
        row2b.pack(fill="x", pady=2, padx=8)
        ctk.CTkLabel(row2b, text="Игнор:").pack(side="left", padx=2)
        self.ignore_entry = ctk.CTkEntry(row2b, width=30*8,
                                          placeholder_text="через запятую: 3s, б/у, сломан")
        self.ignore_entry.pack(side="left", padx=2, fill="x", expand=True)

        row3 = ctk.CTkFrame(search_left)
        row3.pack(fill="x", pady=2, padx=8)
        ctk.CTkLabel(row3, text="Цена от:").pack(side="left", padx=2)
        self.min_price_entry = ctk.CTkEntry(row3, width=8*8)
        self.min_price_entry.pack(side="left", padx=2)
        self.min_price_entry.insert(0, "")
        ctk.CTkLabel(row3, text="до:").pack(side="left", padx=(5, 2))
        self.max_price_entry = ctk.CTkEntry(row3, width=8*8)
        self.max_price_entry.pack(side="left", padx=2)
        self.max_price_entry.insert(0, "")

        # Правая колонка - управление (флажки, кнопки, интервал)
        search_right = ctk.CTkFrame(top_half, border_width=1)
        search_right.pack(side="right", fill="both", expand=True, padx=(3, 0))
        ctk.CTkLabel(search_right, text="Управление", font=ctk.CTkFont(weight="bold")).pack(pady=(5, 0))

        row4 = ctk.CTkFrame(search_right)
        row4.pack(fill="x", pady=2, padx=8)
        self.notify_cb = ctk.CTkCheckBox(row4, text="Звук", variable=self.notify_var)
        self.notify_cb.pack(side="left", padx=2)
        self.filter_cb = ctk.CTkCheckBox(row4, text="Убрать услуги", variable=self.filter_services_var)
        self.filter_cb.pack(side="left", padx=2)
        self.delivery_cb = ctk.CTkCheckBox(row4, text="Авито доставка", variable=self.delivery_var)
        self.delivery_cb.pack(side="left", padx=2)

        row5 = ctk.CTkFrame(search_right)
        row5.pack(fill="x", pady=5, padx=8)
        self.start_button = ctk.CTkButton(row5, text="▶ Начать", command=self.start_parsing)
        self.start_button.pack(side="left", padx=2)
        self.kill_button = ctk.CTkButton(
            row5, text="⏹ Стоп", fg_color="#7a2a2a", hover_color="#a03030",
            command=self.hard_stop_handler, state='disabled', width=90,
        )
        self.kill_button.pack(side="left", padx=2)

        row5b = ctk.CTkFrame(search_right)
        row5b.pack(fill="x", pady=2, padx=8)
        self.clear_history_button = ctk.CTkButton(row5b, text="🗑 Очистить историю", command=self.clear_history)
        self.clear_history_button.pack(side="left", padx=2)
        self.save_as_profile_button = ctk.CTkButton(row5b, text="💾 Сохранить как профиль",
                                                  command=self.save_current_search_as_profile)
        self.save_as_profile_button.pack(side="left", padx=2)

        row7 = ctk.CTkFrame(search_right)
        row7.pack(fill="x", pady=2, padx=8)
        ctk.CTkLabel(row7, text="Интервал (мин): от").pack(side="left", padx=2)
        self.min_interval = ctk.CTkEntry(row7, width=4*8)
        self.min_interval.pack(side="left", padx=2)
        self.min_interval.insert(0, "1")
        ctk.CTkLabel(row7, text="до").pack(side="left", padx=(2, 0))
        self.max_interval = ctk.CTkEntry(row7, width=4*8)
        self.max_interval.pack(side="left", padx=2)
        self.max_interval.insert(0, "3")

        bottom_frame = ctk.CTkFrame(tab_results, border_width=1)
        bottom_frame.pack(fill="both", expand=True, pady=(5, 0))

        # Баннер "Новые объявления" с кнопкой перехода к следующему
        self._new_banner_frame = ctk.CTkFrame(bottom_frame, fg_color="#5a1e1e", height=36)
        self._new_banner_label = ctk.CTkLabel(
            self._new_banner_frame, text="", font=ctk.CTkFont(size=13, weight="bold")
        )
        self._new_banner_label.pack(side="left", padx=10)
        ctk.CTkButton(
            self._new_banner_frame, text="→ Следующее", width=120,
            command=self._jump_to_next_new,
        ).pack(side="left", padx=5, pady=4)
        ctk.CTkButton(
            self._new_banner_frame, text="✕", width=30, fg_color="#7a2a2a",
            command=self._hide_new_banner,
        ).pack(side="right", padx=5, pady=4)
        self._new_jump_cursor = 0

        self.canvas = tk.Canvas(bottom_frame, borderwidth=0, highlightthickness=0, bg=ctk.ThemeManager.theme["CTkFrame"]["fg_color"][1])
        self.scrollbar = tk.Scrollbar(bottom_frame, orient="vertical", command=self.canvas.yview)
        self.canvas.configure(yscrollcommand=self.scrollbar.set)

        self.scrollbar.pack(side="right", fill="y")
        self.canvas.pack(side="left", fill="both", expand=True)

        self.results_frame = ctk.CTkFrame(self.canvas, fg_color="transparent")
        self.canvas.create_window((0, 0), window=self.results_frame, anchor="nw", tags=("window",))
        self.results_frame.bind("<Configure>", self.on_frame_configure)
        self.canvas.bind("<Configure>", self.on_canvas_configure)

        # Колёсико работает только когда курсор над канвасом результатов
        def _bind_wheel(_e=None):
            self.canvas.bind_all("<MouseWheel>", self._on_mousewheel)
            self.canvas.bind_all("<Button-4>", self._on_mousewheel_linux)
            self.canvas.bind_all("<Button-5>", self._on_mousewheel_linux)

        def _unbind_wheel(_e=None):
            self.canvas.unbind_all("<MouseWheel>")
            self.canvas.unbind_all("<Button-4>")
            self.canvas.unbind_all("<Button-5>")

        self.canvas.bind("<Enter>", _bind_wheel)
        self.canvas.bind("<Leave>", _unbind_wheel)
        self.results_frame.bind("<Enter>", _bind_wheel)
        self.results_frame.bind("<Leave>", _unbind_wheel)

        # ========== Вкладка "Лог" ==========
        tab_log = self.notebook.add("Лог")
        self.log_text = ctk.CTkTextbox(tab_log, wrap="word")
        self.log_text.pack(fill="both", expand=True, padx=5, pady=5)
        # Read-only, но с возможностью выделять/копировать
        self.log_text.configure(state="disabled")
        # Тег для ссылок - синие, подчёркнутые, курсор-рука
        self.log_text._textbox.tag_configure("link", foreground="#4EA1FF", underline=True)
        self.log_text._textbox.tag_bind("link", "<Enter>",
            lambda e: self.log_text._textbox.configure(cursor="hand2"))
        self.log_text._textbox.tag_bind("link", "<Leave>",
            lambda e: self.log_text._textbox.configure(cursor=""))
        self.log_text._textbox.tag_bind("link", "<Button-1>", self._on_log_link_click)

        # ========== Вкладка "Настройки" ==========
        tab_settings = self.notebook.add("Настройки")

        # Скролл-контейнер + центрированная секция фиксированной ширины.
        # При растягивании окна пустое место уходит в боковые колонки (weight=1),
        # а содержимое (колонка 1, minsize=720) остаётся в центре.
        settings_scroll = ctk.CTkScrollableFrame(tab_settings)
        settings_scroll.pack(fill="both", expand=True)
        settings_scroll.grid_columnconfigure(0, weight=1)
        settings_scroll.grid_columnconfigure(1, weight=0, minsize=720)
        settings_scroll.grid_columnconfigure(2, weight=1)
        _settings_row = 0

        proxy_frame = ctk.CTkFrame(settings_scroll, border_width=1)
        proxy_frame.grid(row=_settings_row, column=1, sticky="ew", padx=10, pady=5)
        _settings_row += 1
        proxy_frame.grid_columnconfigure(1, weight=1)
        proxy_frame.grid_columnconfigure(3, weight=1)
        proxy_frame.grid_columnconfigure(5, weight=1)

        ctk.CTkLabel(proxy_frame, text="Прокси", font=ctk.CTkFont(weight="bold")).grid(row=0, column=0, columnspan=6, pady=(5,0))
        ctk.CTkLabel(
            proxy_frame,
            text="желательно российский (для Avito)",
            text_color="gray",
            font=ctk.CTkFont(size=11),
        ).grid(row=1, column=0, columnspan=6, pady=(0, 5))

        ctk.CTkLabel(proxy_frame, text="Тип:").grid(row=2, column=0, sticky="e", pady=2, padx=5)
        self.proxy_scheme_var = tk.StringVar(value="http")
        self.proxy_scheme_combo = ctk.CTkComboBox(proxy_frame, variable=self.proxy_scheme_var,
                                                values=["http", "socks5"], width=80, state="readonly")
        self.proxy_scheme_combo.grid(row=2, column=1, padx=5, sticky="w")

        ctk.CTkLabel(proxy_frame, text="Хост:").grid(row=2, column=2, sticky="e", padx=(20, 0))
        self.proxy_host_entry = ctk.CTkEntry(proxy_frame)
        self.proxy_host_entry.grid(row=2, column=3, padx=5, sticky="ew")

        ctk.CTkLabel(proxy_frame, text="Порт:").grid(row=2, column=4, sticky="e")
        self.proxy_port_entry = ctk.CTkEntry(proxy_frame, width=8*8)
        self.proxy_port_entry.grid(row=2, column=5, padx=5, sticky="w")

        ctk.CTkLabel(proxy_frame, text="Логин:").grid(row=3, column=0, sticky="e", padx=5, pady=2)
        self.proxy_user_entry = ctk.CTkEntry(proxy_frame)
        self.proxy_user_entry.grid(row=3, column=1, padx=5, columnspan=2, sticky="ew", pady=2)

        ctk.CTkLabel(proxy_frame, text="Пароль:").grid(row=3, column=3, sticky="e", padx=(20, 0))
        self.proxy_pass_entry = ctk.CTkEntry(proxy_frame, show="*")
        self.proxy_pass_entry.grid(row=3, column=4, padx=5, columnspan=2, sticky="ew", pady=2)

        self.test_proxy_button = ctk.CTkButton(proxy_frame, text="Тест прокси", command=self.test_proxy)
        self.test_proxy_button.grid(row=4, column=0, columnspan=6, pady=(8, 5))

        self.proxy_status_label = ctk.CTkLabel(proxy_frame, text="", text_color="gray")
        self.proxy_status_label.grid(row=5, column=0, columnspan=6, padx=5, pady=(0, 5))

        telegram_frame = ctk.CTkFrame(settings_scroll, border_width=1)
        telegram_frame.grid(row=_settings_row, column=1, sticky="ew", padx=10, pady=5)
        _settings_row += 1
        telegram_frame.grid_columnconfigure(1, weight=1)
        ctk.CTkLabel(telegram_frame, text="Telegram уведомления", font=ctk.CTkFont(weight="bold")).grid(row=0, column=0, columnspan=2, pady=(5,0))

        ctk.CTkLabel(telegram_frame, text="Токен бота:").grid(row=1, column=0, sticky="e", pady=2, padx=5)
        self.telegram_token_entry = ctk.CTkEntry(telegram_frame)
        self.telegram_token_entry.grid(row=1, column=1, padx=5, pady=2, sticky="ew")

        ctk.CTkLabel(telegram_frame, text="Chat ID:").grid(row=2, column=0, sticky="e", pady=2, padx=5)
        self.telegram_chat_id_entry = ctk.CTkEntry(telegram_frame)
        self.telegram_chat_id_entry.grid(row=2, column=1, padx=5, pady=2, sticky="ew")

        # Отдельный прокси для Telegram
        tg_proxy_sub = ctk.CTkFrame(telegram_frame, border_width=1)
        tg_proxy_sub.grid(row=3, column=0, columnspan=2, sticky="ew", padx=5, pady=(10, 5))
        tg_proxy_sub.grid_columnconfigure(1, weight=1)
        tg_proxy_sub.grid_columnconfigure(3, weight=1)
        tg_proxy_sub.grid_columnconfigure(5, weight=1)

        ctk.CTkLabel(tg_proxy_sub, text="Прокси для Telegram (необязательно)", font=ctk.CTkFont(weight="bold")).grid(row=0, column=0, columnspan=6, pady=(5,0))
        ctk.CTkLabel(
            tg_proxy_sub,
            text="только зарубежный (РКН блочит api.telegram.org)",
            text_color="gray",
            font=ctk.CTkFont(size=11),
        ).grid(row=1, column=0, columnspan=6, pady=(0, 5))

        ctk.CTkLabel(tg_proxy_sub, text="Тип:").grid(row=2, column=0, sticky="e", pady=2, padx=5)
        self.tg_proxy_scheme_var = tk.StringVar(value="http")
        self.tg_proxy_scheme_combo = ctk.CTkComboBox(
            tg_proxy_sub, variable=self.tg_proxy_scheme_var,
            values=["http", "socks5"], width=80, state="readonly"
        )
        self.tg_proxy_scheme_combo.grid(row=2, column=1, padx=5, sticky="w")

        ctk.CTkLabel(tg_proxy_sub, text="Хост:").grid(row=2, column=2, sticky="e", padx=(20, 0))
        self.tg_proxy_host_entry = ctk.CTkEntry(tg_proxy_sub)
        self.tg_proxy_host_entry.grid(row=2, column=3, padx=5, sticky="ew")

        ctk.CTkLabel(tg_proxy_sub, text="Порт:").grid(row=2, column=4, sticky="e")
        self.tg_proxy_port_entry = ctk.CTkEntry(tg_proxy_sub, width=8*8)
        self.tg_proxy_port_entry.grid(row=2, column=5, padx=5, sticky="w")

        ctk.CTkLabel(tg_proxy_sub, text="Логин:").grid(row=3, column=0, sticky="e", padx=5, pady=2)
        self.tg_proxy_user_entry = ctk.CTkEntry(tg_proxy_sub)
        self.tg_proxy_user_entry.grid(row=3, column=1, padx=5, columnspan=2, sticky="ew", pady=2)

        ctk.CTkLabel(tg_proxy_sub, text="Пароль:").grid(row=3, column=3, sticky="e", padx=(20, 0))
        self.tg_proxy_pass_entry = ctk.CTkEntry(tg_proxy_sub, show="*")
        self.tg_proxy_pass_entry.grid(row=3, column=4, padx=5, columnspan=2, sticky="ew", pady=2)

        self.test_telegram_button = ctk.CTkButton(tg_proxy_sub, text="Тест прокси (Telegram)", command=self.test_telegram)
        self.test_telegram_button.grid(row=4, column=0, columnspan=6, pady=(8, 5))

        self.telegram_status_label = ctk.CTkLabel(telegram_frame, text="", text_color="gray")
        self.telegram_status_label.grid(row=4, column=0, columnspan=2, padx=5, pady=(5, 5))

        # Уведомления о статусе парсера
        ctk.CTkLabel(telegram_frame, text="Уведомления о статусе:").grid(row=5, column=0, sticky="e", pady=(10, 5), padx=5)
        self.tg_notify_status_var = tk.BooleanVar(value=True)
        self.tg_notify_status_cb = ctk.CTkCheckBox(
            telegram_frame, text="Слать старт/стоп/ошибки в Telegram",
            variable=self.tg_notify_status_var,
        )
        self.tg_notify_status_cb.grid(row=5, column=1, padx=5, sticky="w", pady=(10, 5))

        schedule_frame = ctk.CTkFrame(settings_scroll, border_width=1)
        schedule_frame.grid(row=_settings_row, column=1, sticky="ew", padx=10, pady=5)
        _settings_row += 1
        ctk.CTkLabel(schedule_frame, text="Расписание работы", font=ctk.CTkFont(weight="bold")).grid(row=0, column=0, columnspan=6, pady=(5,0))

        self.schedule_enabled_var = tk.BooleanVar(value=False)
        ctk.CTkCheckBox(
            schedule_frame, text="Работать только по расписанию",
            variable=self.schedule_enabled_var,
        ).grid(row=1, column=0, columnspan=6, sticky="w", pady=2, padx=5)

        ctk.CTkLabel(schedule_frame, text="Начало (ЧЧ:ММ):").grid(row=2, column=0, sticky="w", pady=2, padx=5)
        self.schedule_start_entry = ctk.CTkEntry(schedule_frame, width=8*8)
        self.schedule_start_entry.grid(row=2, column=1, padx=5, sticky="w")
        self.schedule_start_entry.insert(0, "09:00")

        ctk.CTkLabel(schedule_frame, text="Окончание (ЧЧ:ММ):").grid(row=2, column=2, sticky="w", padx=(20, 0))
        self.schedule_end_entry = ctk.CTkEntry(schedule_frame, width=8*8)
        self.schedule_end_entry.grid(row=2, column=3, padx=5, sticky="w")
        self.schedule_end_entry.insert(0, "21:00")

        ctk.CTkLabel(schedule_frame, text="Дни недели:").grid(row=3, column=0, sticky="w", pady=(8, 2), padx=5)
        days_row = ctk.CTkFrame(schedule_frame)
        days_row.grid(row=3, column=1, columnspan=6, sticky="w", pady=(8, 2))

        self.schedule_day_vars = []
        day_names = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
        for i, dname in enumerate(day_names):
            var = tk.BooleanVar(value=True)
            self.schedule_day_vars.append(var)
            ctk.CTkCheckBox(days_row, text=dname, variable=var).pack(side="left", padx=2)

        browser_frame = ctk.CTkFrame(settings_scroll, border_width=1)
        browser_frame.grid(row=_settings_row, column=1, sticky="ew", padx=10, pady=5)
        _settings_row += 1
        ctk.CTkLabel(browser_frame, text="Браузер", font=ctk.CTkFont(weight="bold")).grid(row=0, column=0, columnspan=2, pady=(5, 0))
        self.show_browser_var = tk.BooleanVar(value=False)
        ctk.CTkCheckBox(
            browser_frame, text="Показывать окно браузера (для диагностики капчи)",
            variable=self.show_browser_var,
        ).grid(row=1, column=0, sticky="w", padx=10, pady=(4, 8))

        save_frame = ctk.CTkFrame(settings_scroll, fg_color="transparent")
        save_frame.grid(row=_settings_row, column=1, sticky="ew", padx=10, pady=10)
        _settings_row += 1
        self.save_button = ctk.CTkButton(save_frame, text="💾 Запомнить настройки", command=self.save_settings)
        self.save_button.pack(anchor="center", pady=5)

        # ========== Вкладка "Профили" ==========
        tab_profiles = self.notebook.add("Профили")

        profiles_left = ctk.CTkFrame(tab_profiles, border_width=1)
        profiles_left.pack(side="left", fill="y", padx=(10, 5), pady=10)
        ctk.CTkLabel(profiles_left, text="Список профилей", font=ctk.CTkFont(weight="bold")).pack(pady=(5,0))

        self.profiles_listbox = tk.Listbox(profiles_left, width=30, height=20, bg="#2b2b2b", fg="white", borderwidth=0, highlightthickness=0)
        self.profiles_listbox.pack(side="left", fill="y", padx=5, pady=5)
        self.profiles_listbox.bind("<<ListboxSelect>>", self.on_profile_select)

        profiles_scroll = tk.Scrollbar(profiles_left, orient="vertical", command=self.profiles_listbox.yview)
        profiles_scroll.pack(side="right", fill="y")
        self.profiles_listbox.configure(yscrollcommand=profiles_scroll.set)

        profiles_right = ctk.CTkFrame(tab_profiles, border_width=1)
        profiles_right.pack(side="left", fill="both", expand=True, padx=5, pady=10)
        ctk.CTkLabel(profiles_right, text="Параметры профиля", font=ctk.CTkFont(weight="bold")).grid(row=0, column=0, columnspan=2, pady=(5,0))

        ctk.CTkLabel(profiles_right, text="Название:").grid(row=1, column=0, sticky="w", pady=2, padx=5)
        self.profile_name_entry = ctk.CTkEntry(profiles_right, width=30*8)
        self.profile_name_entry.grid(row=1, column=1, padx=5, pady=2, sticky="w")

        ctk.CTkLabel(profiles_right, text="Запрос:").grid(row=2, column=0, sticky="w", pady=2, padx=5)
        self.profile_query_entry = ctk.CTkEntry(profiles_right, width=30*8)
        self.profile_query_entry.grid(row=2, column=1, padx=5, pady=2, sticky="w")

        ctk.CTkLabel(profiles_right, text="Игнор:").grid(row=3, column=0, sticky="w", pady=2, padx=5)
        self.profile_ignore_entry = ctk.CTkEntry(profiles_right, width=30*8,
                                                  placeholder_text="через запятую: 3s, б/у")
        self.profile_ignore_entry.grid(row=3, column=1, padx=5, pady=2, sticky="w")

        ctk.CTkLabel(profiles_right, text="Город:").grid(row=4, column=0, sticky="w", pady=2, padx=5)
        self.profile_city_var = tk.StringVar(value="Москва")
        self.profile_city_combo = ctk.CTkComboBox(
            profiles_right, variable=self.profile_city_var, values=CITIES, width=27*8, state="readonly"
        )
        self.profile_city_combo.grid(row=4, column=1, padx=5, pady=2, sticky="w")

        ctk.CTkLabel(profiles_right, text="Цена от:").grid(row=5, column=0, sticky="w", pady=2, padx=5)
        self.profile_min_price_entry = ctk.CTkEntry(profiles_right, width=12*8)
        self.profile_min_price_entry.grid(row=5, column=1, padx=5, pady=2, sticky="w")

        ctk.CTkLabel(profiles_right, text="Цена до:").grid(row=6, column=0, sticky="w", pady=2, padx=5)
        self.profile_max_price_entry = ctk.CTkEntry(profiles_right, width=12*8)
        self.profile_max_price_entry.grid(row=6, column=1, padx=5, pady=2, sticky="w")

        ctk.CTkLabel(profiles_right, text="Интервал от (мин):").grid(row=7, column=0, sticky="w", pady=2, padx=5)
        self.profile_min_interval_entry = ctk.CTkEntry(profiles_right, width=12*8)
        self.profile_min_interval_entry.grid(row=7, column=1, padx=5, pady=2, sticky="w")
        self.profile_min_interval_entry.insert(0, "1")

        ctk.CTkLabel(profiles_right, text="Интервал до (мин):").grid(row=8, column=0, sticky="w", pady=2, padx=5)
        self.profile_max_interval_entry = ctk.CTkEntry(profiles_right, width=12*8)
        self.profile_max_interval_entry.grid(row=8, column=1, padx=5, pady=2, sticky="w")
        self.profile_max_interval_entry.insert(0, "3")

        self.profile_delivery_var = tk.BooleanVar(value=False)
        ctk.CTkCheckBox(
            profiles_right, text="Авито доставка", variable=self.profile_delivery_var,
        ).grid(row=9, column=1, padx=5, pady=2, sticky="w")

        self.profile_filter_services_var = tk.BooleanVar(value=False)
        ctk.CTkCheckBox(
            profiles_right, text="Убрать услуги", variable=self.profile_filter_services_var,
        ).grid(row=10, column=1, padx=5, pady=2, sticky="w")

        profiles_buttons = ctk.CTkFrame(profiles_right)
        profiles_buttons.grid(row=11, column=0, columnspan=2, pady=10, sticky="w")

        ctk.CTkButton(profiles_buttons, text="📥 Загрузить в поиск", command=self.profile_load_to_search).pack(side="left", padx=2)
        ctk.CTkButton(profiles_buttons, text="➕ Новый", command=self.profile_new).pack(side="left", padx=2)
        ctk.CTkButton(profiles_buttons, text="💾 Сохранить", command=self.profile_save).pack(side="left", padx=2)
        ctk.CTkButton(profiles_buttons, text="🗑 Удалить", command=self.profile_delete).pack(side="left", padx=2)
        ctk.CTkButton(profiles_buttons, text="✔ Сделать активным", command=self.profile_set_active).pack(side="left", padx=2)

        self.profile_status_label = ctk.CTkLabel(profiles_right, text="", text_color="gray")
        self.profile_status_label.grid(row=11, column=0, columnspan=2, sticky="w", padx=5, pady=(10, 0))

        self._current_profile_id = None

        # ========== Вкладка "Инструкция" ==========
        tab_instructions = self.notebook.add("Инструкция")

        self.instructions_text = ctk.CTkTextbox(tab_instructions, wrap="word", font=ctk.CTkFont(size=12))
        self.instructions_text.pack(fill="both", expand=True, padx=10, pady=10)

        instruction_content = """# 🔧 Инструкция по настройке парсера Avito

## 1. 🌐 Прокси (на примере сервиса mobileproxy.space)

### 1.1. Покупка прокси
1. Перейдите на mobileproxy.space и выберите подходящий тариф.
   - Тип: HTTP или SOCKS5 (рекомендуется HTTP).
   - Страна, срок действия - по вашим задачам, страна "Россия", другие IP могут быть заблокированы.
2. После оплаты зайдите в личный кабинет, раздел «Мои прокси».

### 1.2. Привязка IP-адреса
> 🔒 Обязательный шаг - без привязки IP прокси работать не будет.

1. Найдите купленный прокси в списке и отметьте его галочкой.
2. Нажмите кнопку «Bind IP» (Привязать IP).
3. В открывшемся окне кликните на ссылку с вашим текущим IP.
   Адрес автоматически подставится в текстовое поле.
4. Нажмите «Сохранить».

### 1.3. Получение данных для подключения
В личном кабинете найдите параметры своего прокси:
- Хост (например: z.mobilespace.proxy)
- Порт (например: 12345)
- Логин (если есть)
- Пароль (если есть)

Логин и пароль часто передаются одной строкой через двоеточие:
yR1ByZ:paNHYV8EM7su - до двоеточия логин, после - пароль.

### 1.4. Настройка в программе
1. Откройте вкладку «Настройки» -> раздел «Прокси».
2. Заполните поля: Тип, Хост, Порт, Логин/Пароль.
3. Нажмите «Тест прокси».

## 2. 📬 Telegram-уведомления

### 2.1. Создание бота
1. В Telegram найдите @BotFather и запустите его.
2. Отправьте /newbot и следуйте инструкциям.
3. Сохраните полученный токен.

### 2.2. Получение Chat ID
- Напишите боту @WhatChatIDBot любое сообщение.
- Бот ответит вашим Chat ID.

### 2.3. Настройка в программе
1. На вкладке «Настройки» -> «Telegram уведомления» вставьте токен и Chat ID.
2. Нажмите «Тест».

## 3. 💾 Сохраните настройки
После заполнения всех полей нажмите «💾 Запомнить настройки».
"""
        self.instructions_text.insert('1.0', instruction_content)

        # Tag configuration might not work directly in CTkTextbox the same way as in tk.Text.
        # CTkTextbox doesn't support tag_configure/tag_add like tk.Text.
        # For simplicity, we skip rich formatting in CTkTextbox if not supported or just leave as plain text.
        # But instructions said to keep behavior. CTkTextbox doesn't support tags.
        # I will keep the code but it might not have effect or might need a different approach.
        # Actually, if I want to keep rich text I might need to use a different widget, but rules say use CTkTextbox.
        
        # self.instructions_text.tag_configure("heading1", font=ctk.CTkFont(size=14, weight='bold'), foreground='#2E86C1')
        # ...

        self.instructions_text.configure(state='disabled')

    # ---------- Прокрутка колёсиком ----------
    def _on_mousewheel(self, event):
        self.canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")

    def _on_mousewheel_linux(self, event):
        if event.num == 4:
            self.canvas.yview_scroll(-1, "units")
        elif event.num == 5:
            self.canvas.yview_scroll(1, "units")

    # ---------- Настройки ----------
    def load_settings(self):
        if not os.path.exists(SETTINGS_FILE):
            self.log("ℹ️ Файл настроек не найден, используйте поля ввода")
            return
        s = load_app_settings(SETTINGS_FILE)

        self.telegram_token_entry.delete(0, tk.END)
        self.telegram_token_entry.insert(0, s.telegram_token)
        self.telegram_chat_id_entry.delete(0, tk.END)
        self.telegram_chat_id_entry.insert(0, s.telegram_chat_id)
        self.proxy_scheme_var.set(s.proxy_scheme)
        self.proxy_host_entry.delete(0, tk.END)
        self.proxy_host_entry.insert(0, s.proxy_host)
        self.proxy_port_entry.delete(0, tk.END)
        self.proxy_port_entry.insert(0, s.proxy_port)
        self.proxy_user_entry.delete(0, tk.END)
        self.proxy_user_entry.insert(0, s.proxy_user)
        self.proxy_pass_entry.delete(0, tk.END)
        self.proxy_pass_entry.insert(0, s.proxy_pass)

        self.tg_proxy_scheme_var.set(s.tg_proxy_scheme)
        self.tg_proxy_host_entry.delete(0, tk.END)
        self.tg_proxy_host_entry.insert(0, s.tg_proxy_host)
        self.tg_proxy_port_entry.delete(0, tk.END)
        self.tg_proxy_port_entry.insert(0, s.tg_proxy_port)
        self.tg_proxy_user_entry.delete(0, tk.END)
        self.tg_proxy_user_entry.insert(0, s.tg_proxy_user)
        self.tg_proxy_pass_entry.delete(0, tk.END)
        self.tg_proxy_pass_entry.insert(0, s.tg_proxy_pass)

        self.tg_notify_status_var.set(s.tg_notify_status)

        self.schedule_enabled_var.set(s.schedule_enabled)
        self.schedule_start_entry.delete(0, tk.END)
        self.schedule_start_entry.insert(0, s.schedule_start)
        self.schedule_end_entry.delete(0, tk.END)
        self.schedule_end_entry.insert(0, s.schedule_end)
        for i, v in enumerate(s.schedule_days):
            self.schedule_day_vars[i].set(bool(v))

        self.max_items = s.max_items
        self.show_browser_var.set(s.show_browser)
        self.log("✅ Настройки загружены")

    def save_settings(self):
        s = AppSettings(
            telegram_token=self.telegram_token_entry.get().strip(),
            telegram_chat_id=self.telegram_chat_id_entry.get().strip(),
            proxy_scheme=self.proxy_scheme_var.get(),
            proxy_host=self.proxy_host_entry.get().strip(),
            proxy_port=self.proxy_port_entry.get().strip(),
            proxy_user=self.proxy_user_entry.get().strip(),
            proxy_pass=self.proxy_pass_entry.get().strip(),
            tg_proxy_scheme=self.tg_proxy_scheme_var.get(),
            tg_proxy_host=self.tg_proxy_host_entry.get().strip(),
            tg_proxy_port=self.tg_proxy_port_entry.get().strip(),
            tg_proxy_user=self.tg_proxy_user_entry.get().strip(),
            tg_proxy_pass=self.tg_proxy_pass_entry.get().strip(),
            tg_notify_status=bool(self.tg_notify_status_var.get()),
            schedule_enabled=bool(self.schedule_enabled_var.get()),
            schedule_start=self.schedule_start_entry.get().strip() or "09:00",
            schedule_end=self.schedule_end_entry.get().strip() or "21:00",
            schedule_days=[bool(v.get()) for v in self.schedule_day_vars],
            max_items=self.max_items,
            show_browser=bool(self.show_browser_var.get()),
        )
        if save_app_settings(s, SETTINGS_FILE):
            self.log("✅ Настройки сохранены")
        else:
            self.log("❌ Ошибка сохранения настроек")

    # ---------- Canvas ----------
    def on_frame_configure(self, event):
        self.canvas.configure(scrollregion=self.canvas.bbox("all"))

    def on_canvas_configure(self, event):
        self.canvas.itemconfig("window", width=event.width)

    # ---------- Обработчики ----------
    def on_city_change(self, event):
        if self.city_var.get() == "Вся Россия":
            self.all_russia_var.set(True)
        else:
            self.all_russia_var.set(False)

    def on_all_russia(self):
        if self.all_russia_var.get():
            self.city_var.set("Вся Россия")
        else:
            self.city_var.set("Москва")

    def log(self, message):
        # Всегда сначала в файл/stdout - это thread-safe
        logger.info(message)
        # Запись в виджет - только из main thread через after()
        try:
            self.root.after(0, self._log_on_main, message)
        except Exception:
            # Если root уже уничтожен (при закрытии) - просто тихо пропускаем
            pass

    def _log_on_main(self, message):
        tb = self.log_text._textbox
        tb.configure(state="normal")
        # Ставим mark перед вставкой - gravity="left" чтобы не ехал вместе с текстом
        tb.mark_set("_log_ins", tk.END + "-1c")
        tb.mark_gravity("_log_ins", "left")
        tb.insert(tk.END, message + "\n")
        # Подсвечиваем URL как ссылки
        import re as _re
        for m in _re.finditer(r"https?://\S+", message):
            link_start = f"_log_ins+{m.start()}c"
            link_end = f"_log_ins+{m.end()}c"
            tb.tag_add("link", link_start, link_end)
        try:
            tb.see(tk.END)
        except Exception:
            pass
        tb.configure(state="disabled")

    def _on_log_link_click(self, event):
        tb = self.log_text._textbox
        idx = tb.index(f"@{event.x},{event.y}")
        # Находим границы тега link под курсором
        ranges = tb.tag_prevrange("link", idx + "+1c")
        if not ranges:
            return
        url = tb.get(ranges[0], ranges[1])
        try:
            self.root.clipboard_clear()
            self.root.clipboard_append(url)
            self.root.update()
            # Тост у курсора - закрывается сам через секунду
            self._show_toast("📋 Скопировано", event.x_root, event.y_root)
        except Exception as e:
            logger.error(f"Не удалось скопировать ссылку: {e}")

    def _show_toast(self, text, x=None, y=None, duration_ms=1000):
        """Показывает мини-окошко с текстом, автоматически закрывается через duration_ms."""
        try:
            toast = ctk.CTkToplevel(self.root)
            toast.overrideredirect(True)
            toast.attributes("-topmost", True)
            try:
                toast.attributes("-alpha", 0.92)
            except Exception:
                pass
            lbl = ctk.CTkLabel(
                toast, text=text,
                font=ctk.CTkFont(size=12, weight="bold"),
                fg_color="#2b5a2b", corner_radius=8,
                padx=12, pady=6,
            )
            lbl.pack()
            toast.update_idletasks()
            w, h = toast.winfo_width(), toast.winfo_height()
            if x is None or y is None:
                px = self.root.winfo_rootx() + self.root.winfo_width() - w - 20
                py = self.root.winfo_rooty() + self.root.winfo_height() - h - 40
            else:
                px, py = int(x) + 10, int(y) + 10
            toast.geometry(f"+{px}+{py}")
            toast.after(duration_ms, toast.destroy)
        except Exception as e:
            logger.error(f"Не удалось показать тост: {e}")

    def set_status(self, text, counter=None):
        """Обновляет текст статусбара внизу окна."""
        try:
            self.status_var.set(text)
            if counter is not None:
                self.status_counter_var.set(counter)
            self.root.update_idletasks()
        except Exception:
            pass

    def _get_proxy_settings(self):
        return {
            'scheme': self.proxy_scheme_var.get(),
            'host': self.proxy_host_entry.get().strip(),
            'port': self.proxy_port_entry.get().strip(),
            'user': self.proxy_user_entry.get().strip(),
            'pass': self.proxy_pass_entry.get().strip(),
        }

    def _get_tg_proxies_dict(self):
        """Возвращает словарь proxies для requests на основе полей TG-прокси, либо None."""
        host = self.tg_proxy_host_entry.get().strip()
        port = self.tg_proxy_port_entry.get().strip()
        if not host or not port:
            return None
        scheme = self.tg_proxy_scheme_var.get() or "http"
        user = self.tg_proxy_user_entry.get().strip()
        pwd = self.tg_proxy_pass_entry.get().strip()
        if user and pwd:
            url = f"{scheme}://{user}:{pwd}@{host}:{port}"
        else:
            url = f"{scheme}://{host}:{port}"
        return {"http": url, "https": url}

    def send_tg_status(self, text):
        """Шлёт короткое статусное сообщение в TG, если включено в настройках."""
        if not self.update_telegram_notifier():
            return False
        return self.notifier.send_status(text, status_enabled=bool(self.tg_notify_status_var.get()))

    def send_error_telegram(self, error_text):
        if not self.update_telegram_notifier():
            return False
        return self.notifier.send_error(error_text)

    # ---------- Тесты ----------
    def test_telegram(self):
        token = self.telegram_token_entry.get().strip()
        chat_id = self.telegram_chat_id_entry.get().strip()
        if not token:
            self.telegram_status_label.configure(text="❌ Токен не указан", text_color="red")
            return
        proxies = self._get_tg_proxies_dict()
        notifier = TelegramNotifier(token, chat_id, proxies=proxies)
        ok, msg = notifier.test_connection()
        if ok:
            self.telegram_status_label.configure(text="✅ Бот доступен", text_color="green")
            if chat_id:
                test_text = "🔔 Тестовое сообщение от парсера Avito"
                if notifier.send_message(test_text):
                    self.telegram_status_label.configure(text="✅ Тест отправлен", text_color="green")
                else:
                    self.telegram_status_label.configure(text="❌ Ошибка отправки", text_color="red")
            else:
                self.telegram_status_label.configure(text="✅ Бот доступен, укажите Chat ID", text_color="orange")
        else:
            self.telegram_status_label.configure(text=f"❌ {msg}", text_color="red")

    def update_telegram_notifier(self):
        token = self.telegram_token_entry.get().strip()
        chat_id = self.telegram_chat_id_entry.get().strip()
        proxies = self._get_tg_proxies_dict()
        return self.notifier.configure(token, chat_id, proxies=proxies)

    def test_proxy(self):
        scheme = self.proxy_scheme_var.get()
        host = self.proxy_host_entry.get().strip()
        port = self.proxy_port_entry.get().strip()
        user = self.proxy_user_entry.get().strip()
        pwd = self.proxy_pass_entry.get().strip()
        if not host or not port:
            self.proxy_status_label.configure(text="❌ Укажите хост и порт", text_color="red")
            return
        proxy_url = f"{scheme}://{user}:{pwd}@{host}:{port}" if user and pwd else f"{scheme}://{host}:{port}"
        proxies = {"http": proxy_url, "https": proxy_url}
        try:
            r = requests.get("https://httpbin.org/ip", proxies=proxies, timeout=10)
            if r.status_code == 200:
                ip = r.json()["origin"]
                self.proxy_status_label.configure(text=f"✅ Прокси работает, ваш IP: {ip}", text_color="green")
            else:
                self.proxy_status_label.configure(text=f"❌ Ошибка: {r.status_code}", text_color="red")
        except Exception as e:
            self.proxy_status_label.configure(text=f"❌ Ошибка: {str(e)}", text_color="red")
            logger.error(f"Ошибка теста прокси: {e}")

    # ---------- Профили ----------
    def refresh_profiles_list(self):
        self.profiles_listbox.delete(0, tk.END)
        self._profile_ids_in_list = []
        try:
            profiles = database.list_search_profiles()
        except Exception as e:
            self.log(f"⚠️ Не удалось загрузить профили: {e}")
            return
        for p in profiles:
            marker = "★ " if p["is_active"] else "   "
            self.profiles_listbox.insert(tk.END, f"{marker}{p['name']} ({p['city']})")
            self._profile_ids_in_list.append(p["id"])

    def _clear_profile_form(self):
        self._current_profile_id = None
        self.profile_name_entry.delete(0, tk.END)
        self.profile_query_entry.delete(0, tk.END)
        self.profile_ignore_entry.delete(0, tk.END)
        self.profile_city_var.set("Москва")
        self.profile_min_price_entry.delete(0, tk.END)
        self.profile_max_price_entry.delete(0, tk.END)
        self.profile_min_interval_entry.delete(0, tk.END)
        self.profile_min_interval_entry.insert(0, "1")
        self.profile_max_interval_entry.delete(0, tk.END)
        self.profile_max_interval_entry.insert(0, "3")
        self.profile_delivery_var.set(False)
        self.profile_filter_services_var.set(False)

    def _fill_profile_form(self, profile):
        self._current_profile_id = profile["id"]
        filters = profile.get("filters") or {}
        self.profile_name_entry.delete(0, tk.END)
        self.profile_name_entry.insert(0, profile["name"] or "")
        self.profile_query_entry.delete(0, tk.END)
        self.profile_query_entry.insert(0, filters.get("query", ""))
        self.profile_ignore_entry.delete(0, tk.END)
        self.profile_ignore_entry.insert(0, filters.get("ignore", ""))
        self.profile_city_var.set(profile["city"] or "Москва")
        self.profile_min_price_entry.delete(0, tk.END)
        if filters.get("min_price") is not None:
            self.profile_min_price_entry.insert(0, str(filters.get("min_price")))
        self.profile_max_price_entry.delete(0, tk.END)
        if filters.get("max_price") is not None:
            self.profile_max_price_entry.insert(0, str(filters.get("max_price")))
        self.profile_min_interval_entry.delete(0, tk.END)
        self.profile_min_interval_entry.insert(0, str(filters.get("min_interval", 1)))
        self.profile_max_interval_entry.delete(0, tk.END)
        self.profile_max_interval_entry.insert(0, str(filters.get("max_interval", 3)))
        self.profile_delivery_var.set(bool(filters.get("delivery", False)))
        self.profile_filter_services_var.set(bool(filters.get("filter_services", False)))

    def on_profile_select(self, event):
        sel = self.profiles_listbox.curselection()
        if not sel:
            return
        idx = sel[0]
        ids = getattr(self, "_profile_ids_in_list", [])
        if idx >= len(ids):
            return
        profile = database.get_search_profile(ids[idx])
        if profile:
            self._fill_profile_form(profile)

    def profile_new(self):
        self._clear_profile_form()
        self.profile_status_label.configure(text="Новый профиль - заполните поля и нажмите «Сохранить»", text_color="gray")

    def _collect_profile_from_form(self):
        name = self.profile_name_entry.get().strip()
        query = self.profile_query_entry.get().strip()
        ignore = self.profile_ignore_entry.get().strip()
        city = self.profile_city_var.get().strip() or "Москва"
        min_price_str = self.profile_min_price_entry.get().strip()
        max_price_str = self.profile_max_price_entry.get().strip()
        min_interval_str = self.profile_min_interval_entry.get().strip() or "1"
        max_interval_str = self.profile_max_interval_entry.get().strip() or "3"

        if not name:
            raise ValueError("Название профиля не может быть пустым")
        if not query:
            raise ValueError("Запрос не может быть пустым")
        try:
            min_price = int(min_price_str) if min_price_str else 0
            max_price = int(max_price_str) if max_price_str else 999999999
        except ValueError:
            raise ValueError("Цена должна быть числом")
        try:
            min_interval = float(min_interval_str)
            max_interval = float(max_interval_str)
        except ValueError:
            raise ValueError("Интервал должен быть числом")

        filters = {
            "query": query,
            "ignore": ignore,
            "min_price": min_price,
            "max_price": max_price,
            "min_interval": min_interval,
            "max_interval": max_interval,
            "delivery": bool(self.profile_delivery_var.get()),
            "filter_services": bool(self.profile_filter_services_var.get()),
        }
        return name, city, filters

    def profile_save(self):
        try:
            name, city, filters = self._collect_profile_from_form()
        except ValueError as e:
            self.profile_status_label.configure(text=f"❌ {e}", text_color="red")
            return
        try:
            if self._current_profile_id is None:
                new_id = database.create_search_profile(name, city, filters)
                self._current_profile_id = new_id
                self.profile_status_label.configure(text=f"✅ Профиль «{name}» создан", text_color="green")
            else:
                database.update_search_profile(
                    self._current_profile_id, name=name, city=city, filters=filters,
                )
                self.profile_status_label.configure(text=f"✅ Профиль «{name}» обновлён", text_color="green")
        except Exception as e:
            self.profile_status_label.configure(text=f"❌ Ошибка: {e}", text_color="red")
            logger.error(f"Ошибка сохранения профиля: {e}")
            return
        self.refresh_profiles_list()

    def profile_delete(self):
        if self._current_profile_id is None:
            self.profile_status_label.configure(text="❌ Профиль не выбран", text_color="red")
            return
        if not messagebox.askyesno("Удалить профиль", "Вы уверены, что хотите удалить этот профиль?"):
            return
        try:
            database.delete_search_profile(self._current_profile_id)
            self.profile_status_label.configure(text="✅ Профиль удалён", text_color="green")
            self._clear_profile_form()
            self.refresh_profiles_list()
        except Exception as e:
            self.profile_status_label.configure(text=f"❌ Ошибка: {e}", text_color="red")

    def profile_set_active(self):
        if self._current_profile_id is None:
            self.profile_status_label.configure(text="❌ Профиль не выбран", text_color="red")
            return
        try:
            database.set_active_profile(self._current_profile_id)
            self.profile_status_label.configure(text="✅ Профиль сделан активным", text_color="green")
            self.refresh_profiles_list()
        except Exception as e:
            self.profile_status_label.configure(text=f"❌ Ошибка: {e}", text_color="red")

    def save_current_search_as_profile(self):
        """Сохраняет текущие параметры поиска (главная вкладка) как новый профиль."""
        query = self.query_entry.get().strip()
        if not query:
            messagebox.showwarning("Сохранение профиля", "Сначала заполните поле «Запрос».")
            return
        default_name = query[:30]
        name = simpledialog.askstring(
            "Название профиля", "Как назвать профиль?", initialvalue=default_name, parent=self.root,
        )
        if not name:
            return
        name = name.strip()
        if not name:
            return

        city = "Вся Россия" if self.all_russia_var.get() else (self.city_var.get() or "Москва")
        min_price_str = self.min_price_entry.get().strip()
        max_price_str = self.max_price_entry.get().strip()
        try:
            min_price = int(min_price_str) if min_price_str else 0
            max_price = int(max_price_str) if max_price_str else 999999999
        except ValueError:
            messagebox.showerror("Ошибка", "Цены должны быть числами.")
            return
        try:
            min_interval = float(self.min_interval.get().strip() or "1")
            max_interval = float(self.max_interval.get().strip() or "3")
        except ValueError:
            messagebox.showerror("Ошибка", "Интервал должен быть числом.")
            return

        filters = {
            "query": query,
            "ignore": self.ignore_entry.get().strip(),
            "min_price": min_price,
            "max_price": max_price,
            "min_interval": min_interval,
            "max_interval": max_interval,
            "delivery": bool(self.delivery_var.get()),
            "filter_services": bool(self.filter_services_var.get()),
        }
        try:
            new_id = database.create_search_profile(name, city, filters)
        except Exception as e:
            messagebox.showerror("Ошибка", f"Не удалось создать профиль: {e}")
            logger.error(f"Ошибка создания профиля из поиска: {e}")
            return

        self.refresh_profiles_list()
        self._current_profile_id = new_id
        self.log(f"✅ Профиль «{name}» создан из текущего поиска")
        messagebox.showinfo("Профиль сохранён", f"Профиль «{name}» добавлен во вкладку «Профили».")

    def _apply_active_profile_on_startup(self):
        """Если в БД есть активный профиль - подгружает его параметры во вкладку поиска."""
        try:
            active = database.get_active_profile()
        except Exception as e:
            logger.error(f"Ошибка загрузки активного профиля: {e}")
            return
        if active:
            self._apply_profile_to_search_tab(active)
            self.log(f"✅ Загружен активный профиль: {active['name']}")

    def profile_load_to_search(self):
        """Переносит параметры текущего профиля во вкладку «Результаты поиска»."""
        if self._current_profile_id is None:
            self.profile_status_label.configure(text="❌ Профиль не выбран", text_color="red")
            return
        profile = database.get_search_profile(self._current_profile_id)
        if not profile:
            return
        self._apply_profile_to_search_tab(profile)
        self.profile_status_label.configure(
            text=f"✅ Профиль «{profile['name']}» загружен во вкладку поиска", text_color="green",
        )
        self.notebook.set("Результаты поиска")

    def _apply_profile_to_search_tab(self, profile):
        filters = profile.get("filters") or {}
        self.query_entry.delete(0, tk.END)
        self.query_entry.insert(0, filters.get("query", ""))
        self.ignore_entry.delete(0, tk.END)
        self.ignore_entry.insert(0, filters.get("ignore", ""))
        self.min_price_entry.delete(0, tk.END)
        if filters.get("min_price") is not None:
            self.min_price_entry.insert(0, str(filters.get("min_price")))
        self.max_price_entry.delete(0, tk.END)
        if filters.get("max_price") is not None:
            self.max_price_entry.insert(0, str(filters.get("max_price")))
        self.min_interval.delete(0, tk.END)
        self.min_interval.insert(0, str(filters.get("min_interval", 1)))
        self.max_interval.delete(0, tk.END)
        self.max_interval.insert(0, str(filters.get("max_interval", 3)))
        self.delivery_var.set(bool(filters.get("delivery", False)))
        self.filter_services_var.set(bool(filters.get("filter_services", False)))
        city = profile.get("city") or "Москва"
        if city == "Вся Россия":
            self.all_russia_var.set(True)
            self.city_var.set("Вся Россия")
        else:
            self.all_russia_var.set(False)
            self.city_var.set(city)

    # ---------- Капча ----------
    def _is_captcha_page(self, driver):
        """Похоже ли текущая страница на стену капчи/firewall."""
        try:
            src = (driver.page_source or "").lower()
        except Exception:
            return False
        markers = [
            "captcha",
            "firewall",
            "доступ ограничен",
            "подтвердите, что вы не робот",
            "access-confirm",
            "are you a robot",
        ]
        return any(m in src for m in markers)

    def _recover_from_captcha(self, proxy_settings, show_browser: bool = False):
        """Открываем видимый браузер, ждём пока пользователь решит капчу,
        затем возвращаемся в headless. Куки решения сохраняются через
        общий user-data-dir, поэтому следующий цикл парсит без стены."""
        if self._captcha_recovery_in_progress:
            self.log("🚧 Восстановление после капчи уже идёт - пропускаю")
            return False
        self._captcha_recovery_in_progress = True
        try:
            self.log("🚧 Похоже на капчу - открываю видимый браузер")
            self.set_status("🚧 Решите капчу в открывшемся окне")

            try:
                self.driver_manager.cleanup()
            except Exception:
                pass

            if not self.driver_manager.ensure_driver(
                proxy_settings, self.log,
                show_browser=True,
                user_data_dir=self._chrome_profile_dir,
            ):
                self.log("Не удалось открыть видимый браузер для капчи")
                return False

            try:
                self.driver_manager.driver.get("https://www.avito.ru/")
            except Exception as e:
                self.log(f"Не удалось открыть avito.ru в видимом браузере: {e}")

            event = threading.Event()
            result = {"ok": False}

            def _ask():
                try:
                    result["ok"] = messagebox.askokcancel(
                        "Капча",
                        "Решите капчу в открывшемся окне браузера и нажмите OK.\n\n"
                        "Cancel - пропустить этот цикл.",
                        parent=self.root,
                    )
                finally:
                    event.set()

            self.root.after(0, _ask)
            event.wait(timeout=600)
            solved = bool(result["ok"])

            try:
                self.driver_manager.cleanup()
            except Exception:
                pass

            if not self.driver_manager.ensure_driver(
                proxy_settings, self.log,
                show_browser=show_browser,
                user_data_dir=self._chrome_profile_dir,
            ):
                self.log("Не удалось перезапустить драйвер после капчи")
                return False

            if solved:
                self.log("✓ Капча решена, возвращаюсь в рабочий режим")
                self.set_status("✓ Капча решена")
            else:
                self.log("⏭ Капча не решена, пропускаю цикл")
            return solved
        finally:
            self._captcha_recovery_in_progress = False

    # ---------- Парсинг ----------
    def _build_parse_params(self, query, min_price, max_price, city) -> ParseParams:
        """Снимает все UI-зависимые значения в UI-потоке. Worker не трогает Tk."""
        return ParseParams(
            query=query,
            min_price=min_price,
            max_price=max_price,
            city=city,
            filter_services=bool(self.filter_services_var.get()),
            ignore_words=self._get_ignore_words(),
            delivery=bool(self.delivery_var.get()),
            show_browser=bool(self.show_browser_var.get()),
            proxy_settings=self._get_proxy_settings(),
            schedule_enabled=bool(self.schedule_enabled_var.get()),
            schedule_start=self.schedule_start_entry.get().strip() or "09:00",
            schedule_end=self.schedule_end_entry.get().strip() or "21:00",
            schedule_days=[bool(v.get()) for v in self.schedule_day_vars],
            notify_sound=bool(self.notify_var.get()),
            tg_notify_status=bool(self.tg_notify_status_var.get()),
        )

    def run_parser(self, params: ParseParams):
        query = params.query
        min_price = params.min_price
        max_price = params.max_price
        city = params.city
        proxy_settings = params.proxy_settings

        # Сбрасываем кэш отбракованных при смене любого фильтр-параметра
        filter_key = (
            query, min_price, max_price,
            int(params.filter_services),
            tuple(sorted(params.ignore_words)),
        )
        changed, prev_count = self.history.reset_filter_cache_if_changed(filter_key)
        if changed and prev_count:
            self.log(f"🔄 Фильтры изменились - сброс кэша отбракованных ({prev_count})")

        # Проверка расписания
        ok, reason = is_within_schedule(
            params.schedule_enabled,
            params.schedule_start,
            params.schedule_end,
            params.schedule_days,
        )
        if not ok:
            self.log(f"⏸ {reason} - парсинг пропущен")
            if params.tg_notify_status:
                self.notifier.send_status(f"⏸ {reason}", status_enabled=True)
            self.progress.stop()
            if not self.auto_update:
                self._set_idle_ui()
            if self.auto_update:
                self.root.after(100, self.schedule_next_auto)
            return

        if not self.driver_manager.ensure_driver(
            proxy_settings, self.log,
            show_browser=params.show_browser,
            user_data_dir=self._chrome_profile_dir,
        ):
            self.log("Не удалось создать драйвер. Парсинг невозможен.")
            self.progress.stop()
            self._set_idle_ui()
            return

        driver = self.driver_manager.driver

        try:
            encoded_query = urllib.parse.quote_plus(query)
            search_key = f"{query}|{city}|{int(params.delivery)}"
            cached_url = getattr(self, "cached_search_url", None)
            cached_key = getattr(self, "cached_search_key", None)
            use_cached = cached_url and cached_key == search_key

            if use_cached:
                self.log(f"Открываем сохранённый URL (быстрый путь)")
                driver.get(cached_url)
                random_sleep(2.0, 3.5)
                try:
                    WebDriverWait(driver, 15).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "[data-marker='item']"))
                    )
                    self.log("Карточки загружены")
                except TimeoutException:
                    if _parser_is_captcha_page(driver):
                        self._recover_from_captcha(proxy_settings, show_browser=params.show_browser)
                        return
                    self.log("Кеш URL не сработал, идём долгим путём")
                    self.cached_search_url = None
                    use_cached = False

            if not use_cached:
                if city and city != "Вся Россия":
                    city_slug = transliterate(city)
                    url = f"https://www.avito.ru/{city_slug}?q={encoded_query}&s=104"
                    self.log(f"Открываем URL для города {city}: {url}")
                    driver.get(url)
                    random_sleep(4.0, 7.0)
                else:
                    url = f"https://www.avito.ru/rossiya?q={encoded_query}&s=104"
                    self.log(f"Открываем URL для всей России: {url}")
                    driver.get(url)
                    random_sleep(4.0, 7.0)

            if self.stop_parsing:
                return

            if not use_cached:
                # Принимаем куки
                try:
                    cookie_btn = WebDriverWait(driver, 5).until(
                        EC.element_to_be_clickable((By.XPATH, '//button[contains(text(),"Принять")]'))
                    )
                    cookie_btn.click()
                    self.log("Куки приняты")
                    random_sleep(0.7, 1.8)
                    if self.stop_parsing:
                        return
                except TimeoutException:
                    self.log("Куки уже приняты")

                # Подтверждаем город
                try:
                    city_btn = WebDriverWait(driver, 5).until(
                        EC.element_to_be_clickable((By.XPATH, '//button[contains(text(),"Да")]'))
                    )
                    city_btn.click()
                    self.log("Город подтверждён")
                    random_sleep(1.5, 3.0)
                    if self.stop_parsing:
                        return
                except TimeoutException:
                    pass

                # Поиск
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
                    if self.stop_parsing:
                        return
                except (TimeoutException, NoSuchElementException):
                    self.log("URL уже содержит запрос")

                try:
                    WebDriverWait(driver, 15).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "[data-marker='item']"))
                    )
                except TimeoutException:
                    if _parser_is_captcha_page(driver):
                        self._recover_from_captcha(proxy_settings, show_browser=params.show_browser)
                        return
                    raise
                self.log("Карточки загружены")
                random_sleep(1.5, 3.0)
                if self.stop_parsing:
                    return

                # ===== Фильтр "Авито доставка" =====
                if params.delivery:
                    try:
                        self.log("Применяем фильтр 'Авито Доставка'...")
                        driver.execute_script("window.scrollBy(0, 300);")
                        random_sleep(0.7, 1.6)

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
                                random_sleep(0.4, 0.9)
                                elem.click()
                                delivery_element = elem
                                break
                            except (TimeoutException, NoSuchElementException) as e:
                                self.log(f"Не удалось по селектору {selector}: {e}")
                                continue

                        if delivery_element is None:
                            self.log("Не удалось найти элемент 'Авито Доставка'")
                        else:
                            random_sleep(1.5, 2.8)
                            try:
                                show_span = WebDriverWait(driver, 10).until(
                                    EC.presence_of_element_located(
                                        (By.XPATH, "//span[starts-with(text(),'Показать')]"))
                                )
                                parent_button = show_span.find_element(By.XPATH, "ancestor::button")
                                parent_button.click()
                            except (TimeoutException, NoSuchElementException) as e:
                                self.log(f"Кнопка применения не найдена - возможно, фильтр применился сразу: {e}")

                            random_sleep(2.5, 4.0)
                            try:
                                WebDriverWait(driver, 25).until(
                                    EC.presence_of_element_located((By.CSS_SELECTOR, "[data-marker='item']"))
                                )
                            except TimeoutException:
                                self.log("После фильтра доставки карточки не появились за 25с - продолжаем с текущей страницей")

                    except Exception as e:
                        self.log(f"Не удалось применить фильтр доставки (пропускаем): {e}")
                        logger.error(f"Ошибка при фильтре доставки: {traceback.format_exc()}")

                # Сохраняем финальный URL чтобы следующие циклы шли быстрым путём
                try:
                    self.cached_search_url = driver.current_url
                    self.cached_search_key = search_key
                    self.log("URL сохранён для быстрых перезапросов")
                except Exception:
                    pass

            # Прокрутка - до 50 карточек либо до конца страницы
            self.log("Прокручиваем страницу...")
            target_cards = 50
            last_height = driver.execute_script("return document.body.scrollHeight")
            current_position = 0
            max_scroll_attempts = 15
            attempts = 0

            while attempts < max_scroll_attempts:
                if self.stop_parsing:
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
                time.sleep(random.uniform(0.2, 0.6))
                if self.stop_parsing:
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

            # Постепенный скролл по всей странице, чтобы IntersectionObserver
            # успел сработать для каждой карточки (lazy-loader завязан на появление
            # в viewport). Один быстрый nudge низ->верх пропускает средние
            # карточки - браузер их "перелетает" без срабатывания observer.
            try:
                total_h = driver.execute_script("return document.body.scrollHeight") or 0
                step = 600
                y = 0
                while y < total_h:
                    if self.stop_parsing:
                        return
                    driver.execute_script(f"window.scrollTo(0, {y});")
                    time.sleep(0.35)
                    y += step
                driver.execute_script(f"window.scrollTo(0, {total_h});")
                time.sleep(0.3)
                driver.execute_script("window.scrollTo(0, 0);")
                time.sleep(0.3)
            except Exception:
                pass

            if self.stop_parsing:
                return

            items = driver.find_elements(By.CSS_SELECTOR, "[data-marker='item']")
            self.log(f"Найдено карточек: {len(items)}")
            self.set_status(f"📋 Обработка карточек: {len(items)}")
            new_results, page_summary = self.parse_items(items, params)
            self.log(f"Новых после фильтров: {len(new_results)}")

            # Retry фото у старых объявлений: если у них image_url=="Н/Д" и
            # они сейчас на странице с живым URL - обновляем. Работает даже если
            # на прошлом цикле фото не успело подгрузиться.
            ps_by_id = {p["id"]: p for p in page_summary}
            retry_updated_items = self.history.apply_retry_image_updates(ps_by_id)
            if retry_updated_items:
                self.log(f"🖼 Догружено фото у {len(retry_updated_items)} старых объявлений")

            disappeared = _parser_detect_disappeared(self.history.get_all(), page_summary, query)
            if disappeared:
                database.mark_inactive([it["id"] for it in disappeared])
                self.send_disappeared_notification(disappeared)

            added = self.history.update_with_new(new_results)
            if added > 0:
                self.log(f"Добавлено новых объявлений: {added}")
            else:
                self.log("Новых объявлений не найдено")

            if added > 0 and params.notify_sound:
                NotificationService.play_sound()

            if added > 0:
                self.send_telegram_notification(added)

            # Пишем в БД только изменившееся: новые + те, у кого догрузили фото.
            # Раньше сохраняли self.all_items целиком - это 500+ UPSERT каждый цикл
            # даже если нового 0 штук.
            dirty = list(new_results)
            if retry_updated_items:
                dirty_ids = {it["id"] for it in dirty}
                for it in retry_updated_items:
                    if it["id"] not in dirty_ids:
                        dirty.append(it)
            if dirty:
                save_data(dirty, self.log)

            # Инвалидируем кеш отрисованных карточек если список реально изменился -
            # иначе display_results через fast-path обновит только цвет is_new.
            if added > 0 or retry_updated_items:
                self._rendered_order = None

            self.root.after(0, self.display_results)
            self.set_status(
                f"✅ Готово. Новых: {added}",
                counter=f"Всего в БД: {self.history.count()}",
            )

        except Exception as e:
            if self.stop_parsing:
                # Жёсткий стоп: драйвер убит извне, Selenium кинул exception.
                # Не спамим логи и Telegram.
                logger.info(f"Парсер остановлен (жёсткий стоп): {type(e).__name__}")
                return
            error_trace = traceback.format_exc()
            user_msg = format_user_error(e, context="parser")
            self.log(user_msg)
            logger.error(f"Ошибка парсинга: {error_trace}")
            # Notifier уже сконфигурирован в start_parsing - шлём напрямую, Tk не трогаем.
            if params.tg_notify_status:
                self.notifier.send_status(user_msg, status_enabled=True)
            self.notifier.send_error(error_trace)
            self.set_status(user_msg[:80])

            # --- 1.4 Recovery: задержка при 429/403 от Авито + перезапуск Chrome если сессия мертва ---
            if should_retry(e):
                try:
                    from selenium.common.exceptions import WebDriverException
                    if isinstance(e, WebDriverException):
                        self.log("🔄 Перезапускаем браузер...")
                        self.driver_manager.cleanup()
                    msg_l = str(e).lower()
                    if any(s in msg_l for s in ("429", "403", "too many", "rate limit")):
                        wait = backoff_seconds(getattr(self, "_avito_block_attempts", 0))
                        self._avito_block_attempts = getattr(self, "_avito_block_attempts", 0) + 1
                        self.set_status(f"⏸ Авито блокирует. Жду {wait} сек перед повтором...")
                        self.log(f"⏸ Backoff {wait} сек (попытка {self._avito_block_attempts})")
                        time.sleep(wait)
                    else:
                        self._avito_block_attempts = 0
                except Exception:
                    pass
            else:
                self._avito_block_attempts = 0
        finally:
            self.progress.stop()
            if not self.auto_update:
                self._set_idle_ui()
            if self.auto_update:
                self.root.after(100, self.schedule_next_auto)

    # ---------- Парсинг элементов ----------
    def _get_ignore_words(self):
        raw = self.ignore_entry.get().strip()
        if not raw:
            return []
        return [w.strip().lower() for w in raw.split(",") if w.strip()]

    def parse_items(self, items, params: ParseParams):
        """Тонкий шим над AvitoParser - передаёт снимок UI + состояние БД."""
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
            stop_check=lambda: self.stop_parsing,
        )

    # ---------- Данные ----------
    def _load_data(self):
        # Старт всегда с пустой истории - предложим загрузить из файла после прорисовки UI.
        self.history.clear()
        self.root.after(300, self._startup_history_prompt)

    def _startup_history_prompt(self):
        choice = messagebox.askyesno(
            "История объявлений",
            "Загрузить историю из файла?\n\n"
            "«Да» - выбрать файл с сохранённой историей.\n"
            "«Нет» - начать с пустой истории.",
        )
        if not choice:
            self.log("Старт с пустой историей")
            return
        path = filedialog.askopenfilename(
            title="Загрузить историю",
            filetypes=[("История Avito Hunter", "*.json"), ("Все файлы", "*.*")],
        )
        if not path:
            self.log("Загрузка отменена, история пуста")
            return
        try:
            count = self.history.import_from_file(path)
            self.log(f"История загружена из {path} ({count} объявлений)")
            self.display_results()
        except Exception as e:
            messagebox.showerror("Ошибка загрузки", f"Не удалось загрузить файл:\n{e}")
            self.log(f"Ошибка загрузки истории: {e}")

    def _save_data(self):
        self.history.persist_all()

    def _export_history_to_file(self):
        """Диалог выбора файла + дамп истории в JSON. True если сохранили."""
        default_name = f"avito_history_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        path = filedialog.asksaveasfilename(
            title="Сохранить историю",
            defaultextension=".json",
            initialfile=default_name,
            filetypes=[("История Avito Hunter", "*.json"), ("Все файлы", "*.*")],
        )
        if not path:
            return False
        try:
            self.history.export_to_file(path)
            return True
        except Exception as e:
            messagebox.showerror("Ошибка сохранения", f"Не удалось сохранить файл:\n{e}")
            logger.error(f"Ошибка сохранения истории: {e}")
            return False

    def clear_history(self):
        if messagebox.askyesno("Очистка истории", "Вы уверены, что хотите удалить всю историю объявлений?"):
            self.history.clear()
            self.images = []
            self.display_results()
            self.log("История очищена")

    # ---------- Telegram уведомления ----------
    def send_telegram_notification(self, added):
        if not self.update_telegram_notifier():
            return
        if added <= 0:
            return

        new_items = self.history.iter_new()
        if not new_items:
            return

        # Сессия для скачивания картинок с Avito (не через TG-прокси, с нашими куками)
        img_session = requests.Session()
        if self.driver_manager.driver:
            try:
                for c in self.driver_manager.driver.get_cookies():
                    img_session.cookies.set(c['name'], c['value'])
            except Exception:
                pass
        img_session.headers.update({
            'User-Agent': random.choice(USER_AGENTS),
            'Referer': 'https://www.avito.ru/',
        })

        self.notifier.send_new_items(new_items, img_session)

    def send_disappeared_notification(self, disappeared):
        if not disappeared:
            return
        if not self.update_telegram_notifier():
            return
        self.notifier.send_disappeared(disappeared)

    # ---------- Загрузка изображений ----------
    def _load_image_async(self, session, image_url, img_label, card, gen):
        """Берёт байты картинки (из кэша или качает), превращает в PIL и ставит в карточку."""
        if gen != self._results_gen:
            return

        data = self.notifier.fetch_image_bytes(session, image_url)
        if gen != self._results_gen:
            return

        img = None
        if data:
            try:
                img = Image.open(BytesIO(data))
                img.load()
                img.thumbnail((150, 150))
            except Exception as e:
                logger.warning(f"PIL не открыл картинку {image_url[:80]}: {e}")

        if img is not None:
            self.root.after(0, lambda: self._set_image(img, img_label, gen))
        else:
            self.root.after(0, lambda: self._set_image_fallback(image_url, img_label, card, gen))

    def _set_image(self, pil_image, img_label, gen):
        if gen != self._results_gen:
            return
        try:
            if not img_label.winfo_exists():
                return
            size = pil_image.size
            photo = ctk.CTkImage(light_image=pil_image, dark_image=pil_image, size=size)
            self.images.append(photo)
            img_label.configure(image=photo, text="")
        except Exception as e:
            logger.warning(f"Не применилась картинка: {e}")

    def _set_image_fallback(self, url, img_label, card, gen):
        if gen != self._results_gen:
            return
        try:
            if not img_label.winfo_exists():
                return
            img_label.configure(text="📷 Открыть фото", text_color="#4a9eff", cursor="hand2")
            img_label.bind("<Button-1>", lambda e=None, u=url: webbrowser.open(u))
        except Exception:
            pass

    def _build_image_session(self):
        session = requests.Session()
        if self.driver_manager.driver:
            try:
                selenium_cookies = self.driver_manager.driver.get_cookies()
                for cookie in selenium_cookies:
                    session.cookies.set(cookie['name'], cookie['value'])
            except Exception:
                pass
        session.headers.update({
            'User-Agent': random.choice(USER_AGENTS),
            'Referer': 'https://www.avito.ru/'
        })
        return session

    def _create_card(self, item, gen, session):
        """Собирает одну карточку. Возвращает dict {card, state} для трекинга."""
        card = ctk.CTkFrame(self.results_frame, border_width=1)
        card.pack(fill="x", padx=5, pady=5)
        card.grid_columnconfigure(1, weight=1)

        state = {"hover_handled": False, "is_new": item.get("is_new", False)}

        def get_card_color(st=state, hover=False):
            if hover:
                return "#5a2a2a" if st["is_new"] else "#1e3a5a"
            return "#5a1e1e" if st["is_new"] else "transparent"

        card.configure(fg_color=get_card_color(state))

        def on_enter(event, _item=item, _card=card, st=state):
            if not st["hover_handled"] and st["is_new"]:
                st["hover_handled"] = True
                _item["is_new"] = False
                st["is_new"] = False
                self.root.after(100, lambda: database.upsert_ad(_item))
            _card.configure(fg_color=get_card_color(st, hover=True))

        def on_leave(event, _card=card, st=state):
            _card.configure(fg_color=get_card_color(st))

        card.bind("<Enter>", on_enter, add="+")
        card.bind("<Leave>", on_leave, add="+")

        header = ctk.CTkFrame(card, fg_color="transparent")
        header.grid(row=0, column=0, columnspan=2, sticky="ew", pady=5)

        ctk.CTkLabel(header, text=item['title'], font=ctk.CTkFont(size=14, weight='bold')).pack(side="left", padx=(5, 5))

        img_label = ctk.CTkLabel(card, text="")
        img_label.grid(row=1, column=0, rowspan=5, padx=5, pady=5, sticky="n")

        if item['image_url'] != "Н/Д":
            img_label.configure(text="⏳", text_color="gray")
            self.image_executor.submit(
                self._load_image_async, session, item['image_url'], img_label, card, gen
            )
        else:
            img_label.configure(text="[нет фото]")

        price_frame = ctk.CTkFrame(card, fg_color="transparent")
        price_frame.grid(row=1, column=1, sticky="ew", padx=5)
        ctk.CTkLabel(price_frame, text=f"Цена: {item['price']} руб.", font=ctk.CTkFont(size=13)).pack(side="left")

        desc = ctk.CTkTextbox(card, height=100, wrap="word", font=ctk.CTkFont(size=13))
        desc.insert("1.0", item['description'])
        desc.configure(state='disabled')
        desc.grid(row=2, column=1, sticky="ew", pady=5, padx=5)

        pub_ts = item.get("pub_date_timestamp", 0) or 0
        if pub_ts > 0:
            pub_str = datetime.fromtimestamp(pub_ts).strftime("%d.%m.%Y %H:%M")
        else:
            pub_str = item.get("date", "Н/Д")
        ctk.CTkLabel(card, text=f"🕐 На Авито: {pub_str}", font=ctk.CTkFont(size=13)).grid(
            row=3, column=1, sticky="w", padx=5
        )

        first_seen = item.get("first_seen", "Н/Д")
        ctk.CTkLabel(card, text=f"📥 В программе: {first_seen}", font=ctk.CTkFont(size=13)).grid(
            row=4, column=1, sticky="w", padx=5
        )

        link_label = ctk.CTkLabel(card, text="Открыть объявление", text_color="#4a9eff", cursor="hand2",
                                  font=ctk.CTkFont(size=13))
        link_label.grid(row=5, column=1, sticky="w", padx=5, pady=(5, 15))
        link_label.bind("<Button-1>", lambda e=None, url=item['link']: webbrowser.open(url))

        return {"card": card, "state": state, "get_color": get_card_color}

    def display_results(self):
        try:
            visible_items = self.history.get_all()
            visible_ids = [it["id"] for it in visible_items]

            # Fast path: тот же список в том же порядке - ничего не пересобираем,
            # только синхронизируем цвет is_new у существующих карточек.
            # Это главный выигрыш при авто-парсинге когда новых 0 штук.
            rendered_order = getattr(self, '_rendered_order', None)
            rendered = getattr(self, '_rendered_cards', None)
            if rendered_order is not None and rendered is not None and rendered_order == visible_ids:
                items_by_id = {it["id"]: it for it in visible_items}
                for id_, info in rendered.items():
                    item = items_by_id.get(id_)
                    if item is None:
                        continue
                    new_is_new = item.get("is_new", False)
                    if info["state"]["is_new"] != new_is_new:
                        info["state"]["is_new"] = new_is_new
                        try:
                            info["card"].configure(fg_color=info["get_color"]())
                        except Exception:
                            pass
                self._refresh_new_banner(visible_items)
                return

            # Slow path: полная перестройка.
            self._results_gen = getattr(self, '_results_gen', 0) + 1
            gen = self._results_gen
            for widget in self.results_frame.winfo_children():
                widget.destroy()
            self.images = []
            self._rendered_cards = {}

            session = self._build_image_session()

            for item in visible_items:
                info = self._create_card(item, gen, session)
                self._rendered_cards[item["id"]] = info

            self._rendered_order = list(visible_ids)

            self.results_frame.update_idletasks()
            self.canvas.configure(scrollregion=self.canvas.bbox("all"))
            self._refresh_new_banner(visible_items)
        except Exception as e:
            self.log(f"Ошибка отображения: {e}")
            logger.error(f"Ошибка отображения: {traceback.format_exc()}")

    def _refresh_new_banner(self, visible_items):
        count = sum(1 for it in visible_items if it.get("is_new"))
        if count <= 0:
            self._hide_new_banner()
            return
        self._new_banner_label.configure(text=f"🔔 Новых объявлений: {count}")
        self._new_jump_cursor = 0
        try:
            self._new_banner_frame.pack(fill="x", before=self.canvas, padx=5, pady=(0, 5))
        except Exception:
            self._new_banner_frame.pack(fill="x", padx=5, pady=(0, 5))

    def _hide_new_banner(self):
        try:
            self._new_banner_frame.pack_forget()
        except Exception:
            pass

    def _jump_to_next_new(self):
        order = getattr(self, "_rendered_order", None) or []
        cards = getattr(self, "_rendered_cards", None) or {}
        items_by_id = self.history.items_by_id()
        new_ids = [id_ for id_ in order if items_by_id.get(id_, {}).get("is_new")]
        if not new_ids:
            self._hide_new_banner()
            return
        # Всегда берём первый - после прыжка помечаем как не-новое, поэтому курсор всегда 0
        target_id = new_ids[0]
        target_item = items_by_id.get(target_id)
        info = cards.get(target_id)
        if not info:
            return
        card = info["card"]
        # Снимаем метку "новое": данные + визуал + БД
        if target_item:
            target_item["is_new"] = False
        st = info.get("state")
        if st is not None:
            st["is_new"] = False
            st["hover_handled"] = True
            try:
                card.configure(fg_color="transparent")
            except Exception:
                pass
        if target_item:
            self.root.after(100, lambda _i=target_item: database.upsert_ad(_i))
        # Скролл к карточке
        try:
            self.results_frame.update_idletasks()
            bbox = self.canvas.bbox("all")
            if bbox:
                total_h = bbox[3] - bbox[1]
                card_y = card.winfo_y()
                if total_h > 0:
                    frac = max(0.0, min(1.0, card_y / total_h))
                    self.canvas.yview_moveto(frac)
        except Exception as e:
            logger.warning(f"Не удалось проскроллить к новому объявлению: {e}")
        # Обновляем счётчик в баннере (визуально, без пересборки списка)
        remaining = len(new_ids) - 1
        if remaining <= 0:
            self._hide_new_banner()
        else:
            try:
                self._new_banner_label.configure(text=f"🔔 Новых объявлений: {remaining}")
            except Exception:
                pass

    # ---------- Управление парсингом ----------
    def _set_busy_ui(self):
        self.start_button.configure(state='disabled')
        self.clear_history_button.configure(state='disabled')
        self.save_as_profile_button.configure(state='disabled')
        self.kill_button.configure(state='normal')

    def _set_idle_ui(self):
        self.start_button.configure(state='normal')
        self.clear_history_button.configure(state='normal')
        self.save_as_profile_button.configure(state='normal')
        self.kill_button.configure(state='disabled')

    def start_parsing(self):
        query = self.query_entry.get().strip()
        min_price_str = self.min_price_entry.get().strip()
        max_price_str = self.max_price_entry.get().strip()
        city = self.city_var.get() if not self.all_russia_var.get() else None

        if not query:
            self.log("Введите запрос")
            return
        try:
            min_price = int(min_price_str) if min_price_str else 0
            max_price = int(max_price_str) if max_price_str else 999999999
            if min_price < 0 or max_price < 0 or min_price > max_price:
                self.log("Некорректный диапазон цен")
                return
        except ValueError:
            self.log("Цена должна быть числом")
            return

        # Режим определяется интервалом:
        # пусто или 0 - разовый парсинг, иначе цикл с этим интервалом.
        min_i_str = self.min_interval.get().strip()
        max_i_str = self.max_interval.get().strip()
        auto_mode = False
        if min_i_str or max_i_str:
            try:
                min_i = float(min_i_str) if min_i_str else 0
                max_i = float(max_i_str) if max_i_str else min_i
                if min_i > 0 and max_i >= min_i:
                    auto_mode = True
                elif min_i > 0 or max_i > 0:
                    self.log("Неверный интервал (макс должен быть ≥ мин)")
                    return
            except ValueError:
                self.log("Интервал должен быть числом")
                return

        params = self._build_parse_params(query, min_price, max_price, city)
        # TG-notifier конфигурируем сразу в UI-потоке - worker только шлёт.
        self.update_telegram_notifier()

        self.stop_parsing = False
        self.auto_update = auto_mode
        self._set_busy_ui()
        self.progress.start()
        self.log("🔄 Автопарсинг запущен" if auto_mode else "Разовый парсинг...")
        self.set_status(f"🔍 Ищем: {query}")
        self._parser_thread = threading.Thread(
            target=self.run_parser, args=(params,), daemon=True
        )
        self._parser_thread.start()

    def stop_auto_update(self):
        self.auto_update = False
        if not self.driver_manager.driver or not self.stop_parsing:
            self._set_idle_ui()

    def hard_stop_handler(self):
        """Жёсткая остановка: убиваем драйвер, поток падает с exception,
        который глушится в run_parser по флагу stop_parsing."""
        self.stop_parsing = True
        self.auto_update = False
        self.log("⏹⏹ Жёсткая остановка, убиваем браузер...")
        self.send_tg_status("⏹⏹ Парсер жёстко остановлен")
        self.set_status("⏹⏹ Убито")
        # cleanup может подвиснуть на секунду-другую - в отдельном потоке
        threading.Thread(target=self._hard_stop_cleanup, daemon=True).start()

    def _hard_stop_cleanup(self):
        try:
            # hard_kill не висит на HTTP, даже если worker параллельно держит сокет
            self.driver_manager.hard_kill()
        except Exception as e:
            logger.error(f"Ошибка при жёстком стопе: {e}")
        self.root.after(0, self._set_idle_ui)
        self.root.after(0, self.progress.stop)

    def run_auto_parsing(self):
        if not self.auto_update:
            return
        prev = getattr(self, "_parser_thread", None)
        if prev is not None and prev.is_alive():
            self.log("Предыдущий цикл ещё не завершился - пропускаем тик, перепланируем")
            self.schedule_next_auto()
            return
        query = self.query_entry.get().strip()
        min_price_str = self.min_price_entry.get().strip()
        max_price_str = self.max_price_entry.get().strip()
        city = self.city_var.get() if not self.all_russia_var.get() else None
        if query:
            try:
                min_price = int(min_price_str) if min_price_str else 0
                max_price = int(max_price_str) if max_price_str else 999999999
                if min_price < 0 or max_price < 0 or min_price > max_price:
                    self.log("Некорректный диапазон цен, автообновление остановлено")
                    self.stop_auto_update()
                    return
                self.log("Автообновление...")
                params = self._build_parse_params(query, min_price, max_price, city)
                self.update_telegram_notifier()
                self._parser_thread = threading.Thread(
                    target=self.run_parser, args=(params,), daemon=True
                )
                self._parser_thread.start()
            except ValueError:
                self.log("Ошибка параметров, автообновление остановлено")
                self.stop_auto_update()
        else:
            self.stop_auto_update()

    def schedule_next_auto(self):
        if not self.auto_update:
            return
        try:
            min_i = float(self.min_interval.get())
            max_i = float(self.max_interval.get())
        except ValueError:
            self.stop_auto_update()
            return
        interval = random.uniform(min_i * 60, max_i * 60) * 1000
        self.root.after(int(interval), self.run_auto_parsing)

    def on_closing(self):
        choice = messagebox.askyesnocancel(
            "Закрытие программы",
            "Сохранить историю объявлений перед закрытием?\n\n"
            "«Да» - выбрать файл для сохранения.\n"
            "«Нет» - закрыть без сохранения.\n"
            "«Отмена» - вернуться в программу.",
        )
        if choice is None:
            return
        if choice:
            self._export_history_to_file()
        self.image_executor.shutdown(wait=False)
        self.driver_manager.cleanup()
        try:
            clear_history_files()
        except Exception as e:
            logger.error(f"Не удалось очистить БД при выходе: {e}")
        self.root.destroy()
