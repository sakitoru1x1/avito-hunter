import tkinter as tk
from tkinter import messagebox, simpledialog
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
from storage import save_data, load_data, clear_history_files, update_all_items
import database

ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("blue")

class ParserApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Парсер Avito v.1.1 (fixed)")

        screen_w = root.winfo_screenwidth()
        screen_h = root.winfo_screenheight()
        win_w = min(1600, int(screen_w * 0.8))
        win_h = min(1000, int(screen_h * 0.8))
        x = (screen_w - win_w) // 2
        y = (screen_h - win_h) // 2
        self.root.geometry(f"{win_w}x{win_h}+{x}+{y}")
        self.root.minsize(800, 600)

        self.all_items = []
        self.images = []
        self.auto_update = False
        self.driver_manager = DriverManager()
        self.previous_ids = set()
        self.stop_parsing = False
        self.max_items = DEFAULT_MAX_ITEMS
        self.image_executor = ThreadPoolExecutor(max_workers=4)

        self.notify_var = tk.BooleanVar(value=True)
        self.filter_services_var = tk.BooleanVar(value=False)
        self.delivery_var = tk.BooleanVar(value=False)
        self.telegram_notifier = TelegramNotifier()

        self.create_widgets()
        self.load_settings()
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

        top_half = ctk.CTkFrame(tab_results)
        top_half.pack(fill="x", pady=(0, 5))

        left_frame = ctk.CTkFrame(top_half, border_width=1)
        left_frame.pack(side="left", fill="both", expand=False, padx=(0, 5))
        ctk.CTkLabel(left_frame, text="Параметры поиска", font=ctk.CTkFont(weight="bold")).pack(pady=(5,0))

        row1 = ctk.CTkFrame(left_frame)
        row1.pack(fill="x", pady=2)
        ctk.CTkLabel(row1, text="Город:").pack(side="left", padx=2)
        self.city_var = tk.StringVar(value="Москва")
        self.city_combo = ctk.CTkComboBox(row1, variable=self.city_var, values=CITIES, state="readonly")
        self.city_combo.pack(side="left", padx=2)
        # ctk.CTkComboBox bind is different, it has 'command' parameter, but for backward compatibility we might keep bind if it works or use command.
        # Actually CTkComboBox uses 'command' but it doesn't provide the event.
        # Let's see if bind works or if we should use command.
        self.city_combo.configure(command=lambda _: self.on_city_change(None))

        self.all_russia_var = tk.BooleanVar()
        self.all_russia_cb = ctk.CTkCheckBox(row1, text="Вся Россия", variable=self.all_russia_var,
                                              command=self.on_all_russia)
        self.all_russia_cb.pack(side="left", padx=10)

        row2 = ctk.CTkFrame(left_frame)
        row2.pack(fill="x", pady=2)
        ctk.CTkLabel(row2, text="Запрос:").pack(side="left", padx=2)
        self.query_entry = ctk.CTkEntry(row2, width=30*8)
        self.query_entry.pack(side="left", padx=2, fill="x", expand=True)
        self.query_entry.insert(0, "")

        row2b = ctk.CTkFrame(left_frame)
        row2b.pack(fill="x", pady=2)
        ctk.CTkLabel(row2b, text="Игнор:").pack(side="left", padx=2)
        self.ignore_entry = ctk.CTkEntry(row2b, width=30*8,
                                          placeholder_text="через запятую: 3s, б/у, сломан")
        self.ignore_entry.pack(side="left", padx=2, fill="x", expand=True)

        row3 = ctk.CTkFrame(left_frame)
        row3.pack(fill="x", pady=2)
        ctk.CTkLabel(row3, text="Цена от:").pack(side="left", padx=2)
        self.min_price_entry = ctk.CTkEntry(row3, width=8*8)
        self.min_price_entry.pack(side="left", padx=2)
        self.min_price_entry.insert(0, "")
        ctk.CTkLabel(row3, text="до:").pack(side="left", padx=(5, 2))
        self.max_price_entry = ctk.CTkEntry(row3, width=8*8)
        self.max_price_entry.pack(side="left", padx=2)
        self.max_price_entry.insert(0, "")
        ctk.CTkLabel(row3, text="Рейтинг ≥:").pack(side="left", padx=(15, 2))
        self.min_rating_entry = ctk.CTkEntry(row3, width=4*8)
        self.min_rating_entry.pack(side="left", padx=2)
        self.min_rating_entry.insert(0, "")

        row4 = ctk.CTkFrame(left_frame)
        row4.pack(fill="x", pady=2)
        self.notify_cb = ctk.CTkCheckBox(row4, text="Звук", variable=self.notify_var)
        self.notify_cb.pack(side="left", padx=2)
        self.filter_cb = ctk.CTkCheckBox(row4, text="Убрать услуги", variable=self.filter_services_var)
        self.filter_cb.pack(side="left", padx=2)
        self.delivery_cb = ctk.CTkCheckBox(row4, text="Авито доставка", variable=self.delivery_var)
        self.delivery_cb.pack(side="left", padx=2)

        row5 = ctk.CTkFrame(left_frame)
        row5.pack(fill="x", pady=5)
        self.start_button = ctk.CTkButton(row5, text="▶ Начать", command=self.start_parsing)
        self.start_button.pack(side="left", padx=2)
        self.auto_button = ctk.CTkButton(row5, text="🔄 Авто", command=self.toggle_auto_update)
        self.auto_button.pack(side="left", padx=2)
        self.stop_button = ctk.CTkButton(row5, text="⏹ Стоп", command=self.stop_parsing_handler, state='disabled')
        self.stop_button.pack(side="left", padx=2)

        row5b = ctk.CTkFrame(left_frame)
        row5b.pack(fill="x", pady=2)
        self.clear_history_button = ctk.CTkButton(row5b, text="🗑 Очистить историю", command=self.clear_history)
        self.clear_history_button.pack(side="left", padx=2)
        self.save_as_profile_button = ctk.CTkButton(row5b, text="💾 Сохранить как профиль",
                                                  command=self.save_current_search_as_profile)
        self.save_as_profile_button.pack(side="left", padx=2)

        row7 = ctk.CTkFrame(left_frame)
        row7.pack(fill="x", pady=2)
        ctk.CTkLabel(row7, text="Интервал (мин): от").pack(side="left", padx=2)
        self.min_interval = ctk.CTkEntry(row7, width=4*8)
        self.min_interval.pack(side="left", padx=2)
        self.min_interval.insert(0, "1")
        ctk.CTkLabel(row7, text="до").pack(side="left", padx=(2, 0))
        self.max_interval = ctk.CTkEntry(row7, width=4*8)
        self.max_interval.pack(side="left", padx=2)
        self.max_interval.insert(0, "3")

        row8 = ctk.CTkFrame(left_frame)
        row8.pack(fill="x", pady=2)
        ctk.CTkLabel(row8, text="Макс. объявлений:").pack(side="left", padx=2)
        self.max_items_entry = ctk.CTkEntry(row8, width=5*8)
        self.max_items_entry.pack(side="left", padx=2)
        self.max_items_entry.insert(0, str(DEFAULT_MAX_ITEMS))

        right_frame = ctk.CTkFrame(top_half, border_width=1)
        right_frame.pack(side="right", fill="both", expand=True)
        ctk.CTkLabel(right_frame, text="Лог выполнения", font=ctk.CTkFont(weight="bold")).pack(pady=(5,0))

        self.log_text = ctk.CTkTextbox(right_frame, wrap="word", height=200)
        self.log_text.pack(fill="both", expand=True, padx=5, pady=5)

        bottom_frame = ctk.CTkFrame(tab_results, border_width=1)
        bottom_frame.pack(fill="both", expand=True, pady=(5, 0))
        ctk.CTkLabel(bottom_frame, text="Результаты поиска", font=ctk.CTkFont(weight="bold")).pack(pady=(5,0))

        results_toolbar = ctk.CTkFrame(bottom_frame)
        results_toolbar.pack(fill="x", pady=(0, 5))
        self.favorites_only_var = tk.BooleanVar(value=False)
        ctk.CTkCheckBox(
            results_toolbar, text="⭐ Только избранное",
            variable=self.favorites_only_var,
            command=self.display_results,
        ).pack(side="left", padx=5)

        self.canvas = tk.Canvas(bottom_frame, borderwidth=0, highlightthickness=0, bg=ctk.ThemeManager.theme["CTkFrame"]["fg_color"][1])
        self.scrollbar = tk.Scrollbar(bottom_frame, orient="vertical", command=self.canvas.yview)
        self.canvas.configure(yscrollcommand=self.scrollbar.set)

        self.scrollbar.pack(side="right", fill="y")
        self.canvas.pack(side="left", fill="both", expand=True)

        self.results_frame = ctk.CTkFrame(self.canvas, fg_color="transparent")
        self.canvas.create_window((0, 0), window=self.results_frame, anchor="nw", tags=("window",))
        self.results_frame.bind("<Configure>", self.on_frame_configure)
        self.canvas.bind("<Configure>", self.on_canvas_configure)

        self.canvas.bind_all("<MouseWheel>", self._on_mousewheel)
        self.canvas.bind_all("<Button-4>", self._on_mousewheel_linux)
        self.canvas.bind_all("<Button-5>", self._on_mousewheel_linux)

        # ========== Вкладка "Настройки" ==========
        tab_settings = self.notebook.add("Настройки")

        proxy_frame = ctk.CTkFrame(tab_settings, border_width=1)
        proxy_frame.pack(fill="x", padx=10, pady=5)
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

        telegram_frame = ctk.CTkFrame(tab_settings, border_width=1)
        telegram_frame.pack(fill="x", padx=10, pady=5)
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

        schedule_frame = ctk.CTkFrame(tab_settings, border_width=1)
        schedule_frame.pack(fill="x", padx=10, pady=5)
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

        save_frame = ctk.CTkFrame(tab_settings)
        save_frame.pack(fill="x", padx=10, pady=10)
        self.save_button = ctk.CTkButton(save_frame, text="💾 Запомнить настройки", command=self.save_settings)
        self.save_button.pack(side="left", padx=5)

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

        ctk.CTkLabel(profiles_right, text="Игнор:").grid(row=2, column=2, sticky="w", pady=2, padx=(15, 5))
        self.profile_ignore_entry = ctk.CTkEntry(profiles_right, width=30*8,
                                                  placeholder_text="через запятую: 3s, б/у")
        self.profile_ignore_entry.grid(row=2, column=3, padx=5, pady=2, sticky="w")

        ctk.CTkLabel(profiles_right, text="Город:").grid(row=3, column=0, sticky="w", pady=2, padx=5)
        self.profile_city_var = tk.StringVar(value="Москва")
        self.profile_city_combo = ctk.CTkComboBox(
            profiles_right, variable=self.profile_city_var, values=CITIES, width=27*8, state="readonly"
        )
        self.profile_city_combo.grid(row=3, column=1, padx=5, pady=2, sticky="w")

        ctk.CTkLabel(profiles_right, text="Цена от:").grid(row=4, column=0, sticky="w", pady=2, padx=5)
        self.profile_min_price_entry = ctk.CTkEntry(profiles_right, width=12*8)
        self.profile_min_price_entry.grid(row=4, column=1, padx=5, pady=2, sticky="w")

        ctk.CTkLabel(profiles_right, text="Цена до:").grid(row=5, column=0, sticky="w", pady=2, padx=5)
        self.profile_max_price_entry = ctk.CTkEntry(profiles_right, width=12*8)
        self.profile_max_price_entry.grid(row=5, column=1, padx=5, pady=2, sticky="w")

        ctk.CTkLabel(profiles_right, text="Интервал от (мин):").grid(row=6, column=0, sticky="w", pady=2, padx=5)
        self.profile_min_interval_entry = ctk.CTkEntry(profiles_right, width=12*8)
        self.profile_min_interval_entry.grid(row=6, column=1, padx=5, pady=2, sticky="w")
        self.profile_min_interval_entry.insert(0, "1")

        ctk.CTkLabel(profiles_right, text="Интервал до (мин):").grid(row=7, column=0, sticky="w", pady=2, padx=5)
        self.profile_max_interval_entry = ctk.CTkEntry(profiles_right, width=12*8)
        self.profile_max_interval_entry.grid(row=7, column=1, padx=5, pady=2, sticky="w")
        self.profile_max_interval_entry.insert(0, "3")

        self.profile_delivery_var = tk.BooleanVar(value=False)
        ctk.CTkCheckBox(
            profiles_right, text="Авито доставка", variable=self.profile_delivery_var,
        ).grid(row=8, column=1, padx=5, pady=2, sticky="w")

        self.profile_filter_services_var = tk.BooleanVar(value=False)
        ctk.CTkCheckBox(
            profiles_right, text="Убрать услуги", variable=self.profile_filter_services_var,
        ).grid(row=9, column=1, padx=5, pady=2, sticky="w")

        profiles_buttons = ctk.CTkFrame(profiles_right)
        profiles_buttons.grid(row=10, column=0, columnspan=2, pady=10, sticky="w")

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
        if os.path.exists(SETTINGS_FILE):
            try:
                with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
                    settings = json.load(f)
                self.telegram_token_entry.delete(0, tk.END)
                self.telegram_token_entry.insert(0, settings.get("telegram_token", ""))
                self.telegram_chat_id_entry.delete(0, tk.END)
                self.telegram_chat_id_entry.insert(0, settings.get("telegram_chat_id", ""))
                self.proxy_scheme_var.set(settings.get("proxy_scheme", "http"))
                self.proxy_host_entry.delete(0, tk.END)
                self.proxy_host_entry.insert(0, settings.get("proxy_host", ""))
                self.proxy_port_entry.delete(0, tk.END)
                self.proxy_port_entry.insert(0, settings.get("proxy_port", ""))
                self.proxy_user_entry.delete(0, tk.END)
                self.proxy_user_entry.insert(0, settings.get("proxy_user", ""))
                self.proxy_pass_entry.delete(0, tk.END)
                self.proxy_pass_entry.insert(0, settings.get("proxy_pass", ""))

                # Отдельный прокси для Telegram
                self.tg_proxy_scheme_var.set(settings.get("tg_proxy_scheme", "http"))
                self.tg_proxy_host_entry.delete(0, tk.END)
                self.tg_proxy_host_entry.insert(0, settings.get("tg_proxy_host", ""))
                self.tg_proxy_port_entry.delete(0, tk.END)
                self.tg_proxy_port_entry.insert(0, settings.get("tg_proxy_port", ""))
                self.tg_proxy_user_entry.delete(0, tk.END)
                self.tg_proxy_user_entry.insert(0, settings.get("tg_proxy_user", ""))
                self.tg_proxy_pass_entry.delete(0, tk.END)
                self.tg_proxy_pass_entry.insert(0, settings.get("tg_proxy_pass", ""))

                self.tg_notify_status_var.set(settings.get("tg_notify_status", True))

                # Расписание
                self.schedule_enabled_var.set(settings.get("schedule_enabled", False))
                sched_start = settings.get("schedule_start", "09:00")
                sched_end = settings.get("schedule_end", "21:00")
                self.schedule_start_entry.delete(0, tk.END)
                self.schedule_start_entry.insert(0, sched_start)
                self.schedule_end_entry.delete(0, tk.END)
                self.schedule_end_entry.insert(0, sched_end)
                sched_days = settings.get("schedule_days", [True] * 7)
                if isinstance(sched_days, list) and len(sched_days) == 7:
                    for i, v in enumerate(sched_days):
                        self.schedule_day_vars[i].set(bool(v))

                saved_max = settings.get("max_items", DEFAULT_MAX_ITEMS)
                self.max_items_entry.delete(0, tk.END)
                self.max_items_entry.insert(0, str(saved_max))
                self.max_items = int(saved_max)

                self.log("✅ Настройки загружены")
            except Exception as e:
                self.log(f"⚠️ Ошибка загрузки настроек: {e}")
        else:
            self.log("ℹ️ Файл настроек не найден, используйте поля ввода")

    def save_settings(self):
        try:
            max_items_val = int(self.max_items_entry.get().strip())
            if max_items_val < 10:
                max_items_val = 10
            if max_items_val > 500:
                max_items_val = 500
            self.max_items = max_items_val
        except ValueError:
            self.max_items = DEFAULT_MAX_ITEMS

        settings = {
            "telegram_token": self.telegram_token_entry.get().strip(),
            "telegram_chat_id": self.telegram_chat_id_entry.get().strip(),
            "proxy_scheme": self.proxy_scheme_var.get(),
            "proxy_host": self.proxy_host_entry.get().strip(),
            "proxy_port": self.proxy_port_entry.get().strip(),
            "proxy_user": self.proxy_user_entry.get().strip(),
            "proxy_pass": self.proxy_pass_entry.get().strip(),
            "tg_proxy_scheme": self.tg_proxy_scheme_var.get(),
            "tg_proxy_host": self.tg_proxy_host_entry.get().strip(),
            "tg_proxy_port": self.tg_proxy_port_entry.get().strip(),
            "tg_proxy_user": self.tg_proxy_user_entry.get().strip(),
            "tg_proxy_pass": self.tg_proxy_pass_entry.get().strip(),
            "tg_notify_status": bool(self.tg_notify_status_var.get()),
            "schedule_enabled": bool(self.schedule_enabled_var.get()),
            "schedule_start": self.schedule_start_entry.get().strip() or "09:00",
            "schedule_end": self.schedule_end_entry.get().strip() or "21:00",
            "schedule_days": [bool(v.get()) for v in self.schedule_day_vars],
            "max_items": self.max_items,
        }
        try:
            with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
                json.dump(settings, f, ensure_ascii=False, indent=2)
            self.log("✅ Настройки сохранены")
        except Exception as e:
            self.log(f"❌ Ошибка сохранения настроек: {e}")

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
        self.log_text.insert(tk.END, message + "\n")
        try:
            self.log_text._textbox.see(tk.END)
        except Exception:
            pass
        logger.info(message)
        self.root.update()

    def _extract_seller_rating(self, item):
        """Пытается вытащить рейтинг продавца с карточки объявления. Возвращает float или None."""
        selectors = [
            (By.CSS_SELECTOR, "[data-marker='seller-info/rating-score']"),
            (By.CSS_SELECTOR, "[data-marker='seller-rating/score']"),
            (By.XPATH, ".//span[contains(@class,'rating')]"),
            (By.XPATH, ".//*[starts-with(@aria-label,'Рейтинг')]"),
        ]
        for by, sel in selectors:
            try:
                elem = item.find_element(by, sel)
                text = (elem.text or elem.get_attribute("aria-label") or "").strip()
                if not text:
                    continue
                text = text.replace(",", ".")
                for token in text.split():
                    try:
                        val = float(token)
                        if 0 <= val <= 5:
                            return val
                    except ValueError:
                        continue
            except (NoSuchElementException, Exception):
                continue
        return None

    def _get_min_rating_filter(self):
        """Читает минимальный рейтинг из поля фильтра. None = фильтр не активен."""
        try:
            txt = self.min_rating_entry.get().strip().replace(",", ".")
            if not txt:
                return None
            val = float(txt)
            if 0 <= val <= 5:
                return val
        except (ValueError, AttributeError):
            pass
        return None

    def toggle_favorite(self, item):
        """Переключает отметку 'избранное' у объявления."""
        new_val = not bool(item.get("is_favorite"))
        item["is_favorite"] = new_val
        try:
            database.set_favorite(item["id"], new_val)
        except Exception as e:
            self.log(f"⚠️ Не удалось обновить избранное: {e}")
        self.display_results()

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
        if not self.tg_notify_status_var.get():
            return False
        if not self.update_telegram_notifier():
            return False
        return self.telegram_notifier.send_message(text)

    def send_error_telegram(self, error_text):
        if not self.update_telegram_notifier():
            return False
        error_text = sanitize_error_for_telegram(error_text)
        if len(error_text) > 3500:
            error_text = error_text[:3500] + "..."
        message = f"<b>❌ Ошибка в программе</b>\n<pre>{error_text}</pre>"
        return self.telegram_notifier.send_message(message)

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
        self.telegram_notifier = TelegramNotifier(token, chat_id, proxies=proxies)
        return self.telegram_notifier.enabled

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

    # ---------- Парсинг ----------
    def run_parser(self, query, min_price, max_price, city):
        self.stop_parsing = False

        # Проверка расписания
        days_mask = [v.get() for v in self.schedule_day_vars]
        ok, reason = is_within_schedule(
            self.schedule_enabled_var.get(),
            self.schedule_start_entry.get().strip() or "09:00",
            self.schedule_end_entry.get().strip() or "21:00",
            days_mask,
        )
        if not ok:
            self.log(f"⏸ {reason} - парсинг пропущен")
            self.send_tg_status(f"⏸ {reason}")
            self.progress.stop()
            self.start_button.configure(state='normal')
            if not self.auto_update:
                self.stop_button.configure(state='disabled')
            if self.auto_update:
                self.root.after(100, self.schedule_next_auto)
            return

        proxy_settings = self._get_proxy_settings()

        if not self.driver_manager.ensure_driver(proxy_settings, self.log):
            self.log("Не удалось создать драйвер. Парсинг невозможен.")
            self.progress.stop()
            self.start_button.configure(state='normal')
            self.stop_button.configure(state='disabled')
            return

        driver = self.driver_manager.driver

        try:
            encoded_query = urllib.parse.quote_plus(query)

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

            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "[data-marker='item']"))
            )
            self.log("Карточки загружены")
            random_sleep(1.5, 3.0)
            if self.stop_parsing:
                return

            # ===== Фильтр "Авито доставка" =====
            if self.delivery_var.get():
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
                        WebDriverWait(driver, 15).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "[data-marker='item']"))
                        )

                except Exception as e:
                    self.log(f"Не удалось применить фильтр доставки (пропускаем): {e}")
                    logger.error(f"Ошибка при фильтре доставки: {traceback.format_exc()}")

            # Плавная прокрутка
            self.log("Плавно прокручиваем страницу...")
            last_height = driver.execute_script("return document.body.scrollHeight")
            current_position = 0
            max_scroll_attempts = 30
            attempts = 0

            while attempts < max_scroll_attempts:
                if self.stop_parsing:
                    self.log("Прокрутка прервана")
                    return
                scroll_step = random.randint(250, 800)
                current_position += scroll_step
                if current_position > last_height:
                    current_position = last_height
                driver.execute_script(f"window.scrollTo({{top: {current_position}, behavior: 'smooth'}});")
                time.sleep(random.uniform(0.3, 2.5))
                if self.stop_parsing:
                    return

                new_height = driver.execute_script("return document.body.scrollHeight")
                if new_height > last_height:
                    last_height = new_height
                    self.log(f"Новый контент загружен, высота: {last_height}")
                    attempts = 0
                else:
                    attempts += 1

                if current_position >= last_height - 100:
                    self.log("Достигнут конец страницы.")
                    break

            self.log("Прокрутка завершена")
            random_sleep(1.5, 3.0)
            if self.stop_parsing:
                return

            items = driver.find_elements(By.CSS_SELECTOR, "[data-marker='item']")
            self.log(f"Найдено карточек: {len(items)}")
            self.set_status(f"📋 Обработка карточек: {len(items)}")
            new_results = self.parse_items(items, min_price, max_price)
            self.log(f"Отобрано по цене: {len(new_results)}")

            current_query = self.query_entry.get().strip()
            disappeared = self._detect_disappeared(self.all_items, new_results, current_query)
            if disappeared:
                database.mark_inactive([it["id"] for it in disappeared])
                self.send_disappeared_notification(disappeared)

            self.all_items, added = update_all_items(self.all_items, new_results, self.max_items, self.log)
            if added > 0:
                self.log(f"Добавлено новых объявлений: {added}")
            else:
                self.log("Новых объявлений не найдено")

            if added > 0 and self.notify_var.get():
                self._play_notification_sound()

            if added > 0:
                self.send_telegram_notification(added)

            save_data(self.all_items, self.log)
            self.root.after(0, self.display_results)
            self.set_status(
                f"✅ Готово. Новых: {added}",
                counter=f"Всего в БД: {len(self.all_items)}",
            )

        except Exception as e:
            error_trace = traceback.format_exc()
            self.log(f"Ошибка парсинга: {str(e)}")
            logger.error(f"Ошибка парсинга: {error_trace}")
            self.send_tg_status(f"❌ Ошибка: {str(e)}")
            self.send_error_telegram(error_trace)
            self.set_status(f"❌ Ошибка: {str(e)[:60]}")
        finally:
            self.progress.stop()
            self.start_button.configure(state='normal')
            if not self.auto_update:
                self.stop_button.configure(state='disabled')
            if self.auto_update:
                self.root.after(100, self.schedule_next_auto)

    def _play_notification_sound(self):
        try:
            if sys.platform == 'win32':
                import winsound
                winsound.Beep(440, 200)
            elif sys.platform == 'darwin':
                os.system('afplay /System/Library/Sounds/Glass.aiff &')
            else:
                os.system('paplay /usr/share/sounds/freedesktop/stereo/message.oga 2>/dev/null &')
        except Exception:
            print('\a')

    # ---------- Парсинг элементов ----------
    def extract_date(self, item):
        date_selectors = [
            (By.CSS_SELECTOR, "[data-marker='item-date']"),
            (By.XPATH, ".//span[contains(@class, 'date')]"),
            (By.XPATH, ".//time"),
            (By.XPATH, ".//*[contains(text(), 'сегодня') or contains(text(), 'вчера')]")
        ]
        for by, selector in date_selectors:
            try:
                elem = item.find_element(by, selector)
                return elem.text.strip()
            except NoSuchElementException:
                continue
        return "Н/Д"

    def get_item_id(self, item):
        try:
            item_id = item.get_attribute("data-item-id")
            if item_id:
                return item_id
        except Exception:
            pass
        try:
            link = item.find_element(By.CSS_SELECTOR, "a[itemprop='url']").get_attribute("href")
            title = item.find_element(By.CSS_SELECTOR, "[itemprop='name']").text
            return f"{link}_{title}"
        except Exception:
            return None

    def _get_ignore_words(self):
        raw = self.ignore_entry.get().strip()
        if not raw:
            return []
        return [w.strip().lower() for w in raw.split(",") if w.strip()]

    def parse_items(self, items, min_price, max_price):
        result = []
        total = len(items)
        ignore_words = self._get_ignore_words()
        for idx, item in enumerate(items):
            if self.stop_parsing:
                self.log("⏹️ Парсинг прерван пользователем")
                return result

            self.log(f"🔄 Обработка карточки {idx + 1}/{total}...")

            try:
                price_elem = item.find_element(By.CSS_SELECTOR, "[itemprop='price']")
                price = price_elem.get_attribute("content")
                if not price:
                    self.log("⛔ Цена не найдена - пропущено")
                    continue
                price_int = int(price)
                self.log(f"📄 Цена: {price_int} руб.")

                if price_int < min_price or price_int > max_price:
                    self.log(f"⛔ Цена {price_int} вне диапазона ({min_price}-{max_price}) - пропущено")
                    continue

                try:
                    title = item.find_element(By.CSS_SELECTOR, "[itemprop='name']").text
                except NoSuchElementException:
                    title = "Н/Д"

                try:
                    link = item.find_element(By.CSS_SELECTOR, "a[itemprop='url']").get_attribute("href")
                except NoSuchElementException:
                    link = "Н/Д"

                if self.filter_services_var.get():
                    if link and link != "Н/Д":
                        if "predlozheniya_uslug" in link or "vakansii" in link:
                            self.log(f"🔍 ОТФИЛЬТРОВАНО (услуги): {title[:30]}...")
                            continue

                img_url = "Н/Д"
                try:
                    img = item.find_element(By.CSS_SELECTOR, "img[data-marker='image']")
                    src = img.get_attribute("src")
                    if src and not src.startswith("data:"):
                        img_url = src
                except NoSuchElementException:
                    try:
                        imgs = item.find_elements(By.TAG_NAME, "img")
                        for img in imgs:
                            src = img.get_attribute("src")
                            if src and not src.startswith("data:") and ("avatars" in src or "img" in src):
                                img_url = src
                                break
                    except Exception:
                        pass

                description = "Н/Д"
                for desc_selector in [
                    (By.CSS_SELECTOR, "[itemprop='description']"),
                    (By.CSS_SELECTOR, "[data-marker*='description']"),
                    (By.XPATH, ".//div[contains(@class, 'description')]"),
                ]:
                    try:
                        desc = item.find_element(*desc_selector)
                        text = desc.text.strip()
                        if text and len(text) > 5:
                            description = text
                            break
                    except NoSuchElementException:
                        continue

                if description == "Н/Д" or len(description) < 20:
                    try:
                        paragraphs = item.find_elements(By.TAG_NAME, "p")
                        full_text = []
                        for p in paragraphs:
                            text = p.text.strip()
                            if len(text) > 20 and "₽" not in text and "район" not in text.lower() and "метро" not in text.lower():
                                full_text.append(text)
                        if full_text:
                            description = "\n".join(full_text)
                    except Exception:
                        pass

                if ignore_words:
                    haystack = f"{title} {description}".lower()
                    hit = next((w for w in ignore_words if w in haystack), None)
                    if hit:
                        self.log(f"🚫 Игнор-слово «{hit}»: {title[:40]}...")
                        continue

                date_str = self.extract_date(item)
                timestamp = parse_date_to_timestamp(date_str)
                item_id = self.get_item_id(item)
                if not item_id:
                    self.log("⛔ Не удалось получить ID - пропущено")
                    continue

                seller_rating = self._extract_seller_rating(item)

                min_rating = self._get_min_rating_filter()
                if min_rating is not None and seller_rating is not None and seller_rating < min_rating:
                    self.log(f"⛔ Рейтинг {seller_rating} < {min_rating} - пропущено")
                    continue

                result.append({
                    "id": item_id,
                    "title": title,
                    "price": price_int,
                    "link": link,
                    "image_url": img_url,
                    "description": description,
                    "date": date_str,
                    "pub_date_timestamp": timestamp,
                    "search_query": self.query_entry.get().strip(),
                    "seller_rating": seller_rating,
                    "is_new": False,
                    "first_seen": None
                })
                self.log(f"✅ Добавлено: {title[:30]}...")

            except Exception as e:
                self.log(f"❌ Исключение при обработке карточки: {e}")
                logger.error(f"Ошибка при парсинге элемента: {e}")
                continue
        return result

    # ---------- Данные ----------
    def _load_data(self):
        self.all_items = load_data(self.max_items, self.log)
        if self.all_items:
            self.display_results()

    def _save_data(self):
        save_data(self.all_items, self.log)

    def clear_history(self):
        if messagebox.askyesno("Очистка истории", "Вы уверены, что хотите удалить всю историю объявлений?"):
            self.all_items = []
            self.images = []
            clear_history_files()
            self.display_results()
            self.log("История очищена")

    # ---------- Telegram уведомления ----------
    def _normalize_title(self, title):
        import re
        t = (title or "").lower()
        t = re.sub(r"[^\w\s]", " ", t, flags=re.UNICODE)
        words = [w for w in t.split() if len(w) >= 3]
        return set(words)

    def _is_duplicate(self, new_item, existing_items):
        new_price = new_item.get("price") or 0
        new_title_words = self._normalize_title(new_item.get("title", ""))
        if not new_title_words or new_price <= 0:
            return False
        for old in existing_items:
            if old.get("id") == new_item.get("id"):
                continue
            old_price = old.get("price") or 0
            if old_price <= 0:
                continue
            price_delta = abs(new_price - old_price) / max(old_price, 1)
            if price_delta > 0.1:
                continue
            old_words = self._normalize_title(old.get("title", ""))
            if not old_words:
                continue
            overlap = len(new_title_words & old_words)
            union = len(new_title_words | old_words)
            if union > 0 and overlap / union >= 0.7:
                return True
        return False

    def send_telegram_notification(self, added):
        if not self.update_telegram_notifier():
            return
        if added <= 0:
            return

        new_items = [item for item in self.all_items if item.get("is_new", False)]
        if not new_items:
            return

        existing = [item for item in self.all_items if not item.get("is_new", False)]
        filtered = []
        dupes_skipped = 0
        for item in new_items:
            if self._is_duplicate(item, existing):
                dupes_skipped += 1
                self.log(f"🔁 Дубликат пропущен в ТГ: {item.get('title', '')[:40]}...")
                continue
            filtered.append(item)
        if dupes_skipped:
            self.log(f"🔁 Всего дубликатов пропущено: {dupes_skipped}")
        new_items = filtered
        if not new_items:
            return

        self.telegram_notifier.send_message(
            f"<b>🔔 Найдено новых объявлений: {len(new_items)}</b>"
        )

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

        for item in new_items:
            caption = f"<a href='{item['link']}'>{item['title']}</a>\n"
            caption += f"💰 {item['price']} руб.\n"
            caption += f"🕒 {item.get('first_seen', 'Н/Д')}\n"
            desc = item.get('description', '')
            if desc and desc != "Н/Д":
                if len(desc) > 400:
                    desc = desc[:400] + "..."
                caption += f"📝 {desc}"

            img = item.get('image_url')
            photo_bytes = None
            if img and img != "Н/Д" and img.startswith("http"):
                for attempt in range(3):
                    try:
                        r = img_session.get(img, timeout=25)
                        if r.status_code == 200 and r.content:
                            photo_bytes = r.content
                            break
                    except Exception as e:
                        logger.warning(f"Попытка {attempt+1}/3: не скачалась картинка {img[:60]}: {e}")
                        time.sleep(1)

            if photo_bytes:
                self.telegram_notifier.send_photo(caption=caption, photo_bytes=photo_bytes)
            else:
                # Avito блокирует и нас и TG - шлём просто текст со ссылкой
                self.telegram_notifier.send_message(caption)

    def _detect_disappeared(self, all_items, new_results, current_query):
        """Находит объявления, которые были активны в выдаче и пропали в текущем парсе."""
        if not new_results or not all_items:
            return []
        new_ids = {item["id"] for item in new_results}
        min_ts = min((r.get("pub_date_timestamp", 0) or 0) for r in new_results)
        if min_ts <= 0:
            return []
        disappeared = []
        for old in all_items:
            if old.get("id") in new_ids:
                continue
            if not old.get("is_active", True):
                continue
            if current_query and old.get("search_query") and old["search_query"] != current_query:
                continue
            old_ts = old.get("pub_date_timestamp", 0) or 0
            if old_ts < min_ts:
                continue
            disappeared.append(old)
        return disappeared

    def send_disappeared_notification(self, disappeared):
        if not disappeared:
            return
        if not self.update_telegram_notifier():
            return
        self.log(f"🗑️ Пропало объявлений: {len(disappeared)}")
        MAX_LEN = 4000
        header = f"<b>🗑️ Объявления сняты: {len(disappeared)}</b>\n\n"
        current_msg = header
        messages = []
        for item in disappeared:
            price = item.get("price")
            price_str = f"{price} руб." if price else "цена не указана"
            block = f"• <s>{item.get('title', 'Н/Д')}</s> - было {price_str}\n\n"
            if len(current_msg) + len(block) > MAX_LEN:
                messages.append(current_msg)
                current_msg = "🔹 Продолжение:\n\n" + block
            else:
                current_msg += block
        if current_msg:
            messages.append(current_msg)
        for msg in messages:
            self.telegram_notifier.send_message(msg)

    # ---------- Загрузка изображений ----------
    def _load_image_async(self, session, image_url, img_label, card, gen):
        """Скачивает картинку с 2 попытками. gen - generation counter, чтоб не писать
        результат в уничтоженные виджеты после перерисовки."""
        img = None
        last_err = None
        for attempt in range(2):
            if gen != self._results_gen:
                return  # карточки уже перерисованы - выкидываем результат
            try:
                resp = session.get(image_url, timeout=20)
                if resp.status_code == 200 and resp.content:
                    img = Image.open(BytesIO(resp.content))
                    img.load()
                    img.thumbnail((150, 150))
                    break
                last_err = f"HTTP {resp.status_code}"
            except Exception as e:
                last_err = str(e)

        if gen != self._results_gen:
            return

        if img is not None:
            self.root.after(0, lambda: self._set_image(img, img_label, gen))
        else:
            logger.warning(f"Не скачалась картинка {image_url[:80]}: {last_err}")
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
            img_label.bind("<Button-1>", lambda e, u=url: webbrowser.open(u))
        except Exception:
            pass

    def display_results(self):
        try:
            self._results_gen = getattr(self, '_results_gen', 0) + 1
            gen = self._results_gen
            for widget in self.results_frame.winfo_children():
                widget.destroy()
            self.images = []

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

            fav_only = self.favorites_only_var.get() if hasattr(self, 'favorites_only_var') else False
            visible_items = [it for it in self.all_items if (not fav_only) or it.get("is_favorite")]

            for item in visible_items:
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
                        self.root.after(100, self._save_data)
                    _card.configure(fg_color=get_card_color(st, hover=True))

                def on_leave(event, _card=card, st=state):
                    _card.configure(fg_color=get_card_color(st))

                card.bind("<Enter>", on_enter, add="+")
                card.bind("<Leave>", on_leave, add="+")

                header = ctk.CTkFrame(card, fg_color="transparent")
                header.grid(row=0, column=0, columnspan=2, sticky="ew", pady=5)

                fav_btn = ctk.CTkButton(
                    header,
                    text=("⭐" if item.get("is_favorite") else "☆"),
                    width=30,
                    command=lambda _it=item: self.toggle_favorite(_it),
                )
                fav_btn.pack(side="left", padx=(5, 5))
                ctk.CTkLabel(header, text=item['title'], font=ctk.CTkFont(size=14, weight='bold')).pack(side="left")

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
                if item.get("seller_rating") is not None:
                    ctk.CTkLabel(price_frame, text=f"  ★ {item['seller_rating']:.1f}",
                              font=ctk.CTkFont(size=13), text_color="#c68a00").pack(side="left", padx=(10, 0))

                desc = ctk.CTkTextbox(card, height=100, wrap="word", font=ctk.CTkFont(size=13))
                desc.insert("1.0", item['description'])
                desc.configure(state='disabled')
                desc.grid(row=2, column=1, sticky="ew", pady=5, padx=5)

                first_seen = item.get("first_seen", "Н/Д")
                ctk.CTkLabel(card, text=f"Время добавления в программу: {first_seen}", font=ctk.CTkFont(size=13)).grid(row=3,
                                                                                                              column=1,
                                                                                                              sticky="w", padx=5)

                link_label = ctk.CTkLabel(card, text="Открыть объявление", text_color="#4a9eff", cursor="hand2",
                                          font=ctk.CTkFont(size=13))
                link_label.grid(row=4, column=1, sticky="w", padx=5, pady=(5, 15))
                link_label.bind("<Button-1>", lambda e, url=item['link']: webbrowser.open(url))

            self.results_frame.update_idletasks()
            self.canvas.configure(scrollregion=self.canvas.bbox("all"))
        except Exception as e:
            self.log(f"Ошибка отображения: {e}")
            logger.error(f"Ошибка отображения: {traceback.format_exc()}")

    # ---------- Управление парсингом ----------
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

        self.stop_parsing = False
        self.start_button.configure(state='disabled')
        self.stop_button.configure(state='normal')
        self.progress.start()
        self.log("Ручной парсинг...")
        self.set_status(f"🔍 Ищем: {query}")
        threading.Thread(target=self.run_parser, args=(query, min_price, max_price, city), daemon=True).start()

    def toggle_auto_update(self):
        if not self.auto_update:
            try:
                min_i = float(self.min_interval.get())
                max_i = float(self.max_interval.get())
                if min_i <= 0 or max_i < min_i:
                    self.log("Неверный интервал")
                    return
            except ValueError:
                self.log("Интервал должен быть числом")
                return
            self.auto_update = True
            self.auto_button.configure(text="Автообновление вкл", state='disabled')
            self.stop_button.configure(state='normal')
            self.log("Автообновление запущено")
            self.run_auto_parsing()
        else:
            self.stop_auto_update()

    def stop_auto_update(self):
        self.auto_update = False
        self.auto_button.configure(text="Автообновление", state='normal')
        if not self.driver_manager.driver or not self.stop_parsing:
            self.stop_button.configure(state='disabled')
        self.log("Автообновление остановлено")

    def stop_parsing_handler(self):
        self.stop_parsing = True
        self.stop_auto_update()
        self.log("⏹️ Запрос на остановку парсинга отправлен")
        self.send_tg_status("⏹️ Парсер остановлен")
        self.set_status("⏹ Остановлено")

    def run_auto_parsing(self):
        if not self.auto_update:
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
                threading.Thread(target=self.run_parser, args=(query, min_price, max_price, city),
                                 daemon=True).start()
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
        self.image_executor.shutdown(wait=False)
        self.driver_manager.cleanup()
        save_data(self.all_items)
        self.root.destroy()
