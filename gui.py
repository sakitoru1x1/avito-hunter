import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox, simpledialog
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
from PIL import Image, ImageTk

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
        main_container = ttk.Frame(self.root)
        main_container.pack(fill="both", expand=True, padx=10, pady=5)

        self.notebook = ttk.Notebook(main_container)
        self.notebook.pack(fill="both", expand=True)

        # ========== Вкладка "Результаты поиска" ==========
        tab_results = ttk.Frame(self.notebook)
        self.notebook.add(tab_results, text="Результаты поиска")

        top_half = ttk.Frame(tab_results)
        top_half.pack(fill="x", pady=(0, 5))

        left_frame = ttk.LabelFrame(top_half, text="Параметры поиска", padding=5)
        left_frame.pack(side="left", fill="both", expand=False, padx=(0, 5))

        row1 = ttk.Frame(left_frame)
        row1.pack(fill="x", pady=2)
        ttk.Label(row1, text="Город:").pack(side="left", padx=2)
        self.city_var = tk.StringVar(value="Москва")
        self.city_combo = ttk.Combobox(row1, textvariable=self.city_var, values=CITIES, width=20)
        self.city_combo.pack(side="left", padx=2)
        self.city_combo.bind("<<ComboboxSelected>>", self.on_city_change)

        self.all_russia_var = tk.BooleanVar()
        self.all_russia_cb = ttk.Checkbutton(row1, text="Вся Россия", variable=self.all_russia_var,
                                              command=self.on_all_russia)
        self.all_russia_cb.pack(side="left", padx=10)

        row2 = ttk.Frame(left_frame)
        row2.pack(fill="x", pady=2)
        ttk.Label(row2, text="Запрос:").pack(side="left", padx=2)
        self.query_entry = ttk.Entry(row2, width=30)
        self.query_entry.pack(side="left", padx=2, fill="x", expand=True)
        self.query_entry.insert(0, "")

        row3 = ttk.Frame(left_frame)
        row3.pack(fill="x", pady=2)
        ttk.Label(row3, text="Цена от:").pack(side="left", padx=2)
        self.min_price_entry = ttk.Entry(row3, width=8)
        self.min_price_entry.pack(side="left", padx=2)
        self.min_price_entry.insert(0, "")
        ttk.Label(row3, text="до:").pack(side="left", padx=(5, 2))
        self.max_price_entry = ttk.Entry(row3, width=8)
        self.max_price_entry.pack(side="left", padx=2)
        self.max_price_entry.insert(0, "")

        row4 = ttk.Frame(left_frame)
        row4.pack(fill="x", pady=2)
        self.notify_cb = ttk.Checkbutton(row4, text="Звук", variable=self.notify_var)
        self.notify_cb.pack(side="left", padx=2)
        self.filter_cb = ttk.Checkbutton(row4, text="Убрать услуги", variable=self.filter_services_var)
        self.filter_cb.pack(side="left", padx=2)
        self.delivery_cb = ttk.Checkbutton(row4, text="Авито доставка", variable=self.delivery_var)
        self.delivery_cb.pack(side="left", padx=2)

        row5 = ttk.Frame(left_frame)
        row5.pack(fill="x", pady=5)
        self.start_button = ttk.Button(row5, text="▶ Начать", command=self.start_parsing)
        self.start_button.pack(side="left", padx=2)
        self.auto_button = ttk.Button(row5, text="🔄 Авто", command=self.toggle_auto_update)
        self.auto_button.pack(side="left", padx=2)
        self.stop_button = ttk.Button(row5, text="⏹ Стоп", command=self.stop_parsing_handler, state='disabled')
        self.stop_button.pack(side="left", padx=2)
        self.clear_history_button = ttk.Button(row5, text="🗑 Очистить историю", command=self.clear_history)
        self.clear_history_button.pack(side="left", padx=2)
        self.save_as_profile_button = ttk.Button(row5, text="💾 Сохранить как профиль",
                                                  command=self.save_current_search_as_profile)
        self.save_as_profile_button.pack(side="left", padx=2)

        row6 = ttk.Frame(left_frame)
        row6.pack(fill="x", pady=2)
        self.progress = ttk.Progressbar(row6, mode='indeterminate', length=200)
        self.progress.pack(side="left", padx=2)

        row7 = ttk.Frame(left_frame)
        row7.pack(fill="x", pady=2)
        ttk.Label(row7, text="Интервал (мин): от").pack(side="left", padx=2)
        self.min_interval = ttk.Entry(row7, width=4)
        self.min_interval.pack(side="left", padx=2)
        self.min_interval.insert(0, "1")
        ttk.Label(row7, text="до").pack(side="left", padx=(2, 0))
        self.max_interval = ttk.Entry(row7, width=4)
        self.max_interval.pack(side="left", padx=2)
        self.max_interval.insert(0, "3")

        row8 = ttk.Frame(left_frame)
        row8.pack(fill="x", pady=2)
        ttk.Label(row8, text="Макс. объявлений:").pack(side="left", padx=2)
        self.max_items_entry = ttk.Entry(row8, width=5)
        self.max_items_entry.pack(side="left", padx=2)
        self.max_items_entry.insert(0, str(DEFAULT_MAX_ITEMS))

        right_frame = ttk.LabelFrame(top_half, text="Лог выполнения", padding=5)
        right_frame.pack(side="right", fill="both", expand=True)

        self.log_text = scrolledtext.ScrolledText(right_frame, wrap=tk.WORD, height=10)
        self.log_text.pack(fill="both", expand=True)

        bottom_frame = ttk.LabelFrame(tab_results, text="Результаты поиска", padding=5)
        bottom_frame.pack(fill="both", expand=True, pady=(5, 0))

        self.canvas = tk.Canvas(bottom_frame, borderwidth=0, highlightthickness=0)
        self.scrollbar = ttk.Scrollbar(bottom_frame, orient="vertical", command=self.canvas.yview)
        self.canvas.configure(yscrollcommand=self.scrollbar.set)

        self.scrollbar.pack(side="right", fill="y")
        self.canvas.pack(side="left", fill="both", expand=True)

        self.results_frame = ttk.Frame(self.canvas)
        self.canvas.create_window((0, 0), window=self.results_frame, anchor="nw", tags=("window",))
        self.results_frame.bind("<Configure>", self.on_frame_configure)
        self.canvas.bind("<Configure>", self.on_canvas_configure)

        self.canvas.bind_all("<MouseWheel>", self._on_mousewheel)
        self.canvas.bind_all("<Button-4>", self._on_mousewheel_linux)
        self.canvas.bind_all("<Button-5>", self._on_mousewheel_linux)

        # ========== Вкладка "Настройки" ==========
        tab_settings = ttk.Frame(self.notebook)
        self.notebook.add(tab_settings, text="Настройки")

        proxy_frame = ttk.LabelFrame(tab_settings, text="Прокси", padding=10)
        proxy_frame.pack(fill="x", padx=10, pady=5)

        ttk.Label(proxy_frame, text="Тип:").grid(row=0, column=0, sticky="w", pady=2)
        self.proxy_scheme_var = tk.StringVar(value="http")
        self.proxy_scheme_combo = ttk.Combobox(proxy_frame, textvariable=self.proxy_scheme_var,
                                                values=["http", "socks5"], width=8)
        self.proxy_scheme_combo.grid(row=0, column=1, padx=5, sticky="w")

        ttk.Label(proxy_frame, text="Хост:").grid(row=0, column=2, sticky="w", padx=(20, 0))
        self.proxy_host_entry = ttk.Entry(proxy_frame, width=20)
        self.proxy_host_entry.grid(row=0, column=3, padx=5)

        ttk.Label(proxy_frame, text="Порт:").grid(row=0, column=4, sticky="w")
        self.proxy_port_entry = ttk.Entry(proxy_frame, width=8)
        self.proxy_port_entry.grid(row=0, column=5, padx=5)

        ttk.Label(proxy_frame, text="Логин:").grid(row=1, column=0, sticky="w")
        self.proxy_user_entry = ttk.Entry(proxy_frame, width=20)
        self.proxy_user_entry.grid(row=1, column=1, padx=5, columnspan=2, sticky="w")

        ttk.Label(proxy_frame, text="Пароль:").grid(row=1, column=3, sticky="w", padx=(20, 0))
        self.proxy_pass_entry = ttk.Entry(proxy_frame, width=20, show="*")
        self.proxy_pass_entry.grid(row=1, column=4, padx=5, columnspan=2, sticky="w")

        self.test_proxy_button = ttk.Button(proxy_frame, text="Тест прокси", command=self.test_proxy)
        self.test_proxy_button.grid(row=0, column=6, padx=20, rowspan=2)

        self.proxy_status_label = ttk.Label(proxy_frame, text="", foreground="gray")
        self.proxy_status_label.grid(row=2, column=0, columnspan=7, sticky="w", padx=5)

        telegram_frame = ttk.LabelFrame(tab_settings, text="Telegram уведомления", padding=10)
        telegram_frame.pack(fill="x", padx=10, pady=5)

        ttk.Label(telegram_frame, text="Токен бота:").grid(row=0, column=0, sticky="w", pady=2)
        self.telegram_token_entry = ttk.Entry(telegram_frame, width=50)
        self.telegram_token_entry.grid(row=0, column=1, padx=5, pady=2)

        ttk.Label(telegram_frame, text="Chat ID:").grid(row=1, column=0, sticky="w", pady=2)
        self.telegram_chat_id_entry = ttk.Entry(telegram_frame, width=50)
        self.telegram_chat_id_entry.grid(row=1, column=1, padx=5, pady=2)

        # Отдельный прокси для Telegram
        tg_proxy_sub = ttk.LabelFrame(telegram_frame, text="Прокси для Telegram (необязательно)", padding=5)
        tg_proxy_sub.grid(row=3, column=0, columnspan=2, sticky="ew", padx=5, pady=(10, 5))

        ttk.Label(tg_proxy_sub, text="Тип:").grid(row=0, column=0, sticky="w", pady=2)
        self.tg_proxy_scheme_var = tk.StringVar(value="http")
        self.tg_proxy_scheme_combo = ttk.Combobox(
            tg_proxy_sub, textvariable=self.tg_proxy_scheme_var,
            values=["http", "socks5"], width=8,
        )
        self.tg_proxy_scheme_combo.grid(row=0, column=1, padx=5, sticky="w")

        ttk.Label(tg_proxy_sub, text="Хост:").grid(row=0, column=2, sticky="w", padx=(20, 0))
        self.tg_proxy_host_entry = ttk.Entry(tg_proxy_sub, width=20)
        self.tg_proxy_host_entry.grid(row=0, column=3, padx=5)

        ttk.Label(tg_proxy_sub, text="Порт:").grid(row=0, column=4, sticky="w")
        self.tg_proxy_port_entry = ttk.Entry(tg_proxy_sub, width=8)
        self.tg_proxy_port_entry.grid(row=0, column=5, padx=5)

        ttk.Label(tg_proxy_sub, text="Логин:").grid(row=1, column=0, sticky="w")
        self.tg_proxy_user_entry = ttk.Entry(tg_proxy_sub, width=20)
        self.tg_proxy_user_entry.grid(row=1, column=1, padx=5, columnspan=2, sticky="w")

        ttk.Label(tg_proxy_sub, text="Пароль:").grid(row=1, column=3, sticky="w", padx=(20, 0))
        self.tg_proxy_pass_entry = ttk.Entry(tg_proxy_sub, width=20, show="*")
        self.tg_proxy_pass_entry.grid(row=1, column=4, padx=5, columnspan=2, sticky="w")

        self.test_telegram_button = ttk.Button(telegram_frame, text="Тест", command=self.test_telegram)
        self.test_telegram_button.grid(row=2, column=1, padx=5, pady=5, sticky="w")

        self.telegram_status_label = ttk.Label(telegram_frame, text="", foreground="gray")
        self.telegram_status_label.grid(row=2, column=0, columnspan=2, sticky="w", padx=5)

        # Уведомления о статусе парсера
        ttk.Label(telegram_frame, text="Уведомления о статусе:").grid(row=4, column=0, sticky="w", pady=(10, 2))
        self.tg_notify_status_var = tk.BooleanVar(value=True)
        self.tg_notify_status_cb = ttk.Checkbutton(
            telegram_frame, text="Слать старт/стоп/ошибки в Telegram",
            variable=self.tg_notify_status_var,
        )
        self.tg_notify_status_cb.grid(row=4, column=1, padx=5, sticky="w")

        schedule_frame = ttk.LabelFrame(tab_settings, text="Расписание работы", padding=10)
        schedule_frame.pack(fill="x", padx=10, pady=5)

        self.schedule_enabled_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            schedule_frame, text="Работать только по расписанию",
            variable=self.schedule_enabled_var,
        ).grid(row=0, column=0, columnspan=6, sticky="w", pady=2)

        ttk.Label(schedule_frame, text="Начало (ЧЧ:ММ):").grid(row=1, column=0, sticky="w", pady=2)
        self.schedule_start_entry = ttk.Entry(schedule_frame, width=8)
        self.schedule_start_entry.grid(row=1, column=1, padx=5, sticky="w")
        self.schedule_start_entry.insert(0, "09:00")

        ttk.Label(schedule_frame, text="Окончание (ЧЧ:ММ):").grid(row=1, column=2, sticky="w", padx=(20, 0))
        self.schedule_end_entry = ttk.Entry(schedule_frame, width=8)
        self.schedule_end_entry.grid(row=1, column=3, padx=5, sticky="w")
        self.schedule_end_entry.insert(0, "21:00")

        ttk.Label(schedule_frame, text="Дни недели:").grid(row=2, column=0, sticky="w", pady=(8, 2))
        days_row = ttk.Frame(schedule_frame)
        days_row.grid(row=2, column=1, columnspan=6, sticky="w", pady=(8, 2))

        self.schedule_day_vars = []
        day_names = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
        for i, dname in enumerate(day_names):
            var = tk.BooleanVar(value=True)
            self.schedule_day_vars.append(var)
            ttk.Checkbutton(days_row, text=dname, variable=var).pack(side="left", padx=2)

        save_frame = ttk.Frame(tab_settings)
        save_frame.pack(fill="x", padx=10, pady=10)
        self.save_button = ttk.Button(save_frame, text="💾 Запомнить настройки", command=self.save_settings)
        self.save_button.pack(side="left", padx=5)

        # ========== Вкладка "Профили" ==========
        tab_profiles = ttk.Frame(self.notebook)
        self.notebook.add(tab_profiles, text="Профили")

        profiles_left = ttk.LabelFrame(tab_profiles, text="Список профилей", padding=5)
        profiles_left.pack(side="left", fill="y", padx=(10, 5), pady=10)

        self.profiles_listbox = tk.Listbox(profiles_left, width=30, height=20)
        self.profiles_listbox.pack(side="left", fill="y")
        self.profiles_listbox.bind("<<ListboxSelect>>", self.on_profile_select)

        profiles_scroll = ttk.Scrollbar(profiles_left, orient="vertical", command=self.profiles_listbox.yview)
        profiles_scroll.pack(side="right", fill="y")
        self.profiles_listbox.configure(yscrollcommand=profiles_scroll.set)

        profiles_right = ttk.LabelFrame(tab_profiles, text="Параметры профиля", padding=10)
        profiles_right.pack(side="left", fill="both", expand=True, padx=5, pady=10)

        ttk.Label(profiles_right, text="Название:").grid(row=0, column=0, sticky="w", pady=2)
        self.profile_name_entry = ttk.Entry(profiles_right, width=30)
        self.profile_name_entry.grid(row=0, column=1, padx=5, pady=2, sticky="w")

        ttk.Label(profiles_right, text="Запрос:").grid(row=1, column=0, sticky="w", pady=2)
        self.profile_query_entry = ttk.Entry(profiles_right, width=30)
        self.profile_query_entry.grid(row=1, column=1, padx=5, pady=2, sticky="w")

        ttk.Label(profiles_right, text="Город:").grid(row=2, column=0, sticky="w", pady=2)
        self.profile_city_var = tk.StringVar(value="Москва")
        self.profile_city_combo = ttk.Combobox(
            profiles_right, textvariable=self.profile_city_var, values=CITIES, width=27,
        )
        self.profile_city_combo.grid(row=2, column=1, padx=5, pady=2, sticky="w")

        ttk.Label(profiles_right, text="Цена от:").grid(row=3, column=0, sticky="w", pady=2)
        self.profile_min_price_entry = ttk.Entry(profiles_right, width=12)
        self.profile_min_price_entry.grid(row=3, column=1, padx=5, pady=2, sticky="w")

        ttk.Label(profiles_right, text="Цена до:").grid(row=4, column=0, sticky="w", pady=2)
        self.profile_max_price_entry = ttk.Entry(profiles_right, width=12)
        self.profile_max_price_entry.grid(row=4, column=1, padx=5, pady=2, sticky="w")

        ttk.Label(profiles_right, text="Интервал от (мин):").grid(row=5, column=0, sticky="w", pady=2)
        self.profile_min_interval_entry = ttk.Entry(profiles_right, width=12)
        self.profile_min_interval_entry.grid(row=5, column=1, padx=5, pady=2, sticky="w")
        self.profile_min_interval_entry.insert(0, "1")

        ttk.Label(profiles_right, text="Интервал до (мин):").grid(row=6, column=0, sticky="w", pady=2)
        self.profile_max_interval_entry = ttk.Entry(profiles_right, width=12)
        self.profile_max_interval_entry.grid(row=6, column=1, padx=5, pady=2, sticky="w")
        self.profile_max_interval_entry.insert(0, "3")

        self.profile_delivery_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            profiles_right, text="Авито доставка", variable=self.profile_delivery_var,
        ).grid(row=7, column=1, padx=5, pady=2, sticky="w")

        self.profile_filter_services_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            profiles_right, text="Убрать услуги", variable=self.profile_filter_services_var,
        ).grid(row=8, column=1, padx=5, pady=2, sticky="w")

        profiles_buttons = ttk.Frame(profiles_right)
        profiles_buttons.grid(row=9, column=0, columnspan=2, pady=10, sticky="w")

        ttk.Button(profiles_buttons, text="➕ Новый", command=self.profile_new).pack(side="left", padx=2)
        ttk.Button(profiles_buttons, text="💾 Сохранить", command=self.profile_save).pack(side="left", padx=2)
        ttk.Button(profiles_buttons, text="🗑 Удалить", command=self.profile_delete).pack(side="left", padx=2)
        ttk.Button(profiles_buttons, text="✔ Сделать активным", command=self.profile_set_active).pack(side="left", padx=2)
        ttk.Button(profiles_buttons, text="📥 Загрузить в поиск", command=self.profile_load_to_search).pack(side="left", padx=2)

        self.profile_status_label = ttk.Label(profiles_right, text="", foreground="gray")
        self.profile_status_label.grid(row=10, column=0, columnspan=2, sticky="w", padx=5, pady=(10, 0))

        self._current_profile_id = None

        # ========== Вкладка "Инструкция" ==========
        tab_instructions = ttk.Frame(self.notebook)
        self.notebook.add(tab_instructions, text="Инструкция")

        self.instructions_text = scrolledtext.ScrolledText(tab_instructions, wrap=tk.WORD, font=('Arial', 10))
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

        self.instructions_text.tag_configure("heading1", font=('Arial', 14, 'bold'), foreground='#2E86C1')
        self.instructions_text.tag_configure("heading2", font=('Arial', 12, 'bold'), foreground='#2874A6')
        self.instructions_text.tag_configure("heading3", font=('Arial', 11, 'bold'), foreground='#1F618D')
        self.instructions_text.tag_configure("list", lmargin1=20, lmargin2=40)

        content_lines = self.instructions_text.get('1.0', tk.END).splitlines()
        line_num = 1
        for line in content_lines:
            if line.startswith('# '):
                self.instructions_text.tag_add('heading1', f"{line_num}.0", f"{line_num}.end")
            elif line.startswith('## '):
                self.instructions_text.tag_add('heading2', f"{line_num}.0", f"{line_num}.end")
            elif line.startswith('### '):
                self.instructions_text.tag_add('heading3', f"{line_num}.0", f"{line_num}.end")
            elif line.startswith('- ') or (len(line) > 2 and line[0].isdigit() and line[1] == '.'):
                self.instructions_text.tag_add('list', f"{line_num}.0", f"{line_num}.end")
            line_num += 1

        self.instructions_text.config(state='disabled')

        self.style = ttk.Style()
        self.style.configure('Accent.TButton', foreground='blue', font=('Arial', 10, 'bold'))

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
        self.log_text.see(tk.END)
        logger.info(message)
        self.root.update()

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
            self.telegram_status_label.config(text="❌ Токен не указан", foreground="red")
            return
        proxies = self._get_tg_proxies_dict()
        notifier = TelegramNotifier(token, chat_id, proxies=proxies)
        ok, msg = notifier.test_connection()
        if ok:
            self.telegram_status_label.config(text="✅ Бот доступен", foreground="green")
            if chat_id:
                test_text = "🔔 Тестовое сообщение от парсера Avito"
                if notifier.send_message(test_text):
                    self.telegram_status_label.config(text="✅ Тест отправлен", foreground="green")
                else:
                    self.telegram_status_label.config(text="❌ Ошибка отправки", foreground="red")
            else:
                self.telegram_status_label.config(text="✅ Бот доступен, укажите Chat ID", foreground="orange")
        else:
            self.telegram_status_label.config(text=f"❌ {msg}", foreground="red")

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
            self.proxy_status_label.config(text="❌ Укажите хост и порт", foreground="red")
            return
        proxy_url = f"{scheme}://{user}:{pwd}@{host}:{port}" if user and pwd else f"{scheme}://{host}:{port}"
        proxies = {"http": proxy_url, "https": proxy_url}
        try:
            r = requests.get("https://httpbin.org/ip", proxies=proxies, timeout=10)
            if r.status_code == 200:
                ip = r.json()["origin"]
                self.proxy_status_label.config(text=f"✅ Прокси работает, ваш IP: {ip}", foreground="green")
            else:
                self.proxy_status_label.config(text=f"❌ Ошибка: {r.status_code}", foreground="red")
        except Exception as e:
            self.proxy_status_label.config(text=f"❌ Ошибка: {str(e)}", foreground="red")
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
        self.profile_status_label.config(text="Новый профиль - заполните поля и нажмите «Сохранить»", foreground="gray")

    def _collect_profile_from_form(self):
        name = self.profile_name_entry.get().strip()
        query = self.profile_query_entry.get().strip()
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
            self.profile_status_label.config(text=f"❌ {e}", foreground="red")
            return
        try:
            if self._current_profile_id is None:
                new_id = database.create_search_profile(name, city, filters)
                self._current_profile_id = new_id
                self.profile_status_label.config(text=f"✅ Профиль «{name}» создан", foreground="green")
            else:
                database.update_search_profile(
                    self._current_profile_id, name=name, city=city, filters=filters,
                )
                self.profile_status_label.config(text=f"✅ Профиль «{name}» обновлён", foreground="green")
        except Exception as e:
            self.profile_status_label.config(text=f"❌ Ошибка: {e}", foreground="red")
            logger.error(f"Ошибка сохранения профиля: {e}")
            return
        self.refresh_profiles_list()

    def profile_delete(self):
        if self._current_profile_id is None:
            self.profile_status_label.config(text="❌ Профиль не выбран", foreground="red")
            return
        if not messagebox.askyesno("Удалить профиль", "Вы уверены, что хотите удалить этот профиль?"):
            return
        try:
            database.delete_search_profile(self._current_profile_id)
            self.profile_status_label.config(text="✅ Профиль удалён", foreground="green")
            self._clear_profile_form()
            self.refresh_profiles_list()
        except Exception as e:
            self.profile_status_label.config(text=f"❌ Ошибка: {e}", foreground="red")

    def profile_set_active(self):
        if self._current_profile_id is None:
            self.profile_status_label.config(text="❌ Профиль не выбран", foreground="red")
            return
        try:
            database.set_active_profile(self._current_profile_id)
            self.profile_status_label.config(text="✅ Профиль сделан активным", foreground="green")
            self.refresh_profiles_list()
        except Exception as e:
            self.profile_status_label.config(text=f"❌ Ошибка: {e}", foreground="red")

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
            self.profile_status_label.config(text="❌ Профиль не выбран", foreground="red")
            return
        profile = database.get_search_profile(self._current_profile_id)
        if not profile:
            return
        self._apply_profile_to_search_tab(profile)
        self.profile_status_label.config(
            text=f"✅ Профиль «{profile['name']}» загружен во вкладку поиска", foreground="green",
        )
        self.notebook.select(0)

    def _apply_profile_to_search_tab(self, profile):
        filters = profile.get("filters") or {}
        self.query_entry.delete(0, tk.END)
        self.query_entry.insert(0, filters.get("query", ""))
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
            self.start_button.config(state='normal')
            if not self.auto_update:
                self.stop_button.config(state='disabled')
            if self.auto_update:
                self.root.after(100, self.schedule_next_auto)
            return

        proxy_settings = self._get_proxy_settings()

        if not self.driver_manager.ensure_driver(proxy_settings, self.log):
            self.log("Не удалось создать драйвер. Парсинг невозможен.")
            self.progress.stop()
            self.start_button.config(state='normal')
            self.stop_button.config(state='disabled')
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
            new_results = self.parse_items(items, min_price, max_price)
            self.log(f"Отобрано по цене: {len(new_results)}")

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

        except Exception as e:
            error_trace = traceback.format_exc()
            self.log(f"Ошибка парсинга: {str(e)}")
            logger.error(f"Ошибка парсинга: {error_trace}")
            self.send_tg_status(f"❌ Ошибка: {str(e)}")
            self.send_error_telegram(error_trace)
        finally:
            self.progress.stop()
            self.start_button.config(state='normal')
            if not self.auto_update:
                self.stop_button.config(state='disabled')
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

    def parse_items(self, items, min_price, max_price):
        result = []
        total = len(items)
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
                    (By.XPATH, ".//*[contains(@class, 'description')]"),
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

                date_str = self.extract_date(item)
                timestamp = parse_date_to_timestamp(date_str)
                item_id = self.get_item_id(item)
                if not item_id:
                    self.log("⛔ Не удалось получить ID - пропущено")
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
    def send_telegram_notification(self, added):
        if not self.update_telegram_notifier():
            return
        if added <= 0:
            return

        new_items = [item for item in self.all_items if item.get("is_new", False)]
        if not new_items:
            return

        MAX_LEN = 4000
        messages = []
        header = f"<b>🔔 Найдено новых объявлений: {added}</b>\n\n"
        current_msg = header

        for item in new_items:
            block = f"• <a href='{item['link']}'>{item['title']}</a>\n"
            block += f"  💰 Цена: {item['price']} руб.\n"
            block += f"  🕒 Добавлено: {item.get('first_seen', 'Н/Д')}\n"

            desc = item.get('description', '')
            if desc and desc != "Н/Д":
                if len(desc) > 200:
                    desc = desc[:200] + "..."
                block += f"  📝 {desc}\n"

            if item.get('image_url') and item['image_url'] != "Н/Д":
                block += f"  🖼️ <a href='{item['image_url']}'>Изображение</a>\n"

            block += "\n"

            if len(current_msg) + len(block) > MAX_LEN:
                messages.append(current_msg)
                current_msg = "🔹 Продолжение списка:\n\n" + block
            else:
                current_msg += block

        if current_msg:
            messages.append(current_msg)

        for msg in messages:
            self.telegram_notifier.send_message(msg)

    # ---------- Загрузка изображений ----------
    def _load_image_async(self, session, image_url, img_label, card):
        try:
            resp = session.get(image_url, timeout=15, stream=True)
            if resp.status_code == 200:
                img = Image.open(BytesIO(resp.content))
                img.thumbnail((150, 150))
                self.root.after(0, lambda: self._set_image(img, img_label))
            else:
                self.root.after(0, lambda: self._set_image_fallback(image_url, img_label, card))
        except Exception:
            self.root.after(0, lambda: self._set_image_fallback(image_url, img_label, card))

    def _set_image(self, pil_image, img_label):
        try:
            photo = ImageTk.PhotoImage(pil_image)
            self.images.append(photo)
            img_label.config(image=photo)
        except Exception:
            pass

    def _set_image_fallback(self, url, img_label, card):
        try:
            img_label.destroy()
            btn = ttk.Button(card, text="📷 Открыть фото",
                             command=lambda: webbrowser.open(url))
            btn.grid(row=1, column=0, padx=5, pady=5)
            btn.configure(style='Accent.TButton')
        except Exception:
            pass

    def display_results(self):
        try:
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

            for item in self.all_items:
                card = ttk.Frame(self.results_frame, relief="solid", borderwidth=1, padding=10)
                card.pack(fill="x", padx=5, pady=5)

                hover_handled = False

                def on_enter(event, _item=item, _card=card):
                    nonlocal hover_handled
                    if not hover_handled and _item.get("is_new", False):
                        hover_handled = True
                        _item["is_new"] = False
                        _card.config(style="")
                        self.root.after(100, self._save_data)

                card.bind("<Enter>", on_enter, add="+")

                if item.get("is_new", False):
                    style = ttk.Style()
                    style.configure("New.TFrame", background="#ffcccc")
                    card.config(style="New.TFrame")

                header = ttk.Frame(card)
                header.grid(row=0, column=0, columnspan=2, sticky="w", pady=5)
                ttk.Label(header, text=item['title'], font=('Arial', 12, 'bold')).pack(side="left")

                img_label = ttk.Label(card)
                img_label.grid(row=1, column=0, rowspan=5, padx=5, pady=5, sticky="n")

                if item['image_url'] != "Н/Д":
                    self.image_executor.submit(
                        self._load_image_async, session, item['image_url'], img_label, card
                    )
                else:
                    img_label.config(text="[нет фото]")

                ttk.Label(card, text=f"Цена: {item['price']} руб.", font=('Arial', 10)).grid(row=1, column=1,
                                                                                              sticky="w")

                desc = tk.Text(card, height=4, wrap=tk.WORD, font=('Arial', 9))
                desc.insert("1.0", item['description'])
                desc.config(state='disabled')
                desc.grid(row=2, column=1, sticky="w", pady=5)

                first_seen = item.get("first_seen", "Н/Д")
                ttk.Label(card, text=f"Время добавления в программу: {first_seen}", font=('Arial', 8)).grid(row=3,
                                                                                                              column=1,
                                                                                                              sticky="w")

                link_label = ttk.Label(card, text="Открыть объявление", foreground="blue", cursor="hand2")
                link_label.grid(row=4, column=1, sticky="w")
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
        self.start_button.config(state='disabled')
        self.stop_button.config(state='normal')
        self.progress.start()
        self.log("Ручной парсинг...")
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
            self.auto_button.config(text="Автообновление вкл", state='disabled')
            self.stop_button.config(state='normal')
            self.log("Автообновление запущено")
            self.run_auto_parsing()
        else:
            self.stop_auto_update()

    def stop_auto_update(self):
        self.auto_update = False
        self.auto_button.config(text="Автообновление", state='normal')
        if not self.driver_manager.driver or not self.stop_parsing:
            self.stop_button.config(state='disabled')
        self.log("Автообновление остановлено")

    def stop_parsing_handler(self):
        self.stop_parsing = True
        self.stop_auto_update()
        self.log("⏹️ Запрос на остановку парсинга отправлен")
        self.send_tg_status("⏹️ Парсер остановлен")

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
