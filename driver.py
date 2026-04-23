import os
import random
import shutil
import stat
import subprocess
import sys
import tempfile
import traceback

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium_stealth import stealth
from selenium.common.exceptions import WebDriverException

from config import USER_AGENTS
from logger_setup import logger
from errors import format_user_error


def _kill_process_tree(pid):
    """Убивает процесс вместе со ВСЕМ деревом потомков.

    Зачем: chromedriver порождает много chrome.exe (браузер, рендереры, GPU,
    network service). На Windows при process.kill() родителя дети остаются
    висеть в Диспетчере задач и держат user-data-dir блокировкой
    SingletonLock/SingletonCookie - новый запуск профиля падает.
    """
    if not pid:
        return
    try:
        if sys.platform == "win32":
            subprocess.run(
                ["taskkill", "/F", "/T", "/PID", str(pid)],
                capture_output=True,
                timeout=10,
            )
        else:
            try:
                import psutil
                parent = psutil.Process(pid)
                for ch in parent.children(recursive=True):
                    try:
                        ch.kill()
                    except Exception:
                        pass
                parent.kill()
            except ImportError:
                try:
                    os.kill(pid, 9)
                except Exception:
                    pass
    except Exception as e:
        logger.warning(f"tree-kill pid={pid} упал: {e}")


class DriverManager:
    def __init__(self):
        self.driver = None
        self.extension_dir = None

    def _create_proxy_extension(self, proxy_scheme, proxy_host, proxy_port, proxy_user, proxy_pass):
        """Создает прокси-расширение во временной директории."""
        ext_dir = tempfile.mkdtemp(prefix="avito_proxy_")
        os.chmod(ext_dir, stat.S_IRWXU)

        manifest_json = """{
    "version": "1.0.0",
    "manifest_version": 2,
    "name": "Chrome Proxy",
    "permissions": [
        "proxy", "tabs", "unlimitedStorage", "storage",
        "<all_urls>", "webRequest", "webRequestBlocking"
    ],
    "background": { "scripts": ["background.js"] },
    "minimum_chrome_version": "22.0.0"
}"""

        background_js = f"""
var config = {{
    mode: "fixed_servers",
    rules: {{
        singleProxy: {{
            scheme: "{proxy_scheme}",
            host: "{proxy_host}",
            port: parseInt({proxy_port})
        }},
        bypassList: ["localhost"]
    }}
}};

chrome.proxy.settings.set({{value: config, scope: "regular"}}, function() {{}});

function callbackFn(details) {{
    return {{
        authCredentials: {{
            username: "{proxy_user}",
            password: "{proxy_pass}"
        }}
    }};
}}

chrome.webRequest.onAuthRequired.addListener(
    callbackFn,
    {{urls: ["<all_urls>"]}},
    ['blocking']
);
"""

        manifest_path = os.path.join(ext_dir, "manifest.json")
        bg_path = os.path.join(ext_dir, "background.js")

        with open(manifest_path, "w", encoding="utf-8") as f:
            f.write(manifest_json)
        os.chmod(manifest_path, stat.S_IRUSR | stat.S_IWUSR)

        with open(bg_path, "w", encoding="utf-8") as f:
            f.write(background_js)
        os.chmod(bg_path, stat.S_IRUSR | stat.S_IWUSR)

        return ext_dir

    def create_driver(self, proxy_settings, log_callback=None, show_browser=False, user_data_dir=None):
        """Создает новый экземпляр ChromeDriver.

        proxy_settings: dict с ключами scheme, host, port, user, pass
        log_callback: функция для логирования (например app.log)
        show_browser: если True - запускаем без headless (окно видно на экране).
        user_data_dir: путь к постоянному профилю Chrome. Позволяет сохранять
            куки (в т.ч. решённой капчи) между перезапусками драйвера.
        """
        try:
            if self.extension_dir and os.path.exists(self.extension_dir):
                shutil.rmtree(self.extension_dir, ignore_errors=True)

            self.extension_dir = self._create_proxy_extension(
                proxy_settings.get('scheme', 'http'),
                proxy_settings.get('host', ''),
                proxy_settings.get('port', ''),
                proxy_settings.get('user', ''),
                proxy_settings.get('pass', ''),
            )

            user_agent = random.choice(USER_AGENTS)

            options = Options()
            if not show_browser:
                options.add_argument('--headless=new')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-gpu')
            options.add_argument('--window-size=1920,1080')
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_argument(f'--load-extension={self.extension_dir}')
            options.add_argument(f"user-agent={user_agent}")
            if user_data_dir:
                try:
                    os.makedirs(user_data_dir, exist_ok=True)
                    options.add_argument(f'--user-data-dir={user_data_dir}')
                except Exception as e:
                    logger.warning(f"Не удалось использовать user-data-dir {user_data_dir}: {e}")
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option('useAutomationExtension', False)

            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=options)

            stealth(driver,
                    languages=["ru-RU", "ru"],
                    vendor="Google Inc.",
                    platform="Win32",
                    webgl_vendor="Intel Inc.",
                    renderer="Intel Iris OpenGL Engine",
                    fix_hairline=True,
                    )

            self.driver = driver
            return driver
        except Exception as e:
            error_trace = traceback.format_exc()
            if log_callback:
                log_callback(format_user_error(e, context="driver"))
            logger.error(f"Ошибка создания драйвера: {error_trace}")
            return None

    def ensure_driver(self, proxy_settings, log_callback=None, show_browser=False, user_data_dir=None):
        """Проверяет что драйвер жив, пересоздает если нет."""
        if self.driver is None:
            self.driver = self.create_driver(proxy_settings, log_callback, show_browser=show_browser, user_data_dir=user_data_dir)
            return self.driver is not None
        try:
            self.driver.current_url
            return True
        except Exception:
            if log_callback:
                log_callback("Драйвер не отвечает, пересоздаём...")
            logger.warning("Драйвер не отвечает, пересоздаём...")
            try:
                self.driver.quit()
            except Exception:
                pass
            self.driver = self.create_driver(proxy_settings, log_callback, show_browser=show_browser, user_data_dir=user_data_dir)
            return self.driver is not None

    def cleanup(self):
        """Закрывает драйвер и удаляет временные файлы.

        Порядок: забираем pid chromedriver до quit(), пытаемся graceful quit,
        затем tree-kill как страховку - даже если quit прошёл, иногда chrome.exe
        остаётся висеть в Windows (гонки при закрытии программы).
        """
        if self.driver:
            pid = None
            try:
                service = getattr(self.driver, "service", None)
                process = getattr(service, "process", None) if service else None
                if process is not None:
                    pid = process.pid
            except Exception:
                pass
            try:
                self.driver.quit()
            except Exception:
                pass
            if pid:
                _kill_process_tree(pid)
            self.driver = None

        if self.extension_dir and os.path.exists(self.extension_dir):
            shutil.rmtree(self.extension_dir, ignore_errors=True)
            self.extension_dir = None

    def hard_kill(self):
        """Жёсткая остановка: убиваем chromedriver + все chrome.exe-потомки.
        В отличие от cleanup()/quit() не виснет если worker-поток параллельно
        держит HTTP-соединение."""
        if self.driver is not None:
            try:
                service = getattr(self.driver, "service", None)
                process = getattr(service, "process", None) if service else None
                if process is not None:
                    _kill_process_tree(process.pid)
            except Exception as e:
                logger.warning(f"hard_kill: {e}")
            self.driver = None

        if self.extension_dir and os.path.exists(self.extension_dir):
            shutil.rmtree(self.extension_dir, ignore_errors=True)
            self.extension_dir = None
