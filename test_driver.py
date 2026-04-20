"""
Автотесты для DriverManager - проверяют:
- ensure_driver ловит любые exception при probe (включая urllib3 ошибки)
- hard_kill мгновенно убивает процесс без HTTP-вызовов
- cleanup не крашится при мёртвом драйвере

Запуск: python test_driver.py
"""

import sys
import traceback
from unittest.mock import MagicMock, patch

from selenium.common.exceptions import WebDriverException

from driver import DriverManager

failures = []


def test(name):
    def decorator(func):
        try:
            func()
            print(f"  ✅ {name}")
        except AssertionError as e:
            failures.append((name, str(e), traceback.format_exc()))
            print(f"  ❌ {name}: {e}")
        except Exception as e:
            failures.append((name, f"{type(e).__name__}: {e}", traceback.format_exc()))
            print(f"  💥 {name}: {type(e).__name__}: {e}")
        return func
    return decorator


print("\n=== ensure_driver ===")


@test("probe через urllib3 MaxRetryError ловится, driver пересоздаётся")
def test_urllib3_max_retry():
    import urllib3.exceptions

    dm = DriverManager()
    dead_driver = MagicMock()
    type(dead_driver).current_url = property(
        lambda _: (_ for _ in ()).throw(
            urllib3.exceptions.MaxRetryError(pool=None, url="http://x")
        )
    )
    dm.driver = dead_driver

    with patch.object(dm, "create_driver", return_value=MagicMock()) as mock_create:
        result = dm.ensure_driver(proxy_settings={}, log_callback=None)
        assert result is True, "ensure_driver должен вернуть True после пересоздания"
        assert mock_create.called, "create_driver должен быть вызван"


@test("probe через NewConnectionError (ConnectionRefused) ловится")
def test_urllib3_connection_refused():
    import urllib3.exceptions

    dm = DriverManager()
    dead_driver = MagicMock()
    type(dead_driver).current_url = property(
        lambda _: (_ for _ in ()).throw(
            urllib3.exceptions.NewConnectionError(None, "refused")
        )
    )
    dm.driver = dead_driver

    with patch.object(dm, "create_driver", return_value=MagicMock()) as mock_create:
        result = dm.ensure_driver(proxy_settings={}, log_callback=None)
        assert result is True
        assert mock_create.called


@test("probe через ConnectionRefusedError (raw socket) ловится")
def test_raw_connection_refused():
    dm = DriverManager()
    dead_driver = MagicMock()
    type(dead_driver).current_url = property(
        lambda _: (_ for _ in ()).throw(ConnectionRefusedError("refused"))
    )
    dm.driver = dead_driver

    with patch.object(dm, "create_driver", return_value=MagicMock()) as mock_create:
        result = dm.ensure_driver(proxy_settings={}, log_callback=None)
        assert result is True
        assert mock_create.called


@test("WebDriverException продолжает ловиться (регрессия)")
def test_webdriver_exception_still_caught():
    dm = DriverManager()
    dead_driver = MagicMock()
    type(dead_driver).current_url = property(
        lambda _: (_ for _ in ()).throw(WebDriverException("dead"))
    )
    dm.driver = dead_driver

    with patch.object(dm, "create_driver", return_value=MagicMock()) as mock_create:
        result = dm.ensure_driver(proxy_settings={}, log_callback=None)
        assert result is True
        assert mock_create.called


@test("живой драйвер не пересоздаётся")
def test_live_driver_not_recreated():
    dm = DriverManager()
    live_driver = MagicMock()
    live_driver.current_url = "https://avito.ru"
    dm.driver = live_driver

    with patch.object(dm, "create_driver", return_value=MagicMock()) as mock_create:
        result = dm.ensure_driver(proxy_settings={}, log_callback=None)
        assert result is True
        assert not mock_create.called, "create_driver НЕ должен вызываться для живого"
        assert dm.driver is live_driver


@test("None-драйвер создаётся")
def test_none_driver_created():
    dm = DriverManager()
    dm.driver = None

    fake_new = MagicMock()
    with patch.object(dm, "create_driver", return_value=fake_new) as mock_create:
        result = dm.ensure_driver(proxy_settings={}, log_callback=None)
        assert result is True
        assert mock_create.called
        assert dm.driver is fake_new


print("\n=== hard_kill ===")


@test("hard_kill вызывает service.process.kill(), обнуляет driver")
def test_hard_kill_calls_process_kill():
    dm = DriverManager()
    mock_driver = MagicMock()
    mock_process = MagicMock()
    mock_driver.service.process = mock_process
    dm.driver = mock_driver

    dm.hard_kill()

    assert mock_process.kill.called, "process.kill() должен быть вызван"
    assert dm.driver is None, "driver должен быть обнулён"


@test("hard_kill не вызывает driver.quit() (не HTTP)")
def test_hard_kill_skips_quit():
    dm = DriverManager()
    mock_driver = MagicMock()
    mock_driver.service.process = MagicMock()
    dm.driver = mock_driver

    dm.hard_kill()

    assert not mock_driver.quit.called, "quit() не должен вызываться в hard_kill"


@test("hard_kill не падает если driver=None")
def test_hard_kill_none_driver():
    dm = DriverManager()
    dm.driver = None
    dm.hard_kill()
    assert dm.driver is None


@test("hard_kill не падает если process.kill() бросает")
def test_hard_kill_swallow_exception():
    dm = DriverManager()
    mock_driver = MagicMock()
    mock_driver.service.process.kill.side_effect = OSError("already dead")
    dm.driver = mock_driver

    dm.hard_kill()

    assert dm.driver is None, "driver должен обнулиться даже при ошибке kill"


@test("hard_kill чистит extension_dir")
def test_hard_kill_cleans_extension_dir():
    import tempfile
    import os

    dm = DriverManager()
    dm.driver = None
    ext_dir = tempfile.mkdtemp(prefix="test_ext_")
    dm.extension_dir = ext_dir

    assert os.path.isdir(ext_dir)
    dm.hard_kill()
    assert not os.path.isdir(ext_dir), "extension_dir должна быть удалена"
    assert dm.extension_dir is None


print("\n=== итог ===")
if failures:
    print(f"\n❌ {len(failures)} тестов упало:")
    for name, msg, tb in failures:
        print(f"\n--- {name} ---\n{tb}")
    sys.exit(1)
else:
    print("\n✅ Все тесты прошли")
    sys.exit(0)
