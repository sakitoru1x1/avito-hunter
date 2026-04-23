"""Авто-решение капчи Avito через RuCaptcha / 2Captcha API.

Avito firewall показывает один из трёх типов капчи:
1. GeeTest v4 (перетащи фигуру) - captchaId в JS, initGeetest4()
2. hCaptcha (галочка "я не робот") - data-sitekey на .h-captcha div
3. Avito image captcha (текст с картинки) - собственная реализация

Тип определяется сервером через /web/1/firewallCaptcha/get.
Верификация через /web/1/firewallCaptcha/verify с X-Cube заголовком.
"""
import json
import time

from twocaptcha import TwoCaptcha

from logger_setup import logger


CAPTCHA_SERVERS = {
    "rucaptcha": "rucaptcha.com",
    "2captcha": "2captcha.com",
}

_DETECT_JS = """
var url = window.location.href;

// Какой блок видим?
var geeDiv = document.getElementById('geetest_captcha');
var hDiv = document.getElementById('h-captcha');
var avitoDiv = document.getElementById('inner-captcha');

// GeeTest v4: ищем captchaId в скриптах
if (geeDiv && geeDiv.style.display !== 'none') {
    var scripts = document.querySelectorAll('script');
    var captchaId = '';
    for (var i = 0; i < scripts.length; i++) {
        var text = scripts[i].textContent || '';
        var m = text.match(/captchaId\\s*=\\s*['"]([a-f0-9]{32})['"]/);
        if (m) { captchaId = m[1]; break; }
    }
    if (captchaId) return {type: 'geetest_v4', captcha_id: captchaId, url: url};
}

// hCaptcha: ищем sitekey
if (hDiv && hDiv.style.display !== 'none') {
    var hcEl = hDiv.querySelector('[data-sitekey]') || hDiv.querySelector('.h-captcha');
    var sitekey = '';
    if (hcEl) sitekey = hcEl.getAttribute('data-sitekey') || '';
    if (sitekey) return {type: 'hcaptcha', sitekey: sitekey, url: url};
}

// Avito image captcha
if (avitoDiv && avitoDiv.style.display !== 'none') {
    var img = avitoDiv.querySelector('.js-form-captcha-image');
    var imgSrc = img ? img.getAttribute('src') : '';
    return {type: 'avito_image', image_src: imgSrc, url: url};
}

// Fallback: пробуем найти хоть что-то
var scripts = document.querySelectorAll('script');
for (var i = 0; i < scripts.length; i++) {
    var text = scripts[i].textContent || '';
    var m = text.match(/captchaId\\s*=\\s*['"]([a-f0-9]{32})['"]/);
    if (m) return {type: 'geetest_v4', captcha_id: m[1], url: url};
}
var hcAny = document.querySelector('.h-captcha[data-sitekey]');
if (hcAny) return {type: 'hcaptcha', sitekey: hcAny.getAttribute('data-sitekey'), url: url};

return {type: 'unknown'};
"""

_APPLY_GEETEST_V4_JS = """
var solution = arguments[0];
fetch('/web/1/firewallCaptcha/verify', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({
        captcha: '',
        hCaptchaResponse: '',
        captcha_id: solution.captcha_id || '',
        lot_number: solution.lot_number || '',
        pass_token: solution.pass_token || '',
        gen_time: solution.gen_time || '',
        captcha_output: solution.captcha_output || ''
    }),
    credentials: 'same-origin'
}).then(function(r) { return r.json(); }).then(function(data) {
    window.location.reload();
}).catch(function() {
    window.location.reload();
});
return true;
"""

_APPLY_HCAPTCHA_JS = """
var token = arguments[0];
var textarea = document.getElementById('h-captcha-response');
if (textarea) textarea.value = token;
var textarea2 = document.querySelector('[name="h-captcha-response"]');
if (textarea2) textarea2.value = token;
var form = document.querySelector('.js-firewall-form');
if (form) {
    form.dispatchEvent(new Event('submit'));
}
return true;
"""


class CaptchaSolver:
    """Авто-решение капчи Avito через RuCaptcha/2Captcha."""

    def __init__(self, api_key, service="rucaptcha", log_func=None):
        server = CAPTCHA_SERVERS.get(service, "rucaptcha.com")
        self.solver = TwoCaptcha(api_key, server=server)
        self.log = log_func or (lambda msg: None)
        self.service_name = "RuCaptcha" if service == "rucaptcha" else "2Captcha"

    def detect_captcha_type(self, driver):
        try:
            return driver.execute_script(_DETECT_JS)
        except Exception as e:
            logger.warning(f"Ошибка определения типа капчи: {e}")
            return {"type": "unknown"}

    def _wait_captcha_resolved(self, driver, timeout=20):
        """Ждёт пока страница перезагрузится после verify.

        Сначала ждём reload (fetch → .then → location.reload асинхронный),
        потом проверяем появление карточек ИЛИ исчезновение маркеров капчи.
        """
        time.sleep(3)
        for _ in range(timeout):
            time.sleep(1)
            try:
                ready = driver.execute_script("return document.readyState")
                if ready != "complete":
                    continue
                src = (driver.page_source or "").lower()
                has_items = 'data-marker="item"' in src or "data-marker='item'" in src
                if has_items:
                    self.log("✅ Карточки найдены, капча пройдена!")
                    return True
                has_captcha = any(
                    m in src for m in ("firewallcaptcha", "geetest_captcha", "доступ ограничен")
                )
                if not has_captcha and "avito" in src:
                    self.log("✅ Страница без капчи, продолжаем")
                    return True
            except Exception:
                pass
        return False

    def solve(self, driver):
        info = {"type": "unknown"}
        for wait in (0, 2, 3, 5, 5):
            if wait:
                time.sleep(wait)
            info = self.detect_captcha_type(driver)
            if info.get("type") != "unknown":
                break
        captcha_type = info.get("type", "unknown")
        self.log(f"🔍 Тип капчи: {captcha_type}")

        if captcha_type == "geetest_v4":
            return self._solve_geetest_v4(driver, info)
        elif captcha_type == "hcaptcha":
            return self._solve_hcaptcha(driver, info)
        elif captcha_type == "avito_image":
            self.log("⚠️ Avito image captcha - авто-решение не поддерживается")
            return False
        else:
            from parser import is_captcha_page
            if not is_captcha_page(driver):
                self.log("✅ Капча уже пропала, продолжаем")
                return True
            self.log("⚠️ Неизвестный тип капчи, авто-решение невозможно")
            return False

    def _solve_geetest_v4(self, driver, info, max_attempts=3):
        captcha_id = info["captcha_id"]
        self.log(f"🤖 Решаю GeeTest v4 через {self.service_name} (id: {captcha_id[:8]}...)")

        for attempt in range(1, max_attempts + 1):
            try:
                if attempt > 1:
                    self.log(f"🔄 Попытка {attempt}/{max_attempts}...")
                result = self.solver.geetest_v4(
                    captcha_id=captcha_id,
                    url=info["url"],
                )
                code = result.get("code", result)
                self.log(f"✅ Решение получено (попытка {attempt}), применяю...")

                if isinstance(code, str):
                    try:
                        solution = json.loads(code)
                    except (json.JSONDecodeError, TypeError):
                        solution = code
                else:
                    solution = code

                if isinstance(solution, dict):
                    solution["captcha_id"] = captcha_id

                driver.execute_script(_APPLY_GEETEST_V4_JS, solution)
                self.log("📝 Отправлено на /firewallCaptcha/verify, жду...")

                if self._wait_captcha_resolved(driver):
                    return True

                self.log("⚠️ Капча всё ещё на странице после применения решения")
                return False
            except Exception as e:
                err = str(e)
                if "UNSOLVABLE" in err and attempt < max_attempts:
                    self.log(f"⚠️ Воркер не смог решить (попытка {attempt}), пробую ещё...")
                    continue
                self.log(f"❌ GeeTest v4 не удалось: {e}")
                logger.error(f"GeeTest v4 solver error: {e}")
                return False
        return False

    def _solve_hcaptcha(self, driver, info):
        sitekey = info["sitekey"]
        self.log(f"🤖 Решаю hCaptcha через {self.service_name} (key: {sitekey[:8]}...)")
        try:
            result = self.solver.hcaptcha(
                sitekey=sitekey,
                url=info["url"],
            )
            token = result.get("code", result)
            self.log("✅ Токен получен, применяю...")

            driver.execute_script(_APPLY_HCAPTCHA_JS, token)
            time.sleep(3)

            from parser import is_captcha_page
            if not is_captcha_page(driver):
                return True

            self.log("⚠️ Капча всё ещё на странице после применения токена")
            return False
        except Exception as e:
            self.log(f"❌ hCaptcha не удалось: {e}")
            logger.error(f"hCaptcha solver error: {e}")
            return False

    def check_balance(self):
        try:
            balance = self.solver.balance()
            return True, f"{balance}"
        except Exception as e:
            return False, str(e)
