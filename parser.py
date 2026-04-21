"""Парсер карточек Avito. Чистая логика: DOM → dict, без Tk и БД."""
import re

from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException

from utils import parse_date_to_timestamp
from logger_setup import logger


CAPTCHA_MARKERS = [
    "captcha",
    "firewall",
    "доступ ограничен",
    "подтвердите, что вы не робот",
    "access-confirm",
    "are you a robot",
]


def is_captcha_page(driver):
    """Похоже ли текущая страница на стену капчи/firewall."""
    try:
        src = (driver.page_source or "").lower()
    except Exception:
        return False
    return any(m in src for m in CAPTCHA_MARKERS)


def extract_date(item):
    date_selectors = [
        (By.CSS_SELECTOR, "[data-marker='item-date']"),
        (By.XPATH, ".//span[contains(@class, 'date')]"),
        (By.XPATH, ".//time"),
        (By.XPATH, ".//*[contains(text(), 'сегодня') or contains(text(), 'вчера')]"),
    ]
    for by, selector in date_selectors:
        try:
            elem = item.find_element(by, selector)
            return elem.text.strip()
        except NoSuchElementException:
            continue
    return "Н/Д"


def get_item_id(item):
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


def parse_ignore_words(raw):
    if not raw:
        return []
    return [w.strip().lower() for w in raw.split(",") if w.strip()]


def normalize_title(title):
    t = (title or "").lower()
    t = re.sub(r"[^\w\s]", " ", t, flags=re.UNICODE)
    return set(w for w in t.split() if len(w) >= 3)


def is_duplicate(new_item, existing_items):
    new_price = new_item.get("price") or 0
    new_title_words = normalize_title(new_item.get("title", ""))
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
        old_words = normalize_title(old.get("title", ""))
        if not old_words:
            continue
        overlap = len(new_title_words & old_words)
        union = len(new_title_words | old_words)
        if union > 0 and overlap / union >= 0.7:
            return True
    return False


def detect_disappeared(all_items, page_summary, current_query):
    """Находит объявления, которые были активны в выдаче и пропали в текущем парсе."""
    if not page_summary or not all_items:
        return []
    new_ids = {item["id"] for item in page_summary}
    min_ts = min((r.get("pub_date_timestamp", 0) or 0) for r in page_summary)
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


class AvitoParser:
    """Парсер страницы листинга Avito. Не знает про Tk и БД - только driver + dict."""

    def __init__(self, log):
        self.log = log

    def extract_image_urls_batch(self, driver):
        """Одним JS-запросом извлекает image_url для всех карточек страницы.

        Пробует src, currentSrc, srcset, <picture><source> srcset в порядке надёжности.
        srcset и picture-источники обычно проставлены Avito сразу, даже когда src ещё
        placeholder (data:...) - так обходим lazy-loader.

        Returns:
            dict {item_id: url_str or None}
        """
        js = r"""
        const cards = document.querySelectorAll("[data-marker='item']");
        const out = {};
        const pickSrcset = (ss) => {
            if (!ss) return null;
            const parts = String(ss).split(',').map(s => s.trim().split(/\s+/)[0]).filter(Boolean);
            return parts.length ? parts[parts.length - 1] : null;
        };
        const isReal = (u) => u && !u.startsWith('data:');
        for (const c of cards) {
            const id = c.getAttribute('data-item-id') || c.id;
            if (!id) continue;
            let url = null;
            const img = c.querySelector("img[data-marker='image']") || c.querySelector('img');
            if (img) {
                if (isReal(img.currentSrc)) url = img.currentSrc;
                if (!url) {
                    const src = img.getAttribute('src') || '';
                    if (isReal(src)) url = src;
                }
                if (!url) {
                    const picked = pickSrcset(img.srcset || img.getAttribute('srcset'));
                    if (isReal(picked)) url = picked;
                }
                if (!url) {
                    const dsrc = img.getAttribute('data-src') || '';
                    if (isReal(dsrc)) url = dsrc;
                }
                if (!url) {
                    const picked = pickSrcset(img.getAttribute('data-srcset'));
                    if (isReal(picked)) url = picked;
                }
            }
            if (!url) {
                const sources = c.querySelectorAll('picture source');
                for (const s of sources) {
                    let picked = pickSrcset(s.srcset || s.getAttribute('srcset'));
                    if (!isReal(picked)) picked = pickSrcset(s.getAttribute('data-srcset'));
                    if (isReal(picked)) { url = picked; break; }
                }
            }
            out[id] = isReal(url) ? url : null;
        }
        return out;
        """
        try:
            return driver.execute_script(js) or {}
        except Exception as e:
            logger.warning(f"Batch image extraction failed: {e}")
            return {}

    def fetch_detail_pages_batch(self, driver, id_link_pairs):
        """Параллельно забирает дату и полное описание со страниц объявлений.

        Один HTTP-запрос на страницу закрывает обе задачи. Ходим через
        driver.execute_async_script с fetch() - наследуем cookies/прокси Selenium.
        """
        if not id_link_pairs or not driver:
            return {}
        js = r"""
        const pairs = arguments[0];
        const done = arguments[arguments.length - 1];
        (async () => {
            const strip = (s) => s
                .replace(/<!--[\s\S]*?-->/g, '')
                .replace(/<br\s*\/?>/gi, '\n')
                .replace(/<\/p>/gi, '\n')
                .replace(/<[^>]+>/g, '')
                .replace(/&nbsp;/g, ' ')
                .replace(/&amp;/g, '&')
                .replace(/&lt;/g, '<')
                .replace(/&gt;/g, '>')
                .replace(/&quot;/g, '"')
                .replace(/·/g, ' ')
                .replace(/[ \t]+/g, ' ')
                .replace(/\n{3,}/g, '\n\n')
                .trim();
            const fetchOne = async ([id, url]) => {
                try {
                    const r = await fetch(url, {credentials: 'include'});
                    if (!r.ok) return [id, null];
                    const html = await r.text();
                    const dm = html.match(/data-marker="item-view\/item-date"[^>]*>([\s\S]*?)<\/span>/);
                    const desc_m = html.match(/data-marker="item-view\/item-description"[^>]*>([\s\S]*?)<\/div>\s*<\/div>/)
                        || html.match(/data-marker="item-view\/item-description"[^>]*>([\s\S]*?)<\/div>/);
                    const date_text = dm ? strip(dm[1]).replace(/\s+/g, ' ') : null;
                    const desc_text = desc_m ? strip(desc_m[1]) : null;
                    return [id, {date: date_text, description: desc_text}];
                } catch (e) {
                    return [id, null];
                }
            };
            const results = await Promise.all(pairs.map(fetchOne));
            const out = {};
            for (const [id, data] of results) if (data) out[id] = data;
            done(out);
        })();
        """
        try:
            driver.set_script_timeout(60)
            return driver.execute_async_script(js, id_link_pairs) or {}
        except Exception as e:
            logger.warning(f"Batch fetch детальных страниц не удался: {e}")
            return {}

    def parse_items(self, driver, items, min_price, max_price,
                    search_query, filter_services, ignore_words,
                    known_ids, filtered_ids, stop_check):
        """Двухпроходный парсер.

        Pass 1 - лёгкое сканирование (id + дата + image_url) по ВСЕМ карточкам.
        Pass 2 - полная экстракция остальных полей только для новых карточек.

        Args:
            driver: Selenium driver (для batch JS).
            items: list of WebElement карточек.
            min_price, max_price: фильтр цены.
            search_query: текущий поисковый запрос (для search_query в записях).
            filter_services: bool - отбрасывать ли услуги/вакансии.
            ignore_words: list[str] - игнор-слова (в lowercase).
            known_ids: set[str] - id уже известных объявлений (skip pass 2).
            filtered_ids: set[str] - мутируемый set отбракованных (добавляем туда новые
                id которые не прошли фильтры).
            stop_check: callable() -> bool. Если True - прерываем парс.

        Returns:
            tuple (full_new_items, page_summary).
        """
        total = len(items)

        image_urls = self.extract_image_urls_batch(driver) if driver else {}
        got_imgs = sum(1 for v in image_urls.values() if v)
        if total:
            self.log(f"🖼 Batch-извлечение URL картинок: {got_imgs}/{len(image_urls)}")

        page_summary = []
        cards_to_parse = []

        self.log(f"🔍 Скан карточек на новые (всего: {total})...")
        for idx, item in enumerate(items, 1):
            if stop_check():
                self.log("⏹️ Парсинг прерван пользователем (скан)")
                return [], page_summary
            item_id = get_item_id(item)
            if not item_id:
                continue
            date_str = extract_date(item)
            timestamp = parse_date_to_timestamp(date_str)
            img_from_batch = image_urls.get(item_id)
            page_summary.append({
                "id": item_id,
                "pub_date_timestamp": timestamp,
                "search_query": search_query,
                "image_url": img_from_batch or "Н/Д",
            })
            if item_id not in known_ids:
                cards_to_parse.append((item, item_id, date_str, timestamp, img_from_batch))
            if idx % 10 == 0 or idx == total:
                self.log(f"   скан {idx}/{total}, новых пока: {len(cards_to_parse)}")

        skipped = total - len(cards_to_parse)
        self.log(f"📊 На странице: {total}, известно: {skipped}, на разбор: {len(cards_to_parse)}")

        result = []
        new_total = len(cards_to_parse)
        for idx, (item, item_id, date_str, timestamp, img_from_batch) in enumerate(cards_to_parse):
            if stop_check():
                self.log("⏹️ Парсинг прерван пользователем")
                return result, page_summary

            self.log(f"🔄 Обработка новой карточки {idx + 1}/{new_total}...")

            try:
                try:
                    title = item.find_element(By.CSS_SELECTOR, "[itemprop='name']").text
                except NoSuchElementException:
                    title = "Н/Д"

                try:
                    link = item.find_element(By.CSS_SELECTOR, "a[itemprop='url']").get_attribute("href")
                except NoSuchElementException:
                    link = "Н/Д"

                self.log(f"📦 {title}")
                if link and link != "Н/Д":
                    self.log(f"🔗 {link}")

                price_elem = item.find_element(By.CSS_SELECTOR, "[itemprop='price']")
                price = price_elem.get_attribute("content")
                if not price:
                    self.log("⛔ Цена не найдена - пропущено")
                    filtered_ids.add(item_id)
                    continue
                price_int = int(price)
                self.log(f"📄 Цена: {price_int} руб.")

                if price_int < min_price or price_int > max_price:
                    self.log(f"⛔ Цена {price_int} вне диапазона ({min_price}-{max_price}) - пропущено")
                    filtered_ids.add(item_id)
                    continue

                if filter_services:
                    if link and link != "Н/Д":
                        if "predlozheniya_uslug" in link or "vakansii" in link:
                            self.log(f"🔍 ОТФИЛЬТРОВАНО (услуги): {title[:30]}...")
                            filtered_ids.add(item_id)
                            continue

                img_url = img_from_batch or "Н/Д"

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
                        filtered_ids.add(item_id)
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
                    "search_query": search_query,
                    "is_new": False,
                    "first_seen": None,
                })
                self.log(f"✅ Добавлено: {title[:30]}...")

            except Exception as e:
                self.log(f"❌ Исключение при обработке карточки: {e}")
                logger.error(f"Ошибка при парсинге элемента: {e}")
                continue

        # Точное время публикации + полное описание - ходим на страницу каждого
        # нового объявления через fetch() внутри браузера (batch+parallel, с куками/прокси).
        id_link_pairs = [[r["id"], r["link"]] for r in result if r.get("link") and r["link"] != "Н/Д"]
        if id_link_pairs:
            self.log(f"🕐 Получаю детали (дата + полное описание) для {len(id_link_pairs)} объявлений...")
            details = self.fetch_detail_pages_batch(driver, id_link_pairs)
            got_date = 0
            got_desc = 0
            summary_by_id = {s["id"]: s for s in page_summary}
            for r in result:
                d = details.get(r["id"])
                if not d:
                    continue
                date_text = d.get("date")
                desc_text = d.get("description")
                if date_text:
                    ts = parse_date_to_timestamp(date_text)
                    if ts > 0:
                        r["date"] = date_text
                        r["pub_date_timestamp"] = ts
                        s = summary_by_id.get(r["id"])
                        if s:
                            s["pub_date_timestamp"] = ts
                        got_date += 1
                if desc_text and len(desc_text) > 10:
                    r["description"] = desc_text
                    got_desc += 1
            self.log(f"   ✓ дата: {got_date}/{len(id_link_pairs)}, описание: {got_desc}/{len(id_link_pairs)}")

        return result, page_summary
