from __future__ import annotations
import random
import re
import asyncio
import json
import logging
from typing import Optional
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_random_exponential, retry_if_exception_type

# Capture prices like "722", "722,00", "722.5", "722.50".
# NOTE: start from 2 digits to avoid catching unrelated "1" from delivery=1, page=1, etc.
_PRICE_RE = re.compile(r"\b(\d{2,5}(?:[.,]\d{1,2})?)\b")



class RateLimited(Exception):
    """Raised when the server responds with HTTP 429 (rate limited)."""

    def __init__(self, retry_after: Optional[float] = None):
        super().__init__("HTTP 429 (rate limited)")
        self.retry_after = retry_after


# New exception for transient HTTP 5xx errors
class TransientHTTPError(Exception):
    """Raised for transient server-side HTTP errors (5xx) to trigger retries."""

    def __init__(self, status_code: int):
        super().__init__(f"HTTP {status_code} (transient)")
        self.status_code = status_code


class TabletkiScraper:
    def __init__(self, timeout_sec: float, min_delay: float, max_delay: float, logger):
        self.timeout_sec = timeout_sec
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.logger = logger or logging.getLogger(__name__)

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/124.0.0.0 Safari/537.36",
            "Accept-Language": "uk-UA,uk;q=0.9,ru;q=0.8,en;q=0.7",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Connection": "keep-alive",
        }

    def build_delivery_url(self, product_code: str, city_slug: str) -> str:
        """Build initial delivery offers URL.

        We start from the code-only path, then (if the server redirects to a slug-only page)
        we re-fetch a stable URL that includes BOTH slug and product code.
        """
        product_code = str(product_code).strip().strip("/")
        city_slug = str(city_slug).strip().strip("/")
        return f"https://tabletki.ua/uk/{product_code}/pharmacy/{city_slug}/filter/delivery=1/"

    def _extract_product_slug_from_final_url(self, final_url: str, city_slug: str) -> Optional[str]:
        """Extract product slug from final URL.

        Supported formats:
        1) /uk/<slug>/pharmacy/<city>/...
        2) /uk/<slug>/<code>/pharmacy/<city>/...
        """
        try:
            p = urlparse(final_url)
            parts = [x for x in p.path.split("/") if x]
            # e.g. ['uk', '<slug>', 'pharmacy', '<city>', ...]
            if len(parts) >= 4 and parts[0] == "uk" and parts[2] == "pharmacy" and parts[3] == city_slug:
                return parts[1]
            # e.g. ['uk', '<slug>', '<code>', 'pharmacy', '<city>', ...]
            if len(parts) >= 5 and parts[0] == "uk" and parts[3] == "pharmacy" and parts[4] == city_slug:
                return parts[1]
        except Exception:
            return None
        return None

    def _final_url_contains_code(self, final_url: str, product_code: str) -> bool:
        """Return True if the final URL path still contains the product code segment."""
        try:
            p = urlparse(final_url)
            parts = [x for x in p.path.split("/") if x]
            return str(product_code) in parts
        except Exception:
            return False

    def _extract_slug_from_html_for_code(self, html: str, product_code: str) -> Optional[str]:
        """Try to find the canonical slug for a given product code inside HTML.

        Many Tabletki pages embed links like:
          /uk/<slug>/<code>/...
        We use that as the most reliable way to keep the correct product when the server redirects
        code-only URLs to slug-only pages.
        """
        code = re.escape(str(product_code).strip().strip("/"))
        # Prefer an explicit /uk/<slug>/<code>/ match
        m = re.search(r"/uk/([^/\s\"'>]+)/" + code + r"(?:/|\"|'|\?)", html)
        if m:
            return m.group(1)
        return None

    def _build_delivery_price_url(self, product_code: str, city_slug: str, product_slug: Optional[str] = None) -> str:
        """Build stable delivery URL.

        Stable format (no redirect, keeps correct product):
        /uk/<slug>/<code>/pharmacy/<city>/filter/s=price;delivery=1/

        If slug is unknown, falls back to the code-only URL.
        """
        product_code = str(product_code).strip().strip("/")
        city_slug = str(city_slug).strip().strip("/")
        if product_slug:
            product_slug = str(product_slug).strip().strip("/")
            return (
                f"https://tabletki.ua/uk/{product_slug}/{product_code}/pharmacy/{city_slug}/"
                "filter/s=price;delivery=1/"
            )
        return f"https://tabletki.ua/uk/{product_code}/pharmacy/{city_slug}/filter/delivery=1/"

    async def polite_sleep(self):
        # base delay from settings + small extra jitter to avoid rigid patterns
        base = random.uniform(self.min_delay, self.max_delay)
        extra = random.uniform(0.0, 1.25)
        await asyncio.sleep(base + extra)

    def _debug_dump_html(
        self,
        product_code: str,
        city_slug: str,
        requested_url: str,
        final_url: str,
        html: str,
    ) -> None:
        """Dump fetched HTML to disk for debugging purposes (only when LOG_LEVEL=DEBUG)."""
        if not self.logger.isEnabledFor(logging.DEBUG):
            return

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_dir = Path("debug_html")
        out_dir.mkdir(exist_ok=True)

        safe_code = re.sub(r"[^0-9A-Za-z_-]+", "_", str(product_code))
        safe_city = re.sub(r"[^0-9A-Za-z_-]+", "_", str(city_slug))
        path = out_dir / f"{ts}_{safe_code}_{safe_city}.html"

        meta = (
            f"<!-- requested_url: {requested_url} -->\n"
            f"<!-- final_url: {final_url} -->\n"
            f"<!-- length: {len(html)} -->\n"
        )

        path.write_text(meta + html, encoding="utf-8")
        self.logger.debug(f"Saved debug HTML: {path.resolve()}")

    @retry(
        stop=stop_after_attempt(7),
        # Jittered exponential backoff: 5s, 10s, 20s... up to 15 minutes.
        wait=wait_random_exponential(multiplier=5, max=900),
        retry=retry_if_exception_type(
            (RateLimited, TransientHTTPError, httpx.TimeoutException, httpx.NetworkError)
        ),
        reraise=True,
    )
    async def _fetch_html(self, client: httpx.AsyncClient, url: str) -> tuple[str, str]:
        r = await client.get(url, headers=self.headers, follow_redirects=True)
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(f"HTTP GET: {url}")
            self.logger.debug(f"Status: {r.status_code}")
            self.logger.debug(f"Final URL: {str(r.url)}")
        # Аккуратно реагируем на rate-limit/ошибки
        if r.status_code == 429:
            retry_after: Optional[float] = None
            ra = r.headers.get("Retry-After")
            if ra:
                try:
                    retry_after = float(ra)
                except Exception:
                    retry_after = None

            # Если сервер явно просит подождать — подождём здесь, затем дадим tenacity сделать ретрай.
            if retry_after and retry_after > 0:
                await asyncio.sleep(min(retry_after, 900))

            raise RateLimited(retry_after=retry_after)

        # Retry only for transient server errors.
        if r.status_code in (500, 502, 503, 504):
            raise TransientHTTPError(r.status_code)

        # Do not retry on other 4xx (except 429 handled above).
        r.raise_for_status()

        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(f"Final URL: {str(r.url)}")
            self.logger.debug(f"Response length: {len(r.text)}")

        return r.text, str(r.url)

    def _extract_lowprice_from_jsonld(self, soup: BeautifulSoup) -> Optional[float]:
        """Best source: JSON-LD on the page often contains AggregateOffer with lowPrice/highPrice."""
        scripts = soup.find_all("script", attrs={"type": "application/ld+json"})
        for s in scripts:
            raw = (s.string or s.get_text() or "").strip()
            if not raw:
                continue
            # Some pages include multiple JSON objects or invalid JSON; be defensive.
            try:
                data = json.loads(raw)
            except Exception:
                continue

            # JSON-LD may be a dict or a list of dicts.
            nodes = data if isinstance(data, list) else [data]
            for node in nodes:
                if not isinstance(node, dict):
                    continue

                # Common case: node has "offers" with AggregateOffer.
                offers = node.get("offers")
                low = self._jsonld_try_get_lowprice(offers)
                if low is not None:
                    return low

                # Sometimes offers are nested deeper; also check common containers.
                for key in ("@graph", "mainEntity", "itemListElement"):
                    sub = node.get(key)
                    if sub is None:
                        continue
                    low = self._jsonld_scan_any(sub)
                    if low is not None:
                        return low

        return None

    def _jsonld_try_get_lowprice(self, offers_obj) -> Optional[float]:
        if offers_obj is None:
            return None

        # offers can be a dict (AggregateOffer) or a list.
        if isinstance(offers_obj, list):
            for it in offers_obj:
                low = self._jsonld_try_get_lowprice(it)
                if low is not None:
                    return low
            return None

        if isinstance(offers_obj, dict):
            # AggregateOffer case
            if "lowPrice" in offers_obj:
                return self._to_float_price(offers_obj.get("lowPrice"))
            # Offer list can be under "offers" again
            if "offers" in offers_obj:
                return self._jsonld_try_get_lowprice(offers_obj.get("offers"))

        return None

    def _jsonld_scan_any(self, obj) -> Optional[float]:
        """Depth-first scan for offers.lowPrice."""
        if obj is None:
            return None
        if isinstance(obj, dict):
            if "offers" in obj:
                low = self._jsonld_try_get_lowprice(obj.get("offers"))
                if low is not None:
                    return low
            for v in obj.values():
                low = self._jsonld_scan_any(v)
                if low is not None:
                    return low
            return None
        if isinstance(obj, list):
            for it in obj:
                low = self._jsonld_scan_any(it)
                if low is not None:
                    return low
        return None

    def _to_float_price(self, v) -> Optional[float]:
        if v is None:
            return None
        if isinstance(v, (int, float)):
            f = float(v)
            return f if f >= 10.0 else None
        if isinstance(v, str):
            vv = v.strip().replace(" ", "").replace(",", ".")
            try:
                f = float(vv)
                return f if f >= 10.0 else None
            except Exception:
                return None
        return None

    def _extract_prices_best_effort(self, html: str) -> list[float]:
        """Extract offer prices from the delivery-filtered pharmacy page.

        We request `/pharmacy/<city>/filter/delivery=1/`, so the page should already be limited
        to delivery offers. On this page the header text may differ from the product page block
        ("Пропозиції з доставкою"), so we must not rely on that exact title.

        Strategy:
        1) Prefer structured hints: `itemprop=price`, meta/content, `data-price`.
        2) Fallback to scanning visible text near common price containers.
        """
        soup = BeautifulSoup(html, "lxml")

        low_jsonld = self._extract_lowprice_from_jsonld(soup)
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(f"JSON-LD lowPrice (if any): {low_jsonld}")

        prices: list[float] = []
        debug_sources: list[tuple[float, str]] = []

        if low_jsonld is not None and 10.0 <= low_jsonld <= 100000.0:
            prices.append(low_jsonld)
            debug_sources.append((low_jsonld, "jsonld:lowPrice"))

        # 1) Structured: itemprop=price (often used in offer cards)
        for el in soup.select('[itemprop="price"], meta[itemprop="price"]'):
            if el.name == "meta":
                v = el.get("content")
            else:
                v = el.get("content") or el.get_text(" ", strip=True)
            f = self._to_float_price(v)
            if f is not None and 10.0 <= f <= 100000.0:
                prices.append(f)
                debug_sources.append((f, "itemprop=price"))

        # 2) Structured: data-price attributes
        for el in soup.select('[data-price], [data-offer-price], [data-product-price]'):
            v = el.get("data-price") or el.get("data-offer-price") or el.get("data-product-price")
            f = self._to_float_price(v)
            if f is not None and 10.0 <= f <= 100000.0:
                prices.append(f)
                debug_sources.append((f, "data-price"))

        # 3) Visible text: look inside likely price elements, but only inside delivery offer cards
        if not prices:
            likely = soup.select(
                "[class*='delivery'] [class*='price'], "
                "[class*='delivery'] span, "
                "[class*='delivery'] div"
            )
            грн_re = re.compile(r"(\d[\d\s\u00a0]*[.,]?\d{0,2})\s*грн", re.IGNORECASE)
            for el in likely:
                txt = el.get_text(" ", strip=True)
                if not txt or "грн" not in txt.lower():
                    continue
                for raw in грн_re.findall(txt):
                    s = raw.replace("\u00a0", " ").replace(" ", "").replace(",", ".")
                    try:
                        f = float(s)
                        if 10.0 <= f <= 100000.0:
                            prices.append(f)
                            debug_sources.append((f, "text:грн in element"))
                    except Exception:
                        pass
        # --- END of main extraction logic ---
        if self.logger.isEnabledFor(logging.DEBUG):
            if prices:
                uniq = sorted(set(prices))
                self.logger.debug(
                    f"Extracted {len(prices)} candidate prices (unique={len(uniq)}), min={min(prices)}"
                )
                self.logger.debug(f"First candidates: {uniq[:10]}")
                self.logger.debug(f"Candidate prices with sources (first 15): {debug_sources[:15]}")
            else:
                # Helpful diagnostics: show page title and first h1/h2 texts
                title = (soup.title.get_text(strip=True) if soup.title else "")
                h1 = " | ".join([h.get_text(" ", strip=True) for h in soup.find_all("h1")][:2])
                h2 = " | ".join([h.get_text(" ", strip=True) for h in soup.find_all("h2")][:2])
                self.logger.debug(f"No candidate prices found. title='{title}' h1='{h1}' h2='{h2}'")

        return prices
    def _extract_delivery_tab_url(self, html: str) -> Optional[str]:
        soup = BeautifulSoup(html, "lxml")
        a = soup.select_one("a.btn.btn-radio.delivery[href]")
        if not a:
            return None
        href = a.get("href")
        if not href:
            return None
        # normalize odd tails like 'delivery=1;/' -> 'delivery=1/'
        href = href.replace("delivery=1;/", "delivery=1/")
        if href.startswith("http"):
            return href
        return "https://tabletki.ua" + href

    def _final_url_is_delivery(self, final_url: str) -> bool:
        return "delivery=1" in (final_url or "")

    async def get_min_delivery_price(
        self,
        product_code: str,
        city_slug: str,
        product_slug: Optional[str] = None,
        **kwargs,
    ) -> Optional[float]:
        async with httpx.AsyncClient(timeout=self.timeout_sec) as client:
            # Backward/forward compatibility:
            # allow callers to pass slug under different keyword names.
            if product_slug is None:
                product_slug = (
                    kwargs.get("product_slug")
                    or kwargs.get("slug")
                    or kwargs.get("productSlug")
                )
            code = str(product_code).strip().strip("/")
            slug = str(product_slug).strip().strip("/") if product_slug else None

            # New approach: scrape from product card page (no city in URL).
            if slug:
                url = f"https://tabletki.ua/uk/{slug}/{code}/"
            else:
                url = f"https://tabletki.ua/uk/{code}/"

            if self.logger.isEnabledFor(logging.DEBUG):
                self.logger.debug(
                    f"Fetch delivery price for product={code} slug={slug or '-'} url={url}"
                )

            await self.polite_sleep()
            html, final_url = await self._fetch_html(client, url)

            # If we started without slug, try to resolve it and refetch stable URL.
            if not slug:
                resolved = self._extract_slug_from_html_for_code(html, code)
                if resolved:
                    slug = resolved
                else:
                    # Fallback: parse from final URL like /uk/<slug>/<code>/
                    try:
                        p = urlparse(final_url)
                        parts = [x for x in p.path.split("/") if x]
                        if len(parts) >= 3 and parts[0] == "uk" and parts[2] == code:
                            slug = parts[1]
                    except Exception:
                        slug = None

                if slug:
                    stable_url = f"https://tabletki.ua/uk/{slug}/{code}/"
                    if stable_url != url:
                        if self.logger.isEnabledFor(logging.DEBUG):
                            self.logger.debug(f"Refetch stable product URL: {stable_url}")
                        await self.polite_sleep()
                        html, final_url = await self._fetch_html(client, stable_url)
                        url = stable_url

            if self.logger.isEnabledFor(logging.DEBUG):
                # Keep city_slug only for debug filename compatibility
                self._debug_dump_html(
                    product_code=code,
                    city_slug=city_slug,
                    requested_url=url,
                    final_url=final_url,
                    html=html,
                )

        price = self._extract_delivery_price_from_product_page(html)
        if price is None:
            if self.logger.isEnabledFor(logging.DEBUG):
                self.logger.debug(f"No delivery price found for product={code}")
            return None

        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(f"Delivery price for product={code}: {price}")
        return price


    def _extract_delivery_price_from_product_page(self, html: str) -> Optional[float]:
        soup = BeautifulSoup(html, "lxml")

        def parse_price_from_text(txt: str) -> Optional[float]:
            if not txt:
                return None
            m = re.search(r"(\d[\d\s\u00a0]*[.,]?\d{0,2})\s*грн", txt, re.IGNORECASE)
            if not m:
                return None
            raw = m.group(1).replace("\u00a0", " ").replace(" ", "").replace(",", ".")
            try:
                f = float(raw)
            except Exception:
                return None
            if 10.0 <= f <= 100000.0:
                return f
            return None

        # 1) Prefer the dedicated "ДОСТАВКА" button/block (avoid picking "СХОЖІ ТОВАРИ").
        # The UI typically has blocks like:
        #   <a class="button-link-block ...">
        #     <span class="button-link-block__text-title">ДОСТАВКА</span>
        #     <span class="button-link-block__text-details">від 360.00 грн</span>
        #   </a>
        delivery_candidates: list[float] = []

        # Try to locate any parent "button-link-block" containers and inspect their title.
        for container in soup.select("a.button-link-block, div.button-link-block"):
            title_el = container.select_one(".button-link-block__text-title")
            details_el = container.select_one(".button-link-block__text-details")
            if not details_el:
                continue

            title_txt = (title_el.get_text(" ", strip=True) if title_el else container.get_text(" ", strip=True))
            title_norm = (title_txt or "").casefold()

            # Must be delivery-related.
            if "достав" not in title_norm:
                continue

            price = parse_price_from_text(details_el.get_text(" ", strip=True))
            if price is not None:
                delivery_candidates.append(price)

        if delivery_candidates:
            # If multiple delivery blocks exist, take the lowest among delivery-only candidates.
            return min(delivery_candidates)

        # 2) Fallback: some pages may not have clear containers; scan spans but keep only those
        # that are in a block containing the word "доставка" and NOT containing "схож".
        for details_el in soup.select("span.button-link-block__text-details"):
            txt = details_el.get_text(" ", strip=True)
            if not txt or "грн" not in txt.lower():
                continue

            container = details_el
            for _ in range(0, 5):
                if not container:
                    break
                container = container.parent
                if not container:
                    break
                container_txt = container.get_text(" ", strip=True).casefold()
                if "грн" not in container_txt:
                    continue

                if ("достав" in container_txt) and ("схож" not in container_txt) and ("товар" not in container_txt or "схож" not in container_txt):
                    price = parse_price_from_text(txt)
                    if price is not None:
                        return price
                    break

        # 3) Last resort: search for a nearby "доставка" snippet in the full text.
        page_text = soup.get_text(" ", strip=True)
        # Find patterns like: "ДОСТАВКА від 360.00 грн"
        m = re.search(r"достав\w*[^\d]{0,40}(?:від|от)\s*(\d[\d\s\u00a0]*[.,]?\d{0,2})\s*грн", page_text, re.IGNORECASE)
        if m:
            raw = m.group(1).replace("\u00a0", " ").replace(" ", "").replace(",", ".")
            try:
                f = float(raw)
                if 10.0 <= f <= 100000.0:
                    return f
            except Exception:
                return None

        return None