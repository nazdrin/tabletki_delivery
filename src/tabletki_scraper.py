from __future__ import annotations
import random
import re
import asyncio
import logging
from typing import Optional
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_random_exponential, retry_if_exception_type




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


    async def polite_sleep(self):
        # base delay from settings + small extra jitter to avoid rigid patterns
        base = random.uniform(self.min_delay, self.max_delay)
        extra = random.uniform(0.0, 1.25)
        await asyncio.sleep(base + extra)

    def _debug_dump_html(
        self,
        product_code: str,
        requested_url: str,
        final_url: str,
        html: str,
        product_slug: Optional[str] = None,
    ) -> None:
        """Dump fetched HTML to disk for debugging purposes (only when LOG_LEVEL=DEBUG)."""
        if not self.logger.isEnabledFor(logging.DEBUG):
            return

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_dir = Path("debug_html")
        out_dir.mkdir(exist_ok=True)

        safe_code = re.sub(r"[^0-9A-Za-z_-]+", "_", str(product_code))
        safe_slug = re.sub(r"[^0-9A-Za-z_-]+", "_", str(product_slug)) if product_slug else None
        if safe_slug:
            filename = f"{ts}_{safe_slug}_{safe_code}.html"
        else:
            filename = f"{ts}_{safe_code}.html"
        path = out_dir / filename

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


    async def get_min_delivery_price(
        self,
        product_code: str,
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

            # Scrape from product card page (no city in URL).
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
                self._debug_dump_html(
                    product_code=code,
                    requested_url=url,
                    final_url=final_url,
                    html=html,
                    product_slug=slug,
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