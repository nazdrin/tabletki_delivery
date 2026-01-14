from __future__ import annotations

import asyncio
import logging
import random
import re
from dataclasses import dataclass
from typing import Optional, List, Tuple, Sequence, Dict, Any

import httpx
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_random_exponential, retry_if_exception_type

try:
    # Optional dependency (used on Windows / Cloudflare challenge)
    from .browser_fetcher import BrowserFetcher
except Exception:  # pragma: no cover
    BrowserFetcher = None  # type: ignore


class RateLimited(Exception):
    def __init__(self, retry_after: Optional[float] = None):
        super().__init__("HTTP 429 (rate limited)")
        self.retry_after = retry_after


class TransientHTTPError(Exception):
    def __init__(self, status_code: int):
        super().__init__(f"HTTP {status_code} (transient)")
        self.status_code = status_code


class ForbiddenChallenge(Exception):
    """Raised when we hit HTTP 403 with Cloudflare/challenge-like signals (for fallback to browser)."""
    def __init__(self, status_code: int, headers: Dict[str, str], body_snippet: str = ""):
        super().__init__(f"HTTP {status_code} (forbidden/challenge)")
        self.status_code = status_code
        self.headers = headers
        self.body_snippet = body_snippet


@dataclass
class SellerOffer:
    card_id: str
    seller_name: str
    price: float


class CompetitorsDeliveryScraper:
    """
    Scrapes:
    https://tabletki.ua/uk/{slug}/{code}/pharmacy/kiev/filter/delivery=1/
    and collects first N seller offers (data-price).
    """

    def __init__(
        self,
        timeout_sec: float,
        min_delay: float,
        max_delay: float,
        logger: logging.Logger,
        *,
        use_browser_fetcher: bool = False,
        browser_first: bool = False,
        browser_headless: bool = True,
        browser_timeout_sec: float = 30.0,
        browser_extra_delay_sec: float = 2.0,
    ):
        self.timeout_sec = timeout_sec
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.logger = logger or logging.getLogger(__name__)

        self.use_browser_fetcher = bool(use_browser_fetcher)
        self.browser_first = bool(browser_first)
        self.browser_headless = bool(browser_headless)
        self.browser_timeout_sec = float(browser_timeout_sec)
        self.browser_extra_delay_sec = float(browser_extra_delay_sec)

        self._browser: Optional[Any] = None
        if self.use_browser_fetcher:
            if BrowserFetcher is None:
                raise RuntimeError("BrowserFetcher is enabled but src/browser_fetcher.py is not available.")
            # Reuse a single browser instance across requests (important for speed and stability)
            self._browser = BrowserFetcher(headless=self.browser_headless, timeout_sec=self.browser_timeout_sec)

        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "uk-UA,uk;q=0.9,ru;q=0.8,en;q=0.7",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Connection": "keep-alive",
        }

    def build_url(self, slug: str, code: str, city: str) -> str:
        slug = str(slug).strip().strip("/")
        code = str(code).strip().strip("/")
        city = str(city).strip().strip("/")
        return f"https://tabletki.ua/uk/{slug}/{code}/pharmacy/{city}/filter/delivery=1/"

    async def polite_sleep(self) -> None:
        await asyncio.sleep(random.uniform(self.min_delay, self.max_delay) + random.uniform(0.0, 1.0))

    async def browser_extra_sleep(self) -> None:
        """Extra delay applied before browser-based fetches to reduce pressure and look more human."""
        if self.browser_extra_delay_sec > 0:
            await asyncio.sleep(random.uniform(self.browser_extra_delay_sec, self.browser_extra_delay_sec + 3.0))

    def _looks_like_cloudflare_challenge(self, status_code: int, headers: Dict[str, str], html: str) -> bool:
        if status_code != 403:
            return False
        h = {str(k).lower(): str(v) for k, v in (headers or {}).items()}
        # Cloudflare often marks challenges with these headers
        if "cf-mitigated" in h:
            return True
        server = h.get("server", "")
        if "cloudflare" in server.lower():
            return True
        txt = (html or "").lower()
        # Common challenge markers
        markers = ("cf-chl", "turnstile", "challenge", "/cdn-cgi/", "captcha")
        return any(m in txt for m in markers)

    async def _fetch_html_httpx(self, client: httpx.AsyncClient, url: str) -> str:
        r = await client.get(url, headers=self.headers, follow_redirects=True)
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug("HTTPX response: %s %s", r.status_code, url)

        if r.status_code == 429:
            ra = r.headers.get("Retry-After")
            retry_after: Optional[float] = None
            if ra:
                try:
                    retry_after = float(ra)
                except Exception:
                    retry_after = None
            if retry_after and retry_after > 0:
                await asyncio.sleep(min(retry_after, 900))
            raise RateLimited(retry_after=retry_after)

        if r.status_code in (500, 502, 503, 504):
            raise TransientHTTPError(r.status_code)

        # 403 challenge: do not retry via tenacity forever; allow fallback to browser
        if r.status_code == 403:
            headers = {k: v for k, v in r.headers.items()}
            html = r.text or ""
            if self._looks_like_cloudflare_challenge(403, headers, html):
                snippet = (html[:400] if html else "")
                raise ForbiddenChallenge(status_code=403, headers=headers, body_snippet=snippet)
            # If it's a plain 403 without CF markers, still treat as forbidden
            raise ForbiddenChallenge(status_code=403, headers=headers, body_snippet=(r.text or "")[:200])

        r.raise_for_status()
        return r.text

    @retry(
        stop=stop_after_attempt(7),
        wait=wait_random_exponential(multiplier=5, max=900),
        retry=retry_if_exception_type((RateLimited, TransientHTTPError, httpx.TimeoutException, httpx.NetworkError)),
        reraise=True,
    )
    async def _fetch_html(self, client: httpx.AsyncClient, url: str) -> str:
        return await self._fetch_html_httpx(client, url)

    def _to_float(self, s: Optional[str]) -> Optional[float]:
        if not s:
            return None
        try:
            v = float(s.strip().replace(" ", "").replace(",", "."))
            if 1.0 <= v <= 200000.0:
                return v
        except Exception:
            return None
        return None

    def _norm_seller(self, s: str) -> str:
        """Normalize seller name for robust comparisons."""
        return " ".join((s or "").casefold().split())

    def _is_excluded_seller(self, seller_name: str, excluded_sellers: Sequence[str]) -> bool:
        """
        Return True if seller_name matches any excluded pattern.
        Matching is case-insensitive and ignores extra spaces.
        Patterns can be substrings (recommended) or exact names.
        """
        if not excluded_sellers:
            return False
        name_n = self._norm_seller(seller_name)
        for pat in excluded_sellers:
            pat_n = self._norm_seller(str(pat))
            if not pat_n:
                continue
            if pat_n in name_n:
                return True
        return False

    def _extract_card_id_candidates(self, card) -> List[str]:
        """Collect all plausible card id values from the seller card DOM."""
        cands: List[str] = []

        def add(v: Optional[str]) -> None:
            if not v:
                return
            vv = str(v).strip()
            if vv and vv not in cands:
                cands.append(vv)

        # 1) Outer card attribute
        add(card.get("data-card-id"))

        # 2) Header blocks often keep id on different elements
        hdr = card.select_one(".address-card__header")
        if hdr:
            add(hdr.get("data-id"))
            add(hdr.get("data-card-id"))

        name_div = card.select_one(".address-card__header--name")
        if name_div:
            add(name_div.get("data-id"))
            add(name_div.get("data-card"))

        # 3) Hidden input id like hdnCard_59677
        inp = card.select_one("input[id^='hdnCard_']")
        if inp and inp.get("id"):
            m = re.search(r"hdnCard_(\d+)", inp.get("id"))
            if m:
                add(m.group(1))

        return cands

    def _pick_primary_card_id(self, cands: List[str]) -> str:
        return cands[0] if cands else ""

    def parse_first_offers(
        self,
        html: str,
        limit: int,
        excluded_sellers: Sequence[str] = (),
        excluded_card_ids: Sequence[str] = (),
    ) -> List[SellerOffer]:
        soup = BeautifulSoup(html, "lxml")

        offers: List[SellerOffer] = []
        excluded_ids = {str(x).strip() for x in excluded_card_ids if str(x).strip()}
        cards = soup.select("div.address-card[data-card-id]")
        for card in cards:
            if len(offers) >= limit:
                break

            card_id_candidates = self._extract_card_id_candidates(card)
            card_id = self._pick_primary_card_id(card_id_candidates)

            # Skip excluded seller cards by ANY detected id (most reliable)
            if excluded_ids and any(cid in excluded_ids for cid in card_id_candidates):
                if self.logger.isEnabledFor(logging.DEBUG):
                    self.logger.debug(
                        f"Excluded seller skipped by card_id: candidates={card_id_candidates}"
                    )
                continue

            # Seller name
            name_el = card.select_one(".address-card__header--name span")
            seller_name = name_el.get_text(" ", strip=True) if name_el else ""

            # Skip excluded sellers (e.g., "LikoDar - магазин здорового життя")
            if self._is_excluded_seller(seller_name, excluded_sellers):
                if self.logger.isEnabledFor(logging.DEBUG):
                    self.logger.debug(f"Excluded seller skipped: '{seller_name}' (card_id={card_id})")
                continue

            # Price: prefer data-price on the first delivery button in the card
            btn = card.select_one("button[data-price]")
            price_raw = btn.get("data-price") if btn else None
            price = self._to_float(price_raw)

            # fallback: some cards may have <input ... data-price-min="970.00">
            if price is None:
                inp = card.select_one("input[data-price-min]")
                price = self._to_float(inp.get("data-price-min") if inp else None)

            if price is None:
                continue

            offers.append(SellerOffer(card_id=card_id, seller_name=seller_name, price=price))

        return offers

    async def get_min_price_from_first_sellers(
        self,
        slug: str,
        code: str,
        city: str,
        sellers_limit: int,
        excluded_sellers: Sequence[str] = (),
        excluded_card_ids: Sequence[str] = (),
    ) -> Tuple[Optional[float], List[SellerOffer]]:
        url = self.build_url(slug=slug, code=code, city=city)

        # Visible fetch mode logging (helps to verify browser/http strategy in runtime logs)
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(
                "FETCH strategy: browser_first=%s use_browser=%s headless=%s (timeout=%.1fs extra_delay=%.1fs)",
                self.browser_first,
                self.use_browser_fetcher,
                self.browser_headless,
                self.browser_timeout_sec,
                self.browser_extra_delay_sec,
            )

        # Fetch strategy:
        # - browser_first=True: always fetch via BrowserFetcher (Playwright)
        # - else: try httpx, and if we hit 403 challenge -> fallback to BrowserFetcher (if enabled)
        html: str = ""

        if self.browser_first and self.use_browser_fetcher and self._browser is not None:
            await self.polite_sleep()
            await self.browser_extra_sleep()
            if self.logger.isEnabledFor(logging.DEBUG):
                self.logger.debug("FETCH=BROWSER (browser_first) url=%s", url)
            html = await self._browser.get_html(url)
        else:
            timeout = httpx.Timeout(self.timeout_sec)
            async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
                await self.polite_sleep()
                try:
                    if self.logger.isEnabledFor(logging.DEBUG):
                        self.logger.debug("FETCH=HTTP url=%s", url)
                    html = await self._fetch_html(client, url)
                except ForbiddenChallenge as ex:
                    if self.use_browser_fetcher and self._browser is not None:
                        self.logger.warning(
                            f"HTTP 403 challenge detected for {url}. Falling back to browser fetcher."
                        )
                        # Extra pause before browser fallback to reduce pressure
                        await self.browser_extra_sleep()
                        if self.logger.isEnabledFor(logging.DEBUG):
                            self.logger.debug(
                                "FETCH=BROWSER (fallback after 403) url=%s", url
                            )
                        html = await self._browser.get_html(url)
                    else:
                        # Re-raise so caller can apply cooldown/stop policy
                        raise

        offers = self.parse_first_offers(
            html,
            limit=sellers_limit,
            excluded_sellers=excluded_sellers,
            excluded_card_ids=excluded_card_ids,
        )
        if not offers:
            return None, []

        return min(o.price for o in offers), offers

    async def aclose(self) -> None:
        """Close underlying browser resources (if enabled)."""
        b = getattr(self, "_browser", None)
        if b is None:
            return
        close_fn = getattr(b, "aclose", None) or getattr(b, "close", None)
        if close_fn is None:
            return
        try:
            res = close_fn()
            if asyncio.iscoroutine(res):
                await res
        except Exception:
            # best-effort cleanup
            pass