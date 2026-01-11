from __future__ import annotations

import asyncio
import logging
import random
import re
from dataclasses import dataclass
from typing import Optional, List, Tuple, Sequence

import httpx
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_random_exponential, retry_if_exception_type


class RateLimited(Exception):
    def __init__(self, retry_after: Optional[float] = None):
        super().__init__("HTTP 429 (rate limited)")
        self.retry_after = retry_after


class TransientHTTPError(Exception):
    def __init__(self, status_code: int):
        super().__init__(f"HTTP {status_code} (transient)")
        self.status_code = status_code


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

    def __init__(self, timeout_sec: float, min_delay: float, max_delay: float, logger: logging.Logger):
        self.timeout_sec = timeout_sec
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.logger = logger or logging.getLogger(__name__)

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

    @retry(
        stop=stop_after_attempt(7),
        wait=wait_random_exponential(multiplier=5, max=900),
        retry=retry_if_exception_type((RateLimited, TransientHTTPError, httpx.TimeoutException, httpx.NetworkError)),
        reraise=True,
    )
    async def _fetch_html(self, client: httpx.AsyncClient, url: str) -> str:
        r = await client.get(url, headers=self.headers, follow_redirects=True)

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

        r.raise_for_status()
        return r.text

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

        timeout = httpx.Timeout(self.timeout_sec)
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            await self.polite_sleep()
            html = await self._fetch_html(client, url)

        offers = self.parse_first_offers(
            html,
            limit=sellers_limit,
            excluded_sellers=excluded_sellers,
            excluded_card_ids=excluded_card_ids,
        )
        if not offers:
            return None, []

        return min(o.price for o in offers), offers