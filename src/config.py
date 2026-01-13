# src/config.py
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from datetime import time

from dotenv import load_dotenv

__all__ = ["Settings", "load_settings"]



def _parse_list(raw: str) -> list[str]:
    """Parse a list from env allowing separators: comma, semicolon, pipe."""
    raw = (raw or "").strip()
    if not raw:
        return []
    parts = re.split(r"[|;,]+", raw)
    return [x.strip() for x in parts if x and x.strip()]


def _parse_int_set(raw: str) -> set[int]:
    """Parse a set of ints from env allowing separators: comma, semicolon, pipe.

    Be tolerant to inputs like:
      - "59677."
      - "id=59677"
      - "card:59677"
    We extract the first integer-like sequence from each token.
    """
    out: set[int] = set()
    for p in _parse_list(raw):
        p = (p or "").strip()
        if not p:
            continue
        m = re.search(r"(\d+)", p)
        if not m:
            continue
        try:
            out.add(int(m.group(1)))
        except ValueError:
            continue
    return out


def _parse_time_hhmm(s: str, default: time) -> time:
    s = (s or "").strip()
    if not s:
        return default
    try:
        parts = s.split(":")
        hh = int(parts[0])
        mm = int(parts[1]) if len(parts) > 1 else 0
        return time(hour=hh, minute=mm)
    except Exception:
        return default


@dataclass(frozen=True)
class Settings:
    # Google Drive
    google_credentials_path: str
    source_folder_id: str
    target_folder_id: str
    input_filename: str

    # Parser (night)
    max_items: int
    cities: list[str]
    min_delay_sec: float
    max_delay_sec: float
    http_timeout_sec: float

    # Telegram
    telegram_bot_token: Optional[str]
    telegram_chat_ids: list[int]

    # ---------------------------
    # DAY competitors parser (new)
    # ---------------------------

    # enable/disable day parser
    competitors_enabled: bool

    # work window (local time)
    competitors_window_start: time          # default 09:00
    competitors_window_end: time            # default 21:00

    # processing strategy
    competitors_total_items: int            # default 300
    competitors_max_items: int              # alias for competitors_total_items (compat)
    competitors_batch_size: int             # default 100
    competitors_items_per_hour: int         # default 100
    competitors_cycle_hours: int            # default 3   (100/час * 3 часа = 300 товаров за цикл)
    competitors_runs_per_day: int           # default 4  (4 прогона по 3 часа)
    competitors_offers_per_product: int     # default 7  (5-7 предложений)
    competitors_sellers_limit: int          # alias for competitors_offers_per_product (compat)

    # URL building
    competitors_city: str                   # default "kiev"

    # output
    competitors_output_filename: str        # default "competitors_delivery_total.json"
    # DAY competitors parser (new)
    competitors_excluded_sellers: list[str]     # default [] (patterns, case-insensitive)
    competitors_excluded_card_ids: set[int]      # default set() (address-card id / data-card)

    # Browser fallback / browser-first mode (for Cloudflare/403, especially on Windows)
    competitors_use_browser_fetcher: bool        # default True
    competitors_browser_first: bool              # default False
    competitors_browser_headless: bool           # default True
    competitors_browser_timeout_sec: float       # default 45
    competitors_browser_extra_delay_sec: float   # default 2.0


def load_settings(env_path: str = ".env") -> Settings:
    load_dotenv(env_path)

    google_credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "credentials.json")
    google_credentials_path = str(Path(google_credentials_path).expanduser().resolve())

    source_folder_id = os.getenv("SOURCE_FOLDER_ID")
    if not source_folder_id:
        raise RuntimeError("Missing required env var: SOURCE_FOLDER_ID")

    target_folder_id = os.getenv("TARGET_FOLDER_ID")
    if not target_folder_id:
        raise RuntimeError("Missing required env var: TARGET_FOLDER_ID")

    input_filename = os.getenv("INPUT_FILENAME", "goods.xlsx")

    max_items = int(os.getenv("MAX_ITEMS", "100"))

    cities = _parse_list(os.getenv("CITIES", "kiev"))

    min_delay_sec = float(os.getenv("MIN_DELAY_SEC", "2.0"))
    max_delay_sec = float(os.getenv("MAX_DELAY_SEC", "5.5"))
    http_timeout_sec = float(os.getenv("HTTP_TIMEOUT_SEC", "25"))

    telegram_bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_ids_raw = os.getenv("TELEGRAM_CHAT_IDS", "")
    telegram_chat_ids: list[int] = []
    for part in _parse_list(chat_ids_raw):
        try:
            telegram_chat_ids.append(int(part))
        except ValueError:
            continue

    # ---------------------------
    # DAY competitors parser (new)
    # ---------------------------
    competitors_enabled = os.getenv("COMPETITORS_ENABLED", "1").strip().lower() in ("1", "true", "yes", "y", "on")

    competitors_window_start = _parse_time_hhmm(
        os.getenv("COMPETITORS_WINDOW_START", "09:00"),
        default=time(9, 0),
    )
    competitors_window_end = _parse_time_hhmm(
        os.getenv("COMPETITORS_WINDOW_END", "21:00"),
        default=time(21, 0),
    )

    competitors_total_items = int(os.getenv("COMPETITORS_TOTAL_ITEMS", "300"))
    competitors_max_items = int(os.getenv("COMPETITORS_MAX_ITEMS", str(competitors_total_items)))
    competitors_batch_size = int(os.getenv("COMPETITORS_BATCH_SIZE", "100"))
    competitors_items_per_hour = int(os.getenv("COMPETITORS_ITEMS_PER_HOUR", "100"))
    competitors_cycle_hours = int(os.getenv("COMPETITORS_CYCLE_HOURS", "3"))
    competitors_runs_per_day = int(os.getenv("COMPETITORS_RUNS_PER_DAY", "4"))
    competitors_offers_per_product = int(os.getenv("COMPETITORS_OFFERS_PER_PRODUCT", "7"))
    competitors_sellers_limit = int(
        os.getenv("COMPETITORS_SELLERS_LIMIT", str(competitors_offers_per_product))
    )

    competitors_city = os.getenv("COMPETITORS_CITY", "kiev").strip() or "kiev"
    competitors_output_filename = os.getenv("COMPETITORS_OUTPUT_FILENAME", "competitors_delivery_total.json").strip() or "competitors_delivery_total.json"
    competitors_excluded_sellers = _parse_list(os.getenv("COMPETITORS_EXCLUDED_SELLERS", ""))
    competitors_excluded_card_ids = _parse_int_set(os.getenv("COMPETITORS_EXCLUDED_CARD_IDS", ""))

    competitors_use_browser_fetcher = os.getenv("COMPETITORS_USE_BROWSER_FETCHER", "1").strip().lower() in (
        "1", "true", "yes", "y", "on"
    )
    competitors_browser_first = os.getenv("COMPETITORS_BROWSER_FIRST", "0").strip().lower() in (
        "1", "true", "yes", "y", "on"
    )
    competitors_browser_headless = os.getenv("COMPETITORS_BROWSER_HEADLESS", "1").strip().lower() in (
        "1", "true", "yes", "y", "on"
    )
    competitors_browser_timeout_sec = float(os.getenv("COMPETITORS_BROWSER_TIMEOUT_SEC", "45"))
    competitors_browser_extra_delay_sec = float(os.getenv("COMPETITORS_BROWSER_EXTRA_DELAY_SEC", "2.0"))

    return Settings(
        google_credentials_path=google_credentials_path,
        source_folder_id=source_folder_id,
        target_folder_id=target_folder_id,
        input_filename=input_filename,
        max_items=max_items,
        cities=cities,
        min_delay_sec=min_delay_sec,
        max_delay_sec=max_delay_sec,
        http_timeout_sec=http_timeout_sec,
        telegram_bot_token=telegram_bot_token,
        telegram_chat_ids=telegram_chat_ids,

        competitors_enabled=competitors_enabled,
        competitors_window_start=competitors_window_start,
        competitors_window_end=competitors_window_end,
        competitors_total_items=competitors_total_items,
        competitors_batch_size=competitors_batch_size,
        competitors_items_per_hour=competitors_items_per_hour,
        competitors_cycle_hours=competitors_cycle_hours,
        competitors_runs_per_day=competitors_runs_per_day,
        competitors_offers_per_product=competitors_offers_per_product,
        competitors_city=competitors_city,
        competitors_output_filename=competitors_output_filename,
        competitors_max_items=competitors_max_items,
        competitors_sellers_limit=competitors_sellers_limit,
        competitors_excluded_sellers=competitors_excluded_sellers,
        competitors_excluded_card_ids=competitors_excluded_card_ids,

        competitors_use_browser_fetcher=competitors_use_browser_fetcher,
        competitors_browser_first=competitors_browser_first,
        competitors_browser_headless=competitors_browser_headless,
        competitors_browser_timeout_sec=competitors_browser_timeout_sec,
        competitors_browser_extra_delay_sec=competitors_browser_extra_delay_sec,
    )