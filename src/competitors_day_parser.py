from __future__ import annotations

import json
import asyncio
import random
import re
from dataclasses import dataclass
from datetime import datetime, time
from typing import Dict, List, Optional, Tuple

from .competitors_scraper import CompetitorsDeliveryScraper, RateLimited
from .excel_io import read_input_rows, build_output_json
from .gdrive_client import GoogleDriveClient
from .telegram_notifier import TelegramNotifier


@dataclass
class DayPlan:
    enabled: bool
    cycle_index: int
    hour_in_cycle: int
    start_idx: int
    end_idx: int


def compute_day_plan(now: datetime, window_start: time, window_end: time, items_per_hour: int, cycle_hours: int) -> DayPlan:
    """
    window: [09:00 .. 21:00) => hours 9..20 (12 runs)
    cycle_hours=3 => 0..2 => chunks: 0-99, 100-199, 200-299
    cycle_index = hour_offset // 3 (0..3) => repeats 4 times/day
    """
    t = now.time()
    if not (window_start <= t < window_end):
        return DayPlan(enabled=False, cycle_index=0, hour_in_cycle=0, start_idx=0, end_idx=0)

    hour_offset = now.hour - window_start.hour  # 0..11 (for 09..20)
    cycle_index = hour_offset // cycle_hours    # 0..3
    hour_in_cycle = hour_offset % cycle_hours   # 0..2

    start_idx = hour_in_cycle * items_per_hour
    end_idx = start_idx + items_per_hour
    return DayPlan(enabled=True, cycle_index=cycle_index, hour_in_cycle=hour_in_cycle, start_idx=start_idx, end_idx=end_idx)


def normalize_code(v: object) -> str:
    """Normalize product codes coming from Excel/JSON.

    Excel иногда отдаёт код как float (например 1084018.0).
    Приводим такие значения к строке без дробной части, если она нулевая.
    """
    if v is None:
        return ""

    # Fast-path for numeric types
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        if v.is_integer():
            return str(int(v))
        return str(v).strip()

    s = str(v).strip()
    if not s:
        return ""

    # "1084018.0", "1084018.00", "1084018.000000" -> "1084018"
    m = re.fullmatch(r"(\d+)\.0+", s)
    if m:
        return m.group(1)

    return s


def update_competitors_json(existing_bytes: Optional[bytes], target_cities: List[str], updates: Dict[str, float]) -> bytes:
    """Update competitors JSON with new prices.

    Rules (for *updated* product codes only):
    - Write/update prices for ALL `target_cities` (e.g. Kyiv, Lviv, ...).
    - If a (code, city) record exists -> overwrite `delivery_price`.
    - If it does not exist -> create it.
    - Remove any records for the updated code where city is NOT in `target_cities`.
    - Remove duplicates so the result contains exactly 1 record per (code, city).

    City matching is case-insensitive; city casing in output follows `target_cities`.
    """

    # Load existing
    data: List[dict]
    if existing_bytes:
        try:
            loaded = json.loads(existing_bytes.decode("utf-8"))
            data = loaded if isinstance(loaded, list) else []
        except Exception:
            data = []
    else:
        data = []

    # Normalize target cities
    target_city_list: List[str] = [str(c).strip() for c in target_cities if str(c).strip()]
    target_city_cf_to_canon: Dict[str, str] = {c.casefold(): c for c in target_city_list}

    # Normalize update codes
    updates_norm: Dict[str, float] = {}
    for code_raw, price in updates.items():
        c = normalize_code(code_raw)
        if not c:
            continue
        updates_norm[c] = float(price)

    updated_codes = set(updates_norm.keys())
    if not updated_codes:
        return build_output_json(data)

    # 1) For updated codes: keep only target cities, de-duplicate by (code, city_cf),
    #    and also normalize city casing to canonical.
    cleaned: List[dict] = []
    seen_keys: set[Tuple[str, str]] = set()

    for rec in data:
        code = normalize_code(rec.get("code", ""))
        if not code:
            # keep weird/empty records as-is
            cleaned.append(rec)
            continue

        city_raw = str(rec.get("city", "")).strip()
        city_cf = city_raw.casefold() if city_raw else ""

        if code in updated_codes:
            # Drop non-target cities for updated codes
            if city_cf not in target_city_cf_to_canon:
                continue

            # Canonicalize casing
            canon_city = target_city_cf_to_canon[city_cf]
            key = (code, canon_city.casefold())

            # Drop duplicates
            if key in seen_keys:
                continue
            seen_keys.add(key)

            rec["code"] = code
            rec["city"] = canon_city
            # delivery_price will be overwritten later
            cleaned.append(rec)
        else:
            # For non-updated codes: keep as-is (but normalize code for consistency)
            rec["code"] = code
            cleaned.append(rec)

    data = cleaned

    # Rebuild index for fast updates after cleanup
    idx_map: Dict[Tuple[str, str], int] = {}
    for i, rec in enumerate(data):
        code = normalize_code(rec.get("code", ""))
        city = str(rec.get("city", "")).strip()
        if code and city:
            idx_map[(code, city.casefold())] = i

    # 2) Apply updates: ensure exactly one record per target city
    for code, price in updates_norm.items():
        for canon_city in target_city_list:
            key = (code, canon_city.casefold())
            if key in idx_map:
                rec = data[idx_map[key]]
                rec["delivery_price"] = float(price)
                rec["code"] = code
                rec["city"] = canon_city
            else:
                data.append({"code": code, "city": canon_city, "delivery_price": float(price)})
                idx_map[key] = len(data) - 1

    # 3) Final safety: ensure no duplicates remain for updated codes
    final: List[dict] = []
    seen_final: set[Tuple[str, str]] = set()

    for rec in data:
        code = normalize_code(rec.get("code", ""))
        city = str(rec.get("city", "")).strip()
        city_cf = city.casefold() if city else ""

        if code in updated_codes and city_cf in target_city_cf_to_canon:
            canon_city = target_city_cf_to_canon[city_cf]
            key = (code, canon_city.casefold())
            if key in seen_final:
                continue
            seen_final.add(key)
            rec["code"] = code
            rec["city"] = canon_city

        final.append(rec)

    return build_output_json(final)


async def run_day_competitors_parser(settings, logger) -> None:
    """
    settings must provide:
      - google_credentials_path
      - source_folder_id
      - input_filename (goods)
      - target_folder_id
      - telegram_*
      - http_timeout_sec, min_delay_sec, max_delay_sec
      - competitors_city
      - competitors_offers_per_product
      - competitors_output_filename
      - competitors_items_per_hour
      - competitors_cycle_hours
      - competitors_window_start, competitors_window_end (time)
      - competitors_max_items (e.g. 300)
      - competitors_excluded_sellers: list of seller name patterns to skip
      - competitors_browser_profile_dir (optional, path to persistent browser profile dir for Playwright)
    """
    plan = compute_day_plan(
        now=datetime.now(),
        window_start=settings.competitors_window_start,
        window_end=settings.competitors_window_end,
        items_per_hour=settings.competitors_items_per_hour,
        cycle_hours=settings.competitors_cycle_hours,
    )

    if not plan.enabled:
        logger.info("Day competitors parser: outside working window. Exit.")
        return

    notifier = TelegramNotifier(
        bot_token=settings.telegram_bot_token,
        chat_ids=settings.telegram_chat_ids,
        timeout_sec=15.0,
    )

    drive = GoogleDriveClient(settings.google_credentials_path)

    # goods input (same style as night parser)
    f = drive.find_file_in_folder(settings.source_folder_id, settings.input_filename)
    if not f:
        f = drive.find_latest_xlsx_in_folder(settings.source_folder_id)
    if not f:
        msg = "ERROR: No goods .xlsx file found in SOURCE_FOLDER_ID"
        logger.error(msg)
        await notifier.send(msg)
        return

    xlsx_bytes = drive.download_file_bytes(f.id)
    rows = read_input_rows(xlsx_bytes)
    if not rows:
        msg = "ERROR: Goods file has no valid rows"
        logger.error(msg)
        await notifier.send(msg)
        return

    rows.sort(key=lambda r: r.numerical_order)
    rows = rows[: settings.competitors_total_items]

    # take chunk for this hour
    chunk = rows[plan.start_idx : min(plan.end_idx, len(rows))]
    logger.info(
        f"Day competitors run: cycle={plan.cycle_index} hour_in_cycle={plan.hour_in_cycle} "
        f"items={len(chunk)} range={plan.start_idx+1}-{min(plan.end_idx, len(rows))} of {len(rows)}"
    )

    scraper = CompetitorsDeliveryScraper(
        timeout_sec=settings.http_timeout_sec,
        min_delay=settings.min_delay_sec,
        max_delay=settings.max_delay_sec,
        logger=logger,
        use_browser_fetcher=settings.competitors_use_browser_fetcher,
        browser_first=settings.competitors_browser_first,
        browser_headless=settings.competitors_browser_headless,
        browser_timeout_sec=settings.competitors_browser_timeout_sec,
        browser_extra_delay_sec=settings.competitors_browser_extra_delay_sec,
        browser_profile_dir=getattr(settings, "competitors_browser_profile_dir", None),
    )

    updates: Dict[str, float] = {}

    # `competitors_city` is used ONLY for URL building (slug like "kiev").
    city = settings.competitors_city
    limit = settings.competitors_offers_per_product

    # Cities to WRITE into competitors_delivery_total.json.
    # Some configs may normalize CITIES to slugs; if you have a dedicated list with display names
    # (e.g. cities_display/cities_output/cities_raw), prefer it.
    output_cities: List[str] = (
        getattr(settings, "cities_output", None)
        or getattr(settings, "cities_display", None)
        or getattr(settings, "cities_raw", None)
        or list(getattr(settings, "cities", []) or [])
    )

    excluded_sellers: List[str] = getattr(settings, "competitors_excluded_sellers", []) or []
    if excluded_sellers:
        logger.info(f"Excluded sellers patterns: {excluded_sellers}")

    logger.info(
        f"Scraping city slug for URL building: {city}. Writing updates for cities: {', '.join(list(output_cities))}"
    )

    errors = 0

    # --- Anti-ban pacing / protection ---
    # Even if scraper itself sleeps, we add an extra conservative layer here.
    # Goal: ~100 products/hour -> ~36s per product on average (including network).
    # We keep it variable to avoid rigid patterns.
    consecutive_429 = 0
    consecutive_403 = 0

    # Extra per-item delay (seconds). This is in addition to scraper polite_sleep.
    per_item_extra_delay_range = (12.0, 22.0)

    # Occasional batch pauses to break the pattern.
    next_micro_pause_at = random.randint(8, 14)   # 25–45s
    next_small_pause_at = random.randint(18, 26)  # 60–120s
    next_big_pause_at = random.randint(45, 60)    # 5–10m

    async def _sleep_jitter(a: float, b: float) -> None:
        await asyncio.sleep(random.uniform(a, b))

    async def _batch_pause(kind: str, sec_min: float, sec_max: float) -> None:
        pause = random.uniform(sec_min, sec_max)
        logger.info(f"Batch pause ({kind}): sleeping {pause:.1f}s")
        await asyncio.sleep(pause)

    async def _cooldown_after_429() -> None:
        # long cooldown to escape rate-limit windows
        nonlocal consecutive_429
        consecutive_429 += 1
        cd = min(random.uniform(600.0, 1200.0) + random.uniform(120.0, 360.0) * max(0, consecutive_429 - 1), 3600.0)
        logger.warning(f"HTTP 429 cooldown: {cd/60:.1f} min (consecutive_429={consecutive_429})")
        await asyncio.sleep(cd)

    async def _cooldown_after_403() -> None:
        # 403 often means bot protection. Slow down a lot.
        nonlocal consecutive_403
        consecutive_403 += 1
        if consecutive_403 <= 1:
            cd = random.uniform(45.0, 90.0)
        else:
            cd = random.uniform(180.0, 420.0)  # 3–7 min
        logger.warning(f"HTTP 403 cooldown: {cd:.1f}s (consecutive_403={consecutive_403})")
        await asyncio.sleep(cd)

    for i, r in enumerate(chunk, start=1):
        code = normalize_code(r.product_code)
        slug = str(r.slug).strip() if r.slug is not None else ""

        # Extra jitter BEFORE request to avoid perfectly periodic requests.
        await _sleep_jitter(*per_item_extra_delay_range)

        # Occasional batch pauses (at most one per iteration)
        if i >= next_big_pause_at:
            await _batch_pause("big", 300.0, 600.0)  # 5–10 minutes
            next_big_pause_at = i + random.randint(45, 60)
            # keep smaller pauses ahead
            if next_small_pause_at <= i:
                next_small_pause_at = i + random.randint(18, 26)
            if next_micro_pause_at <= i:
                next_micro_pause_at = i + random.randint(8, 14)
        elif i >= next_small_pause_at:
            await _batch_pause("small", 60.0, 120.0)  # 1–2 minutes
            next_small_pause_at = i + random.randint(18, 26)
            if next_micro_pause_at <= i:
                next_micro_pause_at = i + random.randint(8, 14)
        elif i >= next_micro_pause_at:
            await _batch_pause("micro", 25.0, 45.0)  # 25–45 seconds
            next_micro_pause_at = i + random.randint(8, 14)

        tries = 0
        max_tries = 2  # one retry after cooldown

        while True:
            try:
                min_price, offers = await scraper.get_min_price_from_first_sellers(
                    slug=slug,
                    code=code,
                    city=city,
                    sellers_limit=limit,
                    excluded_sellers=excluded_sellers,
                )

                # Success resets counters.
                consecutive_429 = 0
                consecutive_403 = 0

                if min_price is None:
                    logger.warning(f"[{i}/{len(chunk)}] {code}: no offers parsed")
                    break

                updates[code] = float(min_price)
                logger.info(
                    f"[{i}/{len(chunk)}] {code}: min among first {limit} (excluding blacklisted sellers) = {min_price} "
                    f"(offers_used={len(offers)})"
                )
                break

            except RateLimited as e:
                errors += 1
                logger.error(f"[{i}/{len(chunk)}] {code}: ERROR RateLimited: {e}")
                await _cooldown_after_429()
                tries += 1
                if tries <= max_tries:
                    logger.warning(f"[{i}/{len(chunk)}] {code}: retry after 429 cooldown (try {tries}/{max_tries})")
                    continue
                logger.error(f"[{i}/{len(chunk)}] {code}: giving up after repeated 429")
                break

            except Exception as e:
                # Handle 403s even if scraper raises generic exception.
                msg = str(e)
                is_403 = " 403" in msg or "HTTP 403" in msg or "403 Forbidden" in msg
                is_429 = " 429" in msg or "HTTP 429" in msg

                errors += 1

                if is_429:
                    logger.error(f"[{i}/{len(chunk)}] {code}: ERROR 429-like: {type(e).__name__}: {e}")
                    await _cooldown_after_429()
                    tries += 1
                    if tries <= max_tries:
                        logger.warning(f"[{i}/{len(chunk)}] {code}: retry after 429-like cooldown (try {tries}/{max_tries})")
                        continue
                    logger.error(f"[{i}/{len(chunk)}] {code}: giving up after repeated 429-like")
                    break

                if is_403:
                    logger.error(f"[{i}/{len(chunk)}] {code}: ERROR 403-like: {type(e).__name__}: {e}")
                    await _cooldown_after_403()
                    tries += 1
                    if tries <= max_tries:
                        logger.warning(f"[{i}/{len(chunk)}] {code}: retry after 403 cooldown (try {tries}/{max_tries})")
                        continue
                    logger.error(f"[{i}/{len(chunk)}] {code}: giving up after repeated 403-like")
                    break

                logger.exception(f"[{i}/{len(chunk)}] {code}: ERROR {type(e).__name__}: {e}")
                break

    # download existing competitors file (if exists) and update
    out_name = settings.competitors_output_filename
    existing = drive.find_file_in_folder(settings.target_folder_id, out_name)
    existing_bytes = drive.download_file_bytes(existing.id) if existing else None

    out_bytes = update_competitors_json(existing_bytes, target_cities=list(output_cities), updates=updates)

    # IMPORTANT: upsert -> update existing file in Drive, not create new
    file_id = drive.upsert_json_bytes(settings.target_folder_id, out_name, out_bytes)

    await notifier.send(
        "✅ Day competitors run done\n"
        f"Window: {settings.competitors_window_start.strftime('%H:%M')}-{settings.competitors_window_end.strftime('%H:%M')}\n"
        f"Cycle: {plan.cycle_index+1}/4, hour_in_cycle: {plan.hour_in_cycle+1}/3\n"
        f"Chunk: {plan.start_idx+1}-{min(plan.end_idx, len(rows))} of {len(rows)}\n"
        f"Updated items: {len(updates)}\n"
        f"Errors: {errors}\n"
        f"Excluded seller patterns: {len(excluded_sellers)}\n"
        f"Output: {out_name}\n"
        f"File ID: {file_id}"
    )