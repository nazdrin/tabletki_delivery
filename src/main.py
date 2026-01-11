from __future__ import annotations

import asyncio
import random
from datetime import datetime, time, timedelta
from pathlib import Path

import httpx

from .config import load_settings
from .logger import setup_logger
from .gdrive_client import GoogleDriveClient
from .excel_io import read_input_rows, build_output_json
from .tabletki_scraper import TabletkiScraper
from .telegram_notifier import TelegramNotifier


async def run():
    settings = load_settings()
    logger = setup_logger()

    if not settings.cities:
        msg = "ERROR: CITIES is empty. Set CITIES in .env, e.g. CITIES=kyiv or CITIES=kyiv,lviv"
        logger.error(msg)
        return

    notifier = TelegramNotifier(
        bot_token=settings.telegram_bot_token,
        chat_ids=settings.telegram_chat_ids,
        timeout_sec=15.0,
    )

    logger.info("Starting parser...")

    drive = GoogleDriveClient(settings.google_credentials_path)

    # 1) входной файл: либо по имени, либо самый свежий xlsx
    f = drive.find_file_in_folder(settings.source_folder_id, settings.input_filename)
    if not f:
        f = drive.find_latest_xlsx_in_folder(settings.source_folder_id)
    if not f:
        msg = "ERROR: No input .xlsx file found in SOURCE_FOLDER_ID"
        logger.error(msg)
        await notifier.send(msg)
        return

    logger.info(f"Input file: {f.name} (modified {f.modified_time})")

    xlsx_bytes = drive.download_file_bytes(f.id)
    rows = read_input_rows(xlsx_bytes)
    if not rows:
        msg = "ERROR: Input file has no valid rows."
        logger.error(msg)
        await notifier.send(msg)
        return

    rows.sort(key=lambda r: r.numerical_order)
    rows = rows[: settings.max_items]

    logger.info(f"Will process {len(rows)} items (MAX_ITEMS={settings.max_items})")

    scraper = TabletkiScraper(
        timeout_sec=settings.http_timeout_sec,
        min_delay=settings.min_delay_sec,
        max_delay=settings.max_delay_sec,
        logger=logger,
    )

    # --- Night window strategy ---
    NIGHT_START = time(22, 0)
    NIGHT_END = time(8, 0)
    PRENIGHT_START = time(20, 0)  # 20:00–22:00: allow waiting until night instead of stopping

    def is_night_now() -> bool:
        """Night window is 22:00–08:00 (crosses midnight)."""
        now_t = datetime.now().time()
        return now_t >= NIGHT_START or now_t < NIGHT_END

    def is_day_window_now() -> bool:
        """Day window is 08:00–22:00."""
        now_t = datetime.now().time()
        return NIGHT_END <= now_t < NIGHT_START

    def is_prenight_window_now() -> bool:
        """Pre-night window is 20:00–22:00."""
        now_t = datetime.now().time()
        return PRENIGHT_START <= now_t < NIGHT_START

    entered_night_window = is_night_now()

    async def warmup_visit(reason: str) -> bool:
        """
        Best-effort warmup. IMPORTANT:
        - warmup may be 403 while product pages still sometimes return 200
        - do not treat warmup failures as a hard stop condition
        Returns True if at least one warmup URL returns < 400, else False.
        """
        bust = str(random.randint(100000, 999999))

        warmup_urls = [
            f"https://tabletki.ua/robots.txt?b={bust}",
            f"https://tabletki.ua/favicon.ico?b={bust}",
            f"https://tabletki.ua/uk/search/?q=%D0%B2%D1%96%D1%82%D0%B0%D0%BC%D1%96%D0%BD&b={bust}",
            f"https://tabletki.ua/uk/?b={bust}",
        ]

        ua_pool = [
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
        ]

        headers = {
            "User-Agent": random.choice(ua_pool),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "uk-UA,uk;q=0.9,ru-UA;q=0.8,ru;q=0.7,en-US;q=0.6,en;q=0.5",
            "Connection": "keep-alive",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }

        timeout = httpx.Timeout(settings.http_timeout_sec)
        async with httpx.AsyncClient(timeout=timeout, headers=headers, follow_redirects=True) as c:
            any_ok = False
            for url in warmup_urls:
                try:
                    # Prefer HEAD where possible, fall back to GET.
                    try:
                        r = await c.head(url)
                        if r.status_code in (405, 501):
                            r = await c.get(url)
                    except Exception:
                        r = await c.get(url)

                    logger.info(f"Warmup visit ({reason}): {url} -> {r.status_code}")
                    if r.status_code < 400:
                        any_ok = True
                except Exception as ex:
                    logger.debug(f"Warmup visit failed ({reason}) for {url}: {ex}")

            return any_ok

    def suggest_restart_dt(now: datetime) -> datetime:
        t = now.time()

        def ceil_to_half_hour(dt: datetime) -> datetime:
            m = dt.minute
            if m <= 30:
                return dt.replace(minute=30, second=0, microsecond=0)
            return (dt + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)

        # Day window → wait for deep night
        if NIGHT_END <= t < NIGHT_START:
            return now.replace(hour=22, minute=30, second=0, microsecond=0)

        # 22:00–01:29 → suggest 01:30
        if t >= NIGHT_START or t < time(1, 30):
            if t >= NIGHT_START:
                return (now + timedelta(days=1)).replace(hour=1, minute=30, second=0, microsecond=0)
            return now.replace(hour=1, minute=30, second=0, microsecond=0)

        # 01:30–06:29 → now + 2h (rounded)
        if time(1, 30) <= t < time(6, 30):
            return ceil_to_half_hour(now + timedelta(hours=2))

        # 06:30–07:59 → next night
        return (now + timedelta(days=1)).replace(hour=22, minute=30, second=0, microsecond=0)

    # JSON output records: one record per (product_code, city) where price is found.
    out_records: list[dict] = []
    def _save_output_locally(filename: str, data: bytes) -> str:
        """
        Always save result locally first so we never lose a successful parsing run
        if Google Drive upload fails due to transient SSL/network issues.
        Returns absolute path to the saved file.
        """
        out_dir = Path("./out")
        out_dir.mkdir(parents=True, exist_ok=True)
        path = out_dir / filename
        path.write_bytes(data)
        return str(path.resolve())
    errors = 0
    consecutive_429 = 0
    stopped_early_reason: str | None = None
    suggested_restart: datetime | None = None
    consecutive_403 = 0
    # --- 403 analytics ---
    total_403 = 0
    window_403_count = 0
    window_start_idx = 1
    window_end_idx = 100
    per100_403: dict[str, int] = {}

    def _update_403_window(current_idx: int) -> None:
        nonlocal window_403_count, window_start_idx, window_end_idx
        while current_idx > window_end_idx:
            key = f"{window_start_idx}-{window_end_idx}"
            per100_403[key] = window_403_count
            window_start_idx += 100
            window_end_idx += 100
            window_403_count = 0

    def _current_403_per_100(current_idx: int) -> int:
        _update_403_window(current_idx)
        return window_403_count
    # Cooldown policy for HTTP 403 (Forbidden)
    pause_403_once_range_sec = (30.0, 60.0)    # after any single 403: 30–60 seconds

    # --- Unpredictable batch pauses (to avoid fixed patterns) ---
    # We schedule three kinds of pauses:
    #  - small: roughly every 45–60 items (20–30s)
    #  - medium: roughly every 90–120 items (30–60s)
    #  - large: roughly every 450–600 items (5–10m)
    next_small_pause_at = random.randint(45, 60)
    next_medium_pause_at = random.randint(90, 120)
    next_large_pause_at = random.randint(450, 600)
    next_periodic_warmup_at = random.randint(35, 55)

    # Adaptive cooldown settings for HTTP 429
    cooldown_applied = 0

    # NOTE: We do NOT scrape per-city. We scrape once per product page and then
    # replicate the found price for each city from settings.cities.
    for idx, r in enumerate(rows, start=1):
        # Stop at 08:00 if we have already been running in the night window.
        # This prevents morning scraping and safely uploads partial results.
        if entered_night_window and is_day_window_now():
            stopped_early_reason = "Stopped at 08:00 — night window finished."
            logger.warning(stopped_early_reason)
            break

        _update_403_window(idx)

        # Track if we ever crossed into the night window (e.g., started before 22:00).
        if not entered_night_window and is_night_now():
            entered_night_window = True

        product_code = r.product_code
        product_slug = r.slug

        attempt = 0
        max_403_retries_after_pause = 1  # retry once after a 403 cooldown

        while True:
            try:
                # Scrape once per product page (https://tabletki.ua/uk/{slug}/{code}/)
                # and replicate the found delivery price for each city in settings.cities.
                price = await scraper.get_min_delivery_price(product_code, product_slug=product_slug)

                # Success (or no price) resets blocking counters.
                consecutive_429 = 0
                consecutive_403 = 0

                if price is None:
                    logger.warning(f"[{idx}/{len(rows)}] {product_code}: delivery price NOT FOUND")
                else:
                    logger.info(f"[{idx}/{len(rows)}] {product_code}: delivery price = {price}")
                    for city in settings.cities:
                        out_records.append(
                            {
                                "code": str(product_code),
                                "slug": str(product_slug),
                                "city": str(city),
                                "delivery_price": float(price),
                            }
                        )
                break  # done with this product

            except Exception as e:
                errors += 1

                # Detect rate limiting (HTTP 429) and forbidden (HTTP 403) without tight coupling.
                resp = getattr(e, "response", None)
                status_code = getattr(resp, "status_code", None) if resp is not None else None

                is_429 = status_code == 429 or "HTTP 429" in str(e) or " 429" in str(e)
                is_403 = status_code == 403 or "HTTP 403" in str(e) or " 403" in str(e) or "403 Forbidden" in str(e)

                if is_429:
                    consecutive_429 += 1
                    consecutive_403 = 0
                    logger.error(f"[{idx}/{len(rows)}] {product_code}: HTTP 429 (consecutive={consecutive_429})")

                    # Adaptive cooldown: on the first 429 we cool down long enough to escape the rate window.
                    # On repeated 429s we cool down even longer.
                    base_cooldown = random.uniform(600.0, 1200.0)  # 10–20 minutes
                    extra_cooldown = random.uniform(180.0, 420.0) * max(0, consecutive_429 - 1)
                    cooldown = min(base_cooldown + extra_cooldown, 3600.0)  # cap at 60 minutes
                    cooldown_applied += 1
                    logger.warning(
                        f"Cooling down after 429: sleeping {cooldown:.1f}s (cooldowns_applied={cooldown_applied})"
                    )
                    await asyncio.sleep(cooldown)

                    if consecutive_429 >= 3:
                        stopped_early_reason = (
                            "Stopped early: 3 consecutive HTTP 429 (rate limit) even after cooldowns. "
                            "Stopping to avoid ban."
                        )
                        logger.error(stopped_early_reason)
                        break

                    # After cooldown, retry the same product once (fresh attempt).
                    attempt = 0
                    continue

                if is_403:
                    consecutive_403 += 1
                    consecutive_429 = 0

                    total_403 += 1
                    _update_403_window(idx)
                    window_403_count += 1
                    per100 = _current_403_per_100(idx)

                    logger.error(
                        f"[{idx}/{len(rows)}] {product_code}: HTTP 403 "
                        f"(consecutive={consecutive_403}, 403_per_100_items={per100})"
                    )

                    # Always apply a short cooldown after any 403, then warm up.
                    pause = random.uniform(*pause_403_once_range_sec)
                    logger.warning(f"403 detected. Cooling down {pause:.1f}s, then warmup (best-effort) and retry once.")
                    await asyncio.sleep(pause)
                    await warmup_visit(reason=f"403_once_{consecutive_403}")

                    attempt += 1
                    if attempt <= max_403_retries_after_pause:
                        # Retry the same product once after cooldown.
                        continue

                    # If we already retried and still get 403, handle repeated 403s.
                    if consecutive_403 >= 2:
                        if is_night_now():
                            # 🌙 Night mode: do NOT keep hammering. Save progress by pausing long,
                            # then try to resume if warmup becomes OK.
                            long_pause = random.uniform(3600.0, 7200.0)  # 1–2 hours
                            logger.warning(
                                f"Night mode: repeated 403 (consecutive={consecutive_403}). "
                                f"Sleeping {long_pause/60:.1f} min before retry."
                            )
                            await asyncio.sleep(long_pause)

                            ok = await warmup_visit(reason="night_resume_check")
                            if not ok:
                                suggested_restart = suggest_restart_dt(datetime.now())
                                stopped_early_reason = (
                                    "Warmup still blocked after long night pause. "
                                    "Saving progress and stopping to avoid hard ban. "
                                    "Try again later (e.g., in 1–2 hours or closer to deep night)."
                                    f" Suggested restart: {suggested_restart.strftime('%Y-%m-%d %H:%M')}."
                                )
                                logger.error(stopped_early_reason)
                                break

                            # Warmup OK: reset counters and retry this product from a fresh attempt.
                            consecutive_403 = 0
                            attempt = 0
                            continue

                        # 🌞 Day/evening mode:
                        # If we're close to night (20:00–22:00), it's better to pause until deep night
                        # rather than stopping completely.
                        if is_prenight_window_now():
                            hold_until = suggest_restart_dt(datetime.now())  # usually 22:30
                            wait_sec = max(0.0, (hold_until - datetime.now()).total_seconds())
                            logger.warning(
                                "Pre-night mode: repeated 403. "
                                f"Pausing until {hold_until.strftime('%Y-%m-%d %H:%M')} "
                                f"({wait_sec/60:.1f} min), then resume."
                            )
                            await asyncio.sleep(wait_sec)

                            ok = await warmup_visit(reason="prenight_resume_check")
                            if not ok:
                                suggested_restart = suggest_restart_dt(datetime.now())
                                stopped_early_reason = (
                                    "Pre-night resume check still looks blocked. "
                                    "Saving progress and stopping to avoid hard ban. "
                                    f"Suggested restart: {suggested_restart.strftime('%Y-%m-%d %H:%M')}."
                                )
                                logger.error(stopped_early_reason)
                                break

                            # Warmup OK: reset counters and retry this product.
                            consecutive_403 = 0
                            attempt = 0
                            continue

                        suggested_restart = suggest_restart_dt(datetime.now())
                        stopped_early_reason = (
                            "Repeated HTTP 403 outside night window. "
                            "Stopping to avoid hard ban."
                            f" Suggested restart: {suggested_restart.strftime('%Y-%m-%d %H:%M')}."
                        )
                        logger.error(stopped_early_reason)
                        break

                    # Stop safely after repeated 403s.
                    suggested_restart = suggest_restart_dt(datetime.now())
                    stopped_early_reason = (
                        "Stopped early: repeated HTTP 403 (Forbidden). "
                        "Likely anti-bot protection triggered. "
                        "Results saved, stopping to avoid hard ban."
                        f" Suggested restart: {suggested_restart.strftime('%Y-%m-%d %H:%M')}."
                    )
                    logger.error(stopped_early_reason)
                    break

                # Other errors
                consecutive_429 = 0
                consecutive_403 = 0
                logger.exception(f"[{idx}/{len(rows)}] {product_code}: ERROR: {e}")
                break

        if stopped_early_reason:
            break

        # --- Unpredictable batch pauses ---
        # Apply at most one pause per iteration (prefer the longest), then reschedule that checkpoint.
        if idx >= next_large_pause_at:
            pause = random.uniform(300.0, 600.0)  # 5–10 minutes
            logger.info(
                f"Batch pause after {idx} items (large, next≈{next_large_pause_at}): sleeping {pause:.1f}s"
            )
            await asyncio.sleep(pause)
            await warmup_visit(reason=f"batch_large_{idx}")
            next_large_pause_at = idx + random.randint(450, 600)

            # keep other schedules ahead of us
            if next_medium_pause_at <= idx:
                next_medium_pause_at = idx + random.randint(90, 120)
            if next_small_pause_at <= idx:
                next_small_pause_at = idx + random.randint(45, 60)

        elif idx >= next_medium_pause_at:
            pause = random.uniform(30.0, 60.0)  # 30–60 seconds
            logger.info(
                f"Batch pause after {idx} items (medium, next≈{next_medium_pause_at}): sleeping {pause:.1f}s"
            )
            await asyncio.sleep(pause)
            await warmup_visit(reason=f"batch_medium_{idx}")
            next_medium_pause_at = idx + random.randint(90, 120)

            if next_small_pause_at <= idx:
                next_small_pause_at = idx + random.randint(45, 60)

        elif idx >= next_small_pause_at:
            pause = random.uniform(20.0, 30.0)  # 20–30 seconds
            logger.info(
                f"Batch pause after {idx} items (small, next≈{next_small_pause_at}): sleeping {pause:.1f}s"
            )
            await asyncio.sleep(pause)
            await warmup_visit(reason=f"batch_small_{idx}")
            next_small_pause_at = idx + random.randint(45, 60)

        # Periodic warmup even without a batch pause (roughly every 35–55 items)
        if idx >= next_periodic_warmup_at:
            await warmup_visit(reason=f"periodic_{idx}")
            next_periodic_warmup_at = idx + random.randint(35, 55)

    
    # 2) сохранить JSON
    out_bytes = build_output_json(out_records)

    out_name = "competitors_delivery_total.json"

    # ALWAYS save locally first (so we never lose results)
    local_path: str | None = None
    try:
        local_path = _save_output_locally(out_name, out_bytes)
        logger.info(f"Saved output locally: {local_path}")
    except Exception as e:
        logger.exception(f"Failed to save output locally: {type(e).__name__}: {e}")

    # Upload with retries (network/SSL glitches happen)
    file_id: str | None = None
    max_upload_attempts = 5

    for attempt_no in range(1, max_upload_attempts + 1):
        try:
            file_id = drive.upload_json_bytes(settings.target_folder_id, out_name, out_bytes)
            logger.info(f"Uploaded result: {out_name} (file_id={file_id})")
            break
        except Exception as e:
            logger.exception(
                f"Upload attempt {attempt_no}/{max_upload_attempts} failed: {type(e).__name__}: {e}"
            )
            if attempt_no < max_upload_attempts:
                sleep_s = min((2 ** attempt_no) + random.uniform(0.0, 1.0), 30.0)
                logger.warning(f"Retrying upload in {sleep_s:.1f}s...")
                await asyncio.sleep(sleep_s)

    if not file_id:
        await notifier.send(
            "✅ Парсинг завершён, но ❌ загрузка в Google Drive не удалась (возможен кратковременный сбой SSL/сети).\n"
            f"Файл сохранён локально: {local_path}\n"
            f"Output: {out_name}"
        )

    # processed = number of products attempted; records = rows in JSON
    processed = idx if 'idx' in locals() else 0
    if processed > 0:
        _update_403_window(processed)
        per100_403[f"{window_start_idx}-{window_end_idx}"] = window_403_count

    per100_nonzero = {k: v for k, v in per100_403.items() if v}
    per100_str = "0" if not per100_nonzero else ", ".join(
        [f"{k}:{v}" for k, v in list(per100_nonzero.items())[:6]]
    )

    status_line = "⚠️ Stopped early due to rate limit." if stopped_early_reason else "✅ Completed."
    summary = (
        f"{status_line}\n"
        f"Cities: {', '.join(settings.cities)}\n"
        f"Processed products: {processed}/{len(rows)}\n"
        f"Output records: {len(out_records)}\n"
        f"Errors: {errors}\n"
        f"Total 403: {total_403}\n"
        f"403_per_100_items: {per100_str}\n"
        + (f"Reason: {stopped_early_reason}\n" if stopped_early_reason else "")
        + (f"Suggested restart: {suggested_restart.strftime('%Y-%m-%d %H:%M')}\n" if suggested_restart else "")
        + f"Output: {out_name}\n"
        + (f"File ID: {file_id}" if file_id else "File ID: (upload failed; saved locally)")
        + (f"\nLocal path: {local_path}" if local_path else "")
    )
    await notifier.send(summary)


if __name__ == "__main__":
    asyncio.run(run())

