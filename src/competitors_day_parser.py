from __future__ import annotations

import asyncio
import json
import os
import random
import re
import tempfile
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .competitors_scraper import CompetitorsDeliveryScraper, RateLimited
from .excel_io import build_output_json, read_input_rows
from .gdrive_client import GoogleDriveClient
from .telegram_notifier import TelegramNotifier


@dataclass
class RuntimeState:
    day: str
    next_offset: int
    cycles_completed_today: int
    last_hour_key: str
    last_stop_reason: str


def normalize_code(v: object) -> str:
    """Normalize product codes coming from Excel/JSON."""
    if v is None:
        return ""
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        if v.is_integer():
            return str(int(v))
        return str(v).strip()

    s = str(v).strip()
    if not s:
        return ""
    m = re.fullmatch(r"(\d+)\.0+", s)
    if m:
        return m.group(1)
    return s


def update_competitors_json(existing_bytes: Optional[bytes], target_cities: List[str], updates: Dict[str, float]) -> bytes:
    """Update competitors JSON with new prices."""
    data: List[dict]
    if existing_bytes:
        try:
            loaded = json.loads(existing_bytes.decode("utf-8"))
            data = loaded if isinstance(loaded, list) else []
        except Exception:
            data = []
    else:
        data = []

    target_city_list: List[str] = [str(c).strip() for c in target_cities if str(c).strip()]
    target_city_cf_to_canon: Dict[str, str] = {c.casefold(): c for c in target_city_list}

    updates_norm: Dict[str, float] = {}
    for code_raw, price in updates.items():
        c = normalize_code(code_raw)
        if not c:
            continue
        updates_norm[c] = float(price)

    updated_codes = set(updates_norm.keys())
    if not updated_codes:
        return build_output_json(data)

    cleaned: List[dict] = []
    seen_keys: set[Tuple[str, str]] = set()
    for rec in data:
        code = normalize_code(rec.get("code", ""))
        if not code:
            cleaned.append(rec)
            continue

        city_raw = str(rec.get("city", "")).strip()
        city_cf = city_raw.casefold() if city_raw else ""

        if code in updated_codes:
            if city_cf not in target_city_cf_to_canon:
                continue
            canon_city = target_city_cf_to_canon[city_cf]
            key = (code, canon_city.casefold())
            if key in seen_keys:
                continue
            seen_keys.add(key)
            rec["code"] = code
            rec["city"] = canon_city
            cleaned.append(rec)
        else:
            rec["code"] = code
            cleaned.append(rec)

    data = cleaned

    idx_map: Dict[Tuple[str, str], int] = {}
    for i, rec in enumerate(data):
        code = normalize_code(rec.get("code", ""))
        city = str(rec.get("city", "")).strip()
        if code and city:
            idx_map[(code, city.casefold())] = i

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


def _is_within_window(now: datetime, window_start: time, window_end: time) -> bool:
    t = now.time()
    if window_start <= window_end:
        return window_start <= t < window_end
    return t >= window_start or t < window_end


def _next_window_start(now: datetime, window_start: time, window_end: time) -> datetime:
    today_start = datetime.combine(now.date(), window_start)
    if window_start <= window_end:
        if now < today_start:
            return today_start
        return datetime.combine(now.date() + timedelta(days=1), window_start)

    if now.time() < window_start:
        return today_start
    return datetime.combine(now.date() + timedelta(days=1), window_start)


def _next_hour_start(now: datetime) -> datetime:
    return (now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))


def _default_state() -> RuntimeState:
    return RuntimeState(
        day="",
        next_offset=0,
        cycles_completed_today=0,
        last_hour_key="",
        last_stop_reason="init",
    )


def _load_state(path: Path) -> RuntimeState:
    if not path.exists():
        return _default_state()
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        return RuntimeState(
            day=str(raw.get("day", "")),
            next_offset=max(0, int(raw.get("next_offset", 0))),
            cycles_completed_today=max(0, int(raw.get("cycles_completed_today", 0))),
            last_hour_key=str(raw.get("last_hour_key", "")),
            last_stop_reason=str(raw.get("last_stop_reason", "")),
        )
    except Exception:
        return _default_state()


def _atomic_write_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)
    finally:
        try:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
        except OSError:
            pass


def _save_state(path: Path, state: RuntimeState) -> None:
    payload = {
        "day": state.day,
        "next_offset": state.next_offset,
        "cycles_completed_today": state.cycles_completed_today,
        "last_hour_key": state.last_hour_key,
        "last_stop_reason": state.last_stop_reason,
        "updated_at": datetime.now().isoformat(timespec="seconds"),
    }
    _atomic_write_bytes(path, json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"))


def _resolve_output_path(raw: str) -> Path:
    p = Path(raw).expanduser()
    if p.is_absolute():
        return p
    return (Path.cwd() / p).resolve()


def _format_next(now: datetime, window_start: time, window_end: time, state: RuntimeState, runs_per_day: int) -> str:
    if state.cycles_completed_today >= runs_per_day:
        return _next_window_start(now, window_start, window_end).strftime("%Y-%m-%d %H:%M")
    if _is_within_window(now, window_start, window_end) and state.last_hour_key == now.strftime("%Y-%m-%dT%H"):
        return _next_hour_start(now).strftime("%Y-%m-%d %H:%M")
    if not _is_within_window(now, window_start, window_end):
        return _next_window_start(now, window_start, window_end).strftime("%Y-%m-%d %H:%M")
    return now.strftime("%Y-%m-%d %H:%M")


async def run_day_competitors_parser(settings, logger) -> None:
    window_start = settings.competitors_window_start
    window_end = settings.competitors_window_end
    state_file = Path(getattr(settings, "competitors_state_file", "out/competitors_state.json"))
    save_every_minutes = max(1, int(getattr(settings, "competitors_save_every_minutes", 30)))
    save_interval = timedelta(minutes=save_every_minutes)

    batch_size = max(1, int(settings.competitors_batch_size))
    hourly_quota = batch_size
    items_per_hour_env = max(1, int(settings.competitors_items_per_hour))
    if items_per_hour_env != hourly_quota:
        logger.info(
            "Using COMPETITORS_BATCH_SIZE as hourly quota: batch_size=%s, items_per_hour_env=%s (env ignored for pacing).",
            batch_size,
            items_per_hour_env,
        )

    notifier = TelegramNotifier(
        bot_token=settings.telegram_bot_token,
        chat_ids=settings.telegram_chat_ids,
        timeout_sec=15.0,
    )
    drive = GoogleDriveClient(settings.google_credentials_path)

    while True:
        now = datetime.now()
        state = _load_state(state_file)
        today = now.date().isoformat()
        if state.day != today:
            state.day = today
            state.cycles_completed_today = 0
            state.last_hour_key = ""
            state.last_stop_reason = "new_day_reset_keep_offset"
            _save_state(state_file, state)

        if not _is_within_window(now, window_start, window_end):
            next_start = _next_window_start(now, window_start, window_end)
            sleep_sec = max(1.0, (next_start - now).total_seconds())
            state.last_stop_reason = "outside_work_window_sleep"
            _save_state(state_file, state)
            logger.info(
                "Scheduler idle: outside window %s-%s, now=%s, sleep_until=%s (%.0fs)",
                window_start.strftime("%H:%M"),
                window_end.strftime("%H:%M"),
                now.strftime("%Y-%m-%d %H:%M:%S"),
                next_start.strftime("%Y-%m-%d %H:%M:%S"),
                sleep_sec,
            )
            await asyncio.sleep(sleep_sec)
            continue

        hour_key = now.strftime("%Y-%m-%dT%H")
        if state.last_hour_key == hour_key:
            next_hour = _next_hour_start(now)
            sleep_sec = max(1.0, (next_hour - now).total_seconds())
            state.last_stop_reason = "hour_already_processed_sleep"
            _save_state(state_file, state)
            logger.info(
                "Scheduler idle: current hour already processed (%s), sleep_until=%s (%.0fs)",
                hour_key,
                next_hour.strftime("%Y-%m-%d %H:%M:%S"),
                sleep_sec,
            )
            await asyncio.sleep(sleep_sec)
            continue

        f = drive.find_file_in_folder(settings.source_folder_id, settings.input_filename)
        if not f:
            f = drive.find_latest_xlsx_in_folder(settings.source_folder_id)
        if not f:
            msg = "ERROR: No goods .xlsx file found in SOURCE_FOLDER_ID. Retry in 5 minutes."
            logger.error(msg)
            await notifier.send(msg)
            await asyncio.sleep(300)
            continue

        xlsx_bytes = drive.download_file_bytes(f.id)
        rows = read_input_rows(xlsx_bytes)
        if not rows:
            msg = "ERROR: Goods file has no valid rows. Retry in 5 minutes."
            logger.error(msg)
            await notifier.send(msg)
            await asyncio.sleep(300)
            continue

        rows.sort(key=lambda r: r.numerical_order)
        rows = rows[: settings.competitors_total_items]
        total_items = len(rows)
        if total_items == 0:
            state.last_stop_reason = "no_rows_after_limit_sleep"
            _save_state(state_file, state)
            logger.info("Day competitors parser: no rows after limit, retry in 5 minutes.")
            await asyncio.sleep(300)
            continue

        page_count = max(1, (total_items + batch_size - 1) // batch_size)
        if state.next_offset >= total_items:
            state.next_offset = 0

        start_idx = state.next_offset
        end_idx = min(start_idx + hourly_quota, total_items)
        chunk = rows[start_idx:end_idx]
        if not chunk:
            state.next_offset = 0
            state.last_stop_reason = "empty_chunk_reset_offset_sleep"
            _save_state(state_file, state)
            logger.info("Day competitors parser: empty chunk, reset offset and retry in 1 minute.")
            await asyncio.sleep(60)
            continue

        page_index = start_idx // batch_size
        logger.info(
            "Day competitors run: offset=%s limit=%s range=%s-%s of %s page=%s/%s cycles_done=%s cycle_hours_env=%s",
            start_idx,
            hourly_quota,
            start_idx + 1,
            end_idx,
            total_items,
            page_index + 1,
            page_count,
            state.cycles_completed_today,
            settings.competitors_cycle_hours,
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

        city = settings.competitors_city
        limit = settings.competitors_offers_per_product
        output_cities: List[str] = (
            getattr(settings, "cities_output", None)
            or getattr(settings, "cities_display", None)
            or getattr(settings, "cities_raw", None)
            or list(getattr(settings, "cities", []) or [])
        )
        excluded_sellers: List[str] = getattr(settings, "competitors_excluded_sellers", []) or []
        if excluded_sellers:
            logger.info("Excluded sellers patterns: %s", excluded_sellers)
        logger.info(
            "Scraping city slug=%s, writing updates for cities=%s",
            city,
            ", ".join(list(output_cities)),
        )

        out_name = settings.competitors_output_filename
        out_path = _resolve_output_path(out_name)
        if out_path.exists():
            base_bytes: Optional[bytes] = out_path.read_bytes()
        else:
            existing = drive.find_file_in_folder(settings.target_folder_id, out_name)
            base_bytes = drive.download_file_bytes(existing.id) if existing else None
            if base_bytes:
                _atomic_write_bytes(out_path, base_bytes)

        current_output_bytes: bytes = base_bytes if base_bytes is not None else build_output_json([])
        if not out_path.exists():
            _atomic_write_bytes(out_path, current_output_bytes)

        pending_updates: Dict[str, float] = {}
        last_flush_at = datetime.now()

        async def _flush_output(reason: str, force: bool = False) -> int:
            nonlocal current_output_bytes, pending_updates, last_flush_at
            applied = len(pending_updates)
            uploaded_file_id = ""
            if not pending_updates and not force:
                return out_path.stat().st_size if out_path.exists() else 0
            if pending_updates:
                current_output_bytes = update_competitors_json(
                    current_output_bytes,
                    target_cities=list(output_cities),
                    updates=pending_updates,
                )
                pending_updates = {}
            _atomic_write_bytes(out_path, current_output_bytes)
            size_bytes = out_path.stat().st_size
            uploaded_file_id = drive.upsert_json_bytes(settings.target_folder_id, out_name, current_output_bytes)
            last_flush_at = datetime.now()
            logger.info(
                "Output saved: reason=%s path=%s size=%s bytes applied_updates=%s drive_file_id=%s",
                reason,
                out_path,
                size_bytes,
                applied,
                uploaded_file_id,
            )
            return size_bytes

        errors = 0
        processed = 0
        stop_reason = "hour_quota_reached"
        consecutive_429 = 0
        consecutive_403 = 0
        slot_started_at = datetime.now()
        target_interval_sec = 3600.0 / float(hourly_quota)

        async def _cooldown_after_429() -> None:
            nonlocal consecutive_429
            consecutive_429 += 1
            cd = min(random.uniform(600.0, 1200.0) + random.uniform(120.0, 360.0) * max(0, consecutive_429 - 1), 3600.0)
            logger.warning("HTTP 429 cooldown: %.1f min (consecutive_429=%s)", cd / 60.0, consecutive_429)
            await asyncio.sleep(cd)

        async def _cooldown_after_403() -> None:
            nonlocal consecutive_403
            consecutive_403 += 1
            cd = random.uniform(45.0, 90.0) if consecutive_403 <= 1 else random.uniform(180.0, 420.0)
            logger.warning("HTTP 403 cooldown: %.1fs (consecutive_403=%s)", cd, consecutive_403)
            await asyncio.sleep(cd)

        for i, r in enumerate(chunk, start=1):
            if not _is_within_window(datetime.now(), window_start, window_end):
                stop_reason = "window_closed_mid_chunk"
                logger.info(
                    "Stop requested: reason=%s processed=%s/%s offset=%s",
                    stop_reason,
                    processed,
                    len(chunk),
                    state.next_offset,
                )
                break

            code = normalize_code(r.product_code)
            slug = str(r.slug).strip() if r.slug is not None else ""

            tries = 0
            max_tries = 2
            while True:
                try:
                    min_price, offers = await scraper.get_min_price_from_first_sellers(
                        slug=slug,
                        code=code,
                        city=city,
                        sellers_limit=limit,
                        excluded_sellers=excluded_sellers,
                    )
                    consecutive_429 = 0
                    consecutive_403 = 0
                    if min_price is None:
                        logger.warning("[%s/%s] %s: no offers parsed", i, len(chunk), code)
                        break
                    pending_updates[code] = float(min_price)
                    logger.info(
                        "[%s/%s] %s: min among first %s = %s (offers_used=%s)",
                        i,
                        len(chunk),
                        code,
                        limit,
                        min_price,
                        len(offers),
                    )
                    break
                except RateLimited as e:
                    errors += 1
                    logger.error("[%s/%s] %s: ERROR RateLimited: %s", i, len(chunk), code, e)
                    await _cooldown_after_429()
                    tries += 1
                    if tries <= max_tries:
                        logger.warning("[%s/%s] %s: retry after 429 cooldown (try %s/%s)", i, len(chunk), code, tries, max_tries)
                        continue
                    logger.error("[%s/%s] %s: giving up after repeated 429", i, len(chunk), code)
                    break
                except Exception as e:
                    msg = str(e)
                    is_403 = " 403" in msg or "HTTP 403" in msg or "403 Forbidden" in msg
                    is_429 = " 429" in msg or "HTTP 429" in msg
                    errors += 1
                    if is_429:
                        logger.error("[%s/%s] %s: ERROR 429-like: %s: %s", i, len(chunk), code, type(e).__name__, e)
                        await _cooldown_after_429()
                        tries += 1
                        if tries <= max_tries:
                            logger.warning(
                                "[%s/%s] %s: retry after 429-like cooldown (try %s/%s)",
                                i,
                                len(chunk),
                                code,
                                tries,
                                max_tries,
                            )
                            continue
                        logger.error("[%s/%s] %s: giving up after repeated 429-like", i, len(chunk), code)
                        break
                    if is_403:
                        logger.error("[%s/%s] %s: ERROR 403-like: %s: %s", i, len(chunk), code, type(e).__name__, e)
                        await _cooldown_after_403()
                        tries += 1
                        if tries <= max_tries:
                            logger.warning("[%s/%s] %s: retry after 403 cooldown (try %s/%s)", i, len(chunk), code, tries, max_tries)
                            continue
                        logger.error("[%s/%s] %s: giving up after repeated 403-like", i, len(chunk), code)
                        break
                    logger.exception("[%s/%s] %s: ERROR %s: %s", i, len(chunk), code, type(e).__name__, e)
                    break

            processed += 1
            state.next_offset = start_idx + processed
            if state.next_offset >= total_items:
                state.next_offset = 0
                state.cycles_completed_today += 1
            state.last_stop_reason = f"in_progress_{processed}"
            _save_state(state_file, state)

            if datetime.now() - last_flush_at >= save_interval:
                await _flush_output(reason=f"periodic_{save_every_minutes}m")

            expected_elapsed = processed * target_interval_sec
            actual_elapsed = (datetime.now() - slot_started_at).total_seconds()
            if actual_elapsed < expected_elapsed:
                await asyncio.sleep(expected_elapsed - actual_elapsed)

        if processed < len(chunk) and stop_reason == "hour_quota_reached":
            stop_reason = "stopped_before_hour_quota"
        if processed > 0:
            state.last_hour_key = hour_key

        size_bytes = await _flush_output(reason="run_end", force=True)
        file_id = drive.find_file_in_folder(settings.target_folder_id, out_name)
        drive_file_id = file_id.id if file_id else ""

        state.last_stop_reason = stop_reason
        _save_state(state_file, state)

        logger.info(
            "Day competitors run finished: reason=%s processed=%s/%s quota=%s offset=%s cycles_done=%s output_size=%s bytes",
            stop_reason,
            processed,
            len(chunk),
            hourly_quota,
            state.next_offset,
            state.cycles_completed_today,
            size_bytes,
        )
        await notifier.send(
            "✅ Day competitors run done\n"
            f"Window: {window_start.strftime('%H:%M')}-{window_end.strftime('%H:%M')}\n"
            f"Stop reason: {stop_reason}\n"
            f"Quota/hour: {hourly_quota}\n"
            f"Chunk: {start_idx + 1}-{end_idx} of {total_items}\n"
            f"Processed in run: {processed}\n"
            f"Cycles done today: {state.cycles_completed_today}\n"
            f"Next offset: {state.next_offset}\n"
            f"Errors: {errors}\n"
            f"Output: {out_name}\n"
            f"Output size: {size_bytes} bytes\n"
            f"State: {state_file}\n"
            f"File ID: {drive_file_id}"
        )
