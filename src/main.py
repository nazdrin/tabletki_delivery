from __future__ import annotations

import asyncio
import random
from datetime import datetime

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
        msg = "ERROR: CITIES is empty. Set CITIES in .env, e.g. CITIES=kiev"
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

    # JSON output records: one record per (product_code, city) where price is found.
    out_records: list[dict] = []
    errors = 0
    consecutive_429 = 0
    stopped_early_reason: str | None = None

    # NOTE: We do NOT scrape per-city. We scrape once per product page and then
    # replicate the found price for each city from settings.cities.
    for idx, r in enumerate(rows, start=1):
        product_code = r.product_code
        product_slug = r.slug
        try:
            # We keep passing a city for backward compatibility with the scraper signature,
            # but the scraping logic should rely on the product page and/or slug.
            city_for_request = settings.cities[0]
            price = await scraper.get_min_delivery_price(product_code, city_for_request, product_slug=product_slug)
            consecutive_429 = 0

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

        except Exception as e:
            errors += 1

            # Detect rate limiting (HTTP 429) as safely as possible without tight coupling
            is_429 = False
            if "429" in str(e):
                is_429 = True
            else:
                resp = getattr(e, "response", None)
                if resp is not None and getattr(resp, "status_code", None) == 429:
                    is_429 = True

            if is_429:
                consecutive_429 += 1
                logger.error(
                    f"[{idx}/{len(rows)}] {product_code}: HTTP 429 (consecutive={consecutive_429})"
                )
                if consecutive_429 >= 3:
                    stopped_early_reason = (
                        "Stopped early: 3 consecutive HTTP 429 (rate limit). "
                        "Cooling down to avoid ban."
                    )
                    logger.error(stopped_early_reason)
                    break
            else:
                consecutive_429 = 0
                logger.exception(f"[{idx}/{len(rows)}] {product_code}: ERROR: {e}")

        # Batch pause to reduce rate-limit risk
        if idx % 25 == 0:
            pause = random.uniform(120.0, 300.0)
            logger.info(f"Batch pause after {idx} items: sleeping {pause:.1f}s")
            await asyncio.sleep(pause)

    # 2) сохранить JSON
    out_bytes = build_output_json(out_records)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = "PARTIAL" if stopped_early_reason else "FULL"
    out_name = f"tabletki_delivery_prices_{suffix}_{ts}.json"

    # Upload JSON with correct MIME type so Google Drive doesn't treat it as XLSX/Sheets.
    file_id = drive.upload_json_bytes(settings.target_folder_id, out_name, out_bytes)

    logger.info(f"Uploaded result: {out_name} (file_id={file_id})")

    # processed = number of products attempted; records = rows in JSON
    processed = idx if 'idx' in locals() else 0
    status_line = "⚠️ Stopped early due to rate limit." if stopped_early_reason else "✅ Completed."
    summary = (
        f"{status_line}\n"
        f"Cities: {', '.join(settings.cities)}\n"
        f"Processed products: {processed}/{len(rows)}\n"
        f"Output records: {len(out_records)}\n"
        f"Errors: {errors}\n"
        + (f"Reason: {stopped_early_reason}\n" if stopped_early_reason else "")
        + f"Output: {out_name}\n"
        + f"File ID: {file_id}"
    )
    await notifier.send(summary)


if __name__ == "__main__":
    asyncio.run(run())