# src/config.py
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

__all__ = ["Settings", "load_settings"]


@dataclass(frozen=True)
class Settings:
    # Google Drive
    google_credentials_path: str
    source_folder_id: str
    target_folder_id: str
    input_filename: str

    # Parser
    max_items: int
    cities: list[str]
    min_delay_sec: float
    max_delay_sec: float
    http_timeout_sec: float

    # Telegram
    telegram_bot_token: Optional[str]
    telegram_chat_ids: list[int]


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

    cities_raw = os.getenv("CITIES", "kiev").strip()
    cities = [c.strip() for c in cities_raw.split(",") if c.strip()]

    min_delay_sec = float(os.getenv("MIN_DELAY_SEC", "2.0"))
    max_delay_sec = float(os.getenv("MAX_DELAY_SEC", "5.5"))
    http_timeout_sec = float(os.getenv("HTTP_TIMEOUT_SEC", "25"))

    telegram_bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_ids_raw = os.getenv("TELEGRAM_CHAT_IDS", "")
    telegram_chat_ids: list[int] = []
    for part in chat_ids_raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            telegram_chat_ids.append(int(part))
        except ValueError:
            pass

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
    )
