import logging
import os
from pathlib import Path

def setup_logger() -> logging.Logger:
    logger = logging.getLogger("tabletki_parser")

    # Configurable level via env, e.g. LOG_LEVEL=DEBUG
    level_name = os.getenv("LOG_LEVEL", "INFO").upper().strip()
    level = getattr(logging, level_name, logging.INFO)

    logger.setLevel(level)

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(fmt)

    Path("logs").mkdir(exist_ok=True)
    fh = logging.FileHandler("logs/run.log", encoding="utf-8")
    fh.setLevel(level)
    fh.setFormatter(fmt)

    # Avoid duplicate handlers on repeated runs/imports
    if not logger.handlers:
        logger.addHandler(ch)
        logger.addHandler(fh)

    return logger