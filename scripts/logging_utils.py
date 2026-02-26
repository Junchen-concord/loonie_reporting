from __future__ import annotations

import logging
import os
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = REPO_ROOT / "logs"


def _resolve_level(default: str = "INFO") -> int:
    level_name = str(os.getenv("LOG_LEVEL", default)).strip().upper()
    return getattr(logging, level_name, logging.INFO)


def setup_logger(name: str, log_file_stem: str, *, level: int | None = None) -> logging.Logger:
    """
    Configure a module logger with:
    - console output
    - daily rotating file output under ./logs
    """
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger.setLevel(level or _resolve_level())
    logger.propagate = False

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] [%(name)s] %(message)s")

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(level or _resolve_level())
    logger.addHandler(stream_handler)

    file_handler = TimedRotatingFileHandler(
        filename=str(LOG_DIR / f"{log_file_stem}.log"),
        when="midnight",
        interval=1,
        backupCount=30,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(level or _resolve_level())
    logger.addHandler(file_handler)

    return logger
