from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path


def setup_logger(level: str, log_file: Path, max_bytes: int, backup_count: int) -> logging.Logger:
    logger = logging.getLogger("vps_bridge")
    logger.setLevel(getattr(logging, level, logging.ERROR))
    logger.propagate = False

    log_file.parent.mkdir(parents=True, exist_ok=True)

    if logger.handlers:
        return logger

    handler = RotatingFileHandler(
        filename=log_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger
