import logging
import sys
import os
from pathlib import Path
from logging.handlers import RotatingFileHandler
from typing import Optional
from concurrent_log_handler import ConcurrentRotatingFileHandler as CRFH

MAX_BYTES = int(os.getenv("LOG_ROTATE_MAX_BYTES", 2 * 1024 * 1024))  # 2MB
BACKUP_COUNT = int(os.getenv("LOG_ROTATE_BACKUPS", 1))               # 1 backup
DEFAULT_LOG_PATH = os.getenv("LOG_FILE", "configs/log.log")
ENV_LOG_TO_FILE = os.getenv("LOG_TO_FILE", "1").strip() not in ("0", "false", "no")

class ColoredFormatter(logging.Formatter):
    COLORS = {
        logging.DEBUG: "\033[0;33m",    # Yellow
        logging.INFO: "\033[0;32m",     # Green
        logging.WARNING: "\033[0;35m",  # Purple
        logging.ERROR: "\033[0;31m",    # Red
        logging.CRITICAL: "\033[1;31m"  # Bright Red
    }
    RESET = "\033[0m"

    def __init__(self, fmt: str, datefmt: Optional[str] = None, use_color: Optional[bool] = None):
        super().__init__(fmt=fmt, datefmt=datefmt)
        # Auto-detect color support unless explicitly forced
        if use_color is None:
            self.use_color = sys.stdout.isatty()
        else:
            self.use_color = use_color

    def format(self, record):
        msg = super().format(record)
        if not self.use_color:
            return msg
        return f"{self.COLORS.get(record.levelno, '')}{msg}{self.RESET}"


def setup_logger(
    name: Optional[str] = None,
    level: int = logging.INFO,
    log_to_file: Optional[bool] = None,
    file_path: str = DEFAULT_LOG_PATH,
    max_bytes: int = MAX_BYTES,
    backup_count: int = BACKUP_COUNT,
) -> logging.Logger:
    """
    Production-safe logger setup:
      - Colored console output.
      - Concurrent-safe file rotation on Windows when concurrent-log-handler is installed.
      - Single initialization guard, no duplicate handlers.
      - Env overrides: LOG_TO_FILE, LOG_FILE, LOG_ROTATE_MAX_BYTES, LOG_ROTATE_BACKUPS
    """
    logger = logging.getLogger(name)
    # Prevent reconfiguration
    if getattr(logger, "_configured", False):
        return logger

    logger.setLevel(level)
    logger.propagate = False  # don't double-log to root

    # Remove any existing handlers (e.g., if someone configured root elsewhere)
    for h in list(logger.handlers):
        logger.removeHandler(h)

    # Console handler (colored)
    console_fmt = "%(asctime)s [%(levelname)s] %(filename)s - %(name)s - %(funcName)s:%(lineno)d - %(message)s"
    console = logging.StreamHandler(stream=sys.stdout)
    console.setLevel(level)
    console.setFormatter(ColoredFormatter(console_fmt, datefmt="%Y-%m-%d %H:%M:%S"))
    logger.addHandler(console)

    # File handler
    if log_to_file is None:
        log_to_file = ENV_LOG_TO_FILE

    if log_to_file:
        path_obj = Path(file_path)
        path_obj.parent.mkdir(parents=True, exist_ok=True)

        file_fmt = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(filename)s - %(name)s - %(funcName)s:%(lineno)d - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        if CRFH is not None:
            # Concurrent & Windows-friendly rotation
            fh = CRFH(
                str(path_obj),
                maxBytes=max_bytes,
                backupCount=backup_count,
                encoding="utf-8",
            )
        else:
            # Fallback: not process-safe, but delay=True helps reduce lock window
            fh = RotatingFileHandler(
                str(path_obj),
                maxBytes=max_bytes,
                backupCount=backup_count,
                encoding="utf-8",
                delay=True,
            )
        fh.setLevel(level)
        fh.setFormatter(file_fmt)
        logger.addHandler(fh)

    logger._configured = True
    return logger