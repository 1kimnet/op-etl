# stdlib only; works in ArcGIS Pro/Server envs
from __future__ import annotations

import logging
import logging.handlers
from pathlib import Path
from typing import Any, Mapping, Optional

DEFAULT_FMT = "%(asctime)s - %(levelname)s - [%(name)s] %(message)s"

def setup_logging(cfg: Optional[Mapping[str, Any]]) -> None:
    """
    Configure root logging from a dict (logging section from config.yaml).
    Idempotent: clears existing handlers to avoid duplicates.
    """
    # Sensible fallbacks if cfg is None or partial
    cfg = cfg or {}
    level_name = str(cfg.get("level", "INFO")).upper()
    console_level_name = str(cfg.get("console_level", level_name)).upper()
    fmt = cfg.get("format", DEFAULT_FMT)

    # Files
    summary_file = cfg.get("summary_file")   # e.g. logs/etl.log
    debug_file = cfg.get("debug_file")       # e.g. logs/etl.debug.log
    max_mb = cfg.get("max_file_size_mb", 0)
    backup_count = int(cfg.get("backup_count", 5))

    # Root logger with permissive level based on configured handlers
    root = logging.getLogger()
    root_requested = _coerce_level(level_name)
    console_requested = _coerce_level(console_level_name)

    # Determine minimal level needed across handlers so they can filter
    effective_root = min(root_requested, console_requested)
    if cfg.get("debug_file"):
        effective_root = min(effective_root, logging.DEBUG)
    if cfg.get("summary_file"):
        effective_root = min(effective_root, root_requested)

    root.setLevel(effective_root)

    # Nuke old handlers to prevent duplicate lines across reruns
    for h in list(root.handlers):
        root.removeHandler(h)
        try:
            h.close()
        except Exception:
            pass

    formatter = logging.Formatter(fmt)

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(_coerce_level(console_level_name))
    ch.setFormatter(formatter)
    root.addHandler(ch)

    # Ensure logs directory exists if any file is requested
    files = [p for p in [summary_file, debug_file] if p]
    if files:
        Path("logs").mkdir(exist_ok=True)

    def make_file_handler(path: str, level: int) -> logging.Handler:
        if max_mb and max_mb > 0:
            return logging.handlers.RotatingFileHandler(
                path, maxBytes=int(max_mb * 1024 * 1024), backupCount=backup_count, encoding="utf-8"
            )
        return logging.FileHandler(path, encoding="utf-8")

    # Summary file (usually INFO/WARNING+)
    if summary_file:
        fh = make_file_handler(summary_file, _coerce_level(level_name))
        fh.setLevel(_coerce_level(level_name))
        fh.setFormatter(formatter)
        root.addHandler(fh)

    # Debug file (full verbosity)
    if debug_file:
        dfh = make_file_handler(debug_file, logging.DEBUG)
        dfh.setLevel(logging.DEBUG)
        dfh.setFormatter(formatter)
        root.addHandler(dfh)

    # Don't let lib loggers spawn their own handlers
    _disable_library_basic_configs()


def _coerce_level(name_or_int: Any) -> int:
    if isinstance(name_or_int, int):
        return name_or_int
    try:
        return getattr(logging, str(name_or_int).upper())
    except Exception:
        return logging.INFO


def _disable_library_basic_configs() -> None:
    """
    Keep third-party/basicConfig noise from duplicating output.
    In practice: avoid calling basicConfig anywhere else.
    """
    for noisy in ("urllib3", "requests"):
        logging.getLogger(noisy).propagate = True
        logging.getLogger(noisy).handlers.clear()
