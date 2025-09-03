"""
Simple Logging System for OP-ETL

This module provides a unified, simple logging setup that replaces the complex
logging configuration system with a clean, consistent approach.

Key principles:
- Single logging setup function
- Consistent format across all modules
- Simple console and optional file output
- Clear success/failure indicators
"""

import logging as std_logging
import os
import sys
from pathlib import Path
from typing import Optional

# Import the standard logging module with a different name to avoid conflicts

def _supports_utf8_console() -> bool:
    """Best-effort check whether the current stdout can render UTF-8.

    Returns True if the stdout encoding looks like UTF-8; False otherwise.
    """
    enc = getattr(sys.stdout, "encoding", None) or ""
    return "utf" in enc.lower()


def _sanitize_console_text(text: str) -> str:
    """Replace emojis and non-ASCII characters with ASCII-safe alternatives.

    This keeps console output readable on Windows cmd.exe without UTF-8.
    """
    replacements = {
        "🚀": ">>",
        "✅": "[OK]",
        "❌": "[FAIL]",
        "⚠️": "[WARN]",
    }
    for k, v in replacements.items():
        text = text.replace(k, v)
    try:
        enc = getattr(sys.stdout, "encoding", None) or "ascii"
        text.encode(enc)  # will raise if not encodable
        return text
    except Exception:
        return text.encode("ascii", "ignore").decode("ascii")


class _ConsoleFormatter(std_logging.Formatter):
    def __init__(self, fmt: str, datefmt: Optional[str] = None, ascii_fallback: bool = False):
        super().__init__(fmt=fmt, datefmt=datefmt)
        self._ascii_fallback = ascii_fallback

    def format(self, record: std_logging.LogRecord) -> str:
        formatted = super().format(record)
        return _sanitize_console_text(formatted) if self._ascii_fallback else formatted


def _make_file_handler(file_path: Path, level_name: str) -> std_logging.Handler:
    file_path.parent.mkdir(parents=True, exist_ok=True)
    h = std_logging.FileHandler(file_path, mode='w', encoding='utf-8')
    h.setLevel(getattr(std_logging, level_name.upper(), std_logging.DEBUG))
    file_format = std_logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(name)s | %(funcName)s:%(lineno)d | %(message)s'
    )
    h.setFormatter(file_format)
    return h

def setup_pipeline_logging(
    console_level: str = "INFO",
    file_path: Optional[Path] = None,
    file_level: str = "DEBUG"
) -> None:
    """
    Configure consistent logging for the entire pipeline.

    Removes any existing root handlers, sets the root logger to DEBUG (to capture all messages),
    and installs a console StreamHandler writing to stdout. Optionally installs a FileHandler
    to the provided path (parents created if needed). Console and file handlers use different
    default formats and respect the provided level names.

    Parameters:
        console_level (str): Logging level name for the console handler (e.g., "INFO"). Default: "INFO".
        file_path (Optional[Path]): If provided, path to the log file; parent directories are created. If None, no file handler is added.
        file_level (str): Logging level name for the file handler (e.g., "DEBUG"). Ignored if file_path is None. Default: "DEBUG".
    """

    # Clear any existing handlers
    root_logger = std_logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Set root logger level to capture all messages
    root_logger.setLevel(std_logging.DEBUG)

    # Determine if we need ASCII-safe console fallback
    env_force_ascii = os.environ.get("OP_ETL_ASCII_CONSOLE", "").lower() in {"1", "true", "yes"}
    ascii_fallback = env_force_ascii or (os.name == "nt" and not _supports_utf8_console())

    # Console handler with clean format (and optional ASCII fallback)
    console_handler = std_logging.StreamHandler(sys.stdout)
    console_handler.setLevel(console_level.upper())
    console_format = _ConsoleFormatter(
        '%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%H:%M:%S',
        ascii_fallback=ascii_fallback,
    )
    console_handler.setFormatter(console_format)
    root_logger.addHandler(console_handler)

    # File handler with detailed format (if specified)
    if file_path:
        root_logger.addHandler(_make_file_handler(file_path, file_level))

def log_phase_start(phase_name: str) -> None:
    """
    Log a standard "phase start" message for the pipeline.

    Logs an INFO-level message indicating the beginning of the given phase (prefixed with a rocket emoji).

    Parameters:
        phase_name (str): Human-readable name of the pipeline phase being started.
    """
    std_logging.info(f"🚀 Starting {phase_name} phase")

def log_source_result(
    source_name: str,
    success: bool,
    feature_count: int = 0,
    error: Optional[str] = None
) -> None:
    """
    Log a standardized result message for a pipeline source.

    When success is True:
    - If feature_count > 0: logs an info message with a checkmark and the feature count (thousands-separated).
    - If feature_count == 0: logs a generic success info message.

    When success is False:
    - Logs an error message with a cross mark and, if provided, the error text in parentheses.

    Parameters:
        source_name: Human-readable name of the source being reported.
        success: True if the source processed successfully, False otherwise.
        feature_count: Number of features produced by the source (used only on success).
        error: Optional error description to include when success is False.
    """
    if success:
        if feature_count > 0:
            std_logging.info(f"✅ {source_name}: {feature_count:,} features")
        else:
            std_logging.info(f"✅ {source_name}: processed successfully")
    else:
        error_msg = f" ({error})" if error else ""
        std_logging.error(f"❌ {source_name}: failed{error_msg}")

def log_phase_complete(phase_name: str, total_sources: int, successful: int) -> None:
    """
    Log a standardized completion message for a pipeline phase.

    Logs an info message when all sources succeeded; otherwise logs a warning that includes the number of failed sources.

    Parameters:
        phase_name: Human-readable name of the pipeline phase (e.g., "ingest").
        total_sources: Total number of sources expected for the phase.
        successful: Number of sources that completed successfully.
    """
    if successful == total_sources:
        std_logging.info(f"✅ {phase_name} complete: {successful}/{total_sources} sources successful")
    else:
        failed = total_sources - successful
        std_logging.warning(f"⚠️  {phase_name} complete: {successful}/{total_sources} sources successful ({failed} failed)")

if __name__ == "__main__":
    """Test the logging setup."""
    import tempfile

    # Test console-only logging
    print("Testing console-only logging:")
    setup_pipeline_logging(console_level="INFO")

    std_logging.info("This is an info message")
    std_logging.warning("This is a warning message")
    std_logging.error("This is an error message")

    # Test with file logging
    print("\nTesting with file logging:")
    with tempfile.TemporaryDirectory() as tmpdir:
        log_file = Path(tmpdir) / "pipeline.log"

        setup_pipeline_logging(console_level="INFO", file_path=log_file)

        log_phase_start("Test Phase")
        log_source_result("test_source", True, 1234)
        log_source_result("failed_source", False, 0, "Network error")
        log_phase_complete("Test Phase", 2, 1)

        # Show file contents
        print(f"\nLog file contents ({log_file}):")
        with log_file.open('r') as f:
            print(f.read())
    print("✅ Logging test complete")
