"""
Simple Logging System for OP-ETL v2.0

This module provides a unified, simple logging setup that replaces the complex
logging configuration system with a clean, consistent approach.

Key principles:
- Single logging setup function
- Consistent format across all modules
- Simple console and optional file output
- Clear success/failure indicators
"""

import logging
import sys
from typing import Optional
from pathlib import Path

def setup_pipeline_logging(
    console_level: str = "INFO",
    file_path: Optional[Path] = None,
    file_level: str = "DEBUG"
) -> None:
    """Configure simple, consistent logging for entire pipeline."""

    # Clear any existing handlers
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Set root logger level to capture all messages
    root_logger.setLevel(logging.DEBUG)

    # Console handler with clean format
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, console_level.upper()))
    console_format = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%H:%M:%S'
    )
    console_handler.setFormatter(console_format)
    root_logger.addHandler(console_handler)

    # File handler with detailed format (if specified)
    if file_path:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(file_path, mode='w', encoding='utf-8')
        file_handler.setLevel(getattr(logging, file_level.upper()))
        file_format = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(name)s | %(funcName)s:%(lineno)d | %(message)s'
        )
        file_handler.setFormatter(file_format)
        root_logger.addHandler(file_handler)

def log_phase_start(phase_name: str) -> None:
    """Standard start message for pipeline phases."""
    logging.info(f"🚀 Starting {phase_name} phase")

def log_source_result(
    source_name: str,
    success: bool,
    feature_count: int = 0,
    error: Optional[str] = None
) -> None:
    """Standard result logging for sources."""
    if success:
        if feature_count > 0:
            logging.info(f"✅ {source_name}: {feature_count:,} features")
        else:
            logging.info(f"✅ {source_name}: processed successfully")
    else:
        error_msg = f" ({error})" if error else ""
        logging.error(f"❌ {source_name}: failed{error_msg}")

def log_phase_complete(phase_name: str, total_sources: int, successful: int) -> None:
    """Standard completion message."""
    if successful == total_sources:
        logging.info(f"✅ {phase_name} complete: {successful}/{total_sources} sources successful")
    else:
        failed = total_sources - successful
        logging.warning(f"⚠️  {phase_name} complete: {successful}/{total_sources} sources successful ({failed} failed)")

if __name__ == "__main__":
    """Test the logging setup."""
    import tempfile
    
    # Test console-only logging
    print("Testing console-only logging:")
    setup_pipeline_logging(console_level="INFO")
    
    logging.info("This is an info message")
    logging.warning("This is a warning message")
    logging.error("This is an error message")
    
    # Test with file logging
    print("\nTesting with file logging:")
    with tempfile.NamedTemporaryFile(suffix='.log', delete=False) as f:
        log_file = Path(f.name)
    
    setup_pipeline_logging(console_level="INFO", file_path=log_file)
    
    log_phase_start("Test Phase")
    log_source_result("test_source", True, 1234)
    log_source_result("failed_source", False, 0, "Network error")
    log_phase_complete("Test Phase", 2, 1)
    
    # Show file contents
    print(f"\nLog file contents ({log_file}):")
    with log_file.open('r') as f:
        print(f.read())
    
    # Cleanup
    log_file.unlink()
    print("✅ Logging test complete")