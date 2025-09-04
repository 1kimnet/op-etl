import argparse
import contextlib
import logging
import os
import shutil
import stat
import sys
import time
from pathlib import Path

from etl.config import ConfigError, load_config
from etl.paths import ensure_workspaces


def clear_arcpy_caches():
    """Clear ArcPy internal caches and reset workspace to avoid locks."""
    try:
        # Lazy import to avoid heavy ArcPy init before logging
        # Reset workspace to system temp directory to avoid locks
        import tempfile

        import arcpy  # noqa: F401
        temp_dir = tempfile.gettempdir()

        # Use setattr to avoid Pylance warnings about dynamic attributes
        # Ignore if assignment fails in this environment/version
        with contextlib.suppress(AttributeError, TypeError):
            setattr(arcpy.env, 'workspace', str(temp_dir))
        with contextlib.suppress(AttributeError, TypeError):
            setattr(arcpy.env, 'scratchWorkspace', str(temp_dir))

        # Clear any cached connections (may be unavailable in some ArcPy versions)
        with contextlib.suppress(Exception):
            arcpy.ClearWorkspaceCache_management()

        # Force garbage collection
        import gc
        gc.collect()

        # Small delay to let Windows release file handles
        time.sleep(0.5)

    except Exception as e:
        logging.debug(f"Error clearing ArcPy caches: {e}")


def remove_geodatabase_safely(gdb_path):
    """
    Safely remove a geodatabase directory with ArcPy-aware cleanup.
    """
    gdb_path = Path(gdb_path).resolve()

    if not gdb_path.exists():
        return True

    logging.info(f"Removing geodatabase: {gdb_path}")

    # Step 1: Clear ArcPy caches first
    clear_arcpy_caches()

    # Step 2: Try ArcPy delete first (handles ArcGIS locks better)
    try:
        import arcpy  # Lazy import
        if arcpy.Exists(str(gdb_path)):
            logging.debug("Using ArcPy Delete management tool")
            arcpy.management.Delete(str(gdb_path))
            if not gdb_path.exists():
                logging.info("Successfully removed geodatabase using ArcPy")
                return True
    except Exception as e:
        logging.debug(f"ArcPy Delete failed: {e}")

    # Step 3: Try standard filesystem removal with retries
    def handle_remove_readonly(func, path, exc):
        """Error handler for read-only files."""
        # PermissionError is a subclass of OSError; suppress both via OSError
        with contextlib.suppress(OSError):
            os.chmod(path, stat.S_IWRITE)
            func(path)

    max_attempts = 5
    for attempt in range(max_attempts):
        try:
            clear_arcpy_caches()  # Clear caches before each attempt
            shutil.rmtree(gdb_path, onerror=handle_remove_readonly)

            if not gdb_path.exists():
                logging.info(f"Successfully removed geodatabase (attempt {attempt + 1})")
                return True

        except Exception as e:
            logging.debug(f"Attempt {attempt + 1} failed: {e}")

        if attempt < max_attempts - 1:
            wait_time = (attempt + 1) * 0.5  # Increasing delays
            logging.debug(f"Waiting {wait_time}s before retry...")
            time.sleep(wait_time)

    # Step 4: Try rename strategy as fallback
    try:
        clear_arcpy_caches()
        timestamp = int(time.time())
        temp_path = gdb_path.with_name(f"{gdb_path.name}.{timestamp}.old")

        logging.debug(f"Attempting to rename to: {temp_path}")
        gdb_path.rename(temp_path)

        # Try to remove the renamed directory in background
        try:
            shutil.rmtree(temp_path, ignore_errors=True)
            if not temp_path.exists():
                logging.info("Successfully removed renamed geodatabase")
            else:
                logging.warning(f"Renamed geodatabase to {temp_path} (manual cleanup needed)")
        except Exception:
            logging.warning(f"Geodatabase renamed to {temp_path} (remove manually when possible)")

        return True

    except Exception as rename_error:
        logging.error(f"Rename strategy failed: {rename_error}")

    # Step 5: Final attempt - clear contents only
    try:
        clear_arcpy_caches()

        # Remove all contents recursively
        for item in gdb_path.rglob("*"):
            if item.is_file():
                with contextlib.suppress(Exception):
                    item.chmod(stat.S_IWRITE)
                    item.unlink(missing_ok=True)

        # Try to remove empty directories
        for item in sorted(gdb_path.rglob("*"), key=str, reverse=True):
            if item.is_dir() and item != gdb_path:
                with contextlib.suppress(Exception):
                    item.rmdir()

        # Finally try to remove the main directory
        try:
            gdb_path.rmdir()
            logging.info("Successfully cleared geodatabase directory")
            return True
        except Exception:
            logging.warning("Geodatabase contents cleared but directory remains")
            return False  # Contents cleared but directory still exists

    except Exception as final_error:
        logging.error(f"Final cleanup failed: {final_error}")
        return False


def create_clean_staging_gdb(staging_gdb_path):
    """Create a fresh staging geodatabase."""
    staging_path = Path(staging_gdb_path).resolve()
    staging_dir = staging_path.parent
    gdb_name = staging_path.name

    # Ensure parent directory exists
    staging_dir.mkdir(parents=True, exist_ok=True)

    # Clear any ArcPy workspace references
    clear_arcpy_caches()

    try:
        import arcpy  # Lazy import
        logging.info(f"Creating staging geodatabase: {staging_path}")
        arcpy.management.CreateFileGDB(str(staging_dir), gdb_name)

        if staging_path.exists():
            logging.info("Staging geodatabase created successfully")
            return True
        else:
            logging.error("Geodatabase creation appeared to succeed but file doesn't exist")
            return False

    except Exception as e:
        logging.error(f"Failed to create staging geodatabase: {e}")
        return False


# Download flow extracted for clarity
def _run_download(cfg, args):
    logging.info("Starting download process...")

    # Optional source filters
    sources = cfg["sources"]
    if args.authority:
        sources = [s for s in sources if s.get("authority") == args.authority]
    if args.type:
        sources = [s for s in sources if s.get("type") == args.type]

    filtered_cfg = cfg.copy()
    filtered_cfg["sources"] = sources

    use_unified = bool(cfg.get("use_unified_downloader", False))
    if use_unified:
        from etl import download as unified
        unified.run(filtered_cfg, authority=args.authority, type=args.type)
    else:
        from etl import download_atom, download_http, download_ogc, download_wfs

        # Check for simplified REST downloader
        use_simple_rest = bool(cfg.get("use_simplified_rest", False))
        if use_simple_rest:
            from etl import download_rest_simple as download_rest
            logging.info("Using simplified REST downloader")
        else:
            from etl import download_rest
            logging.info("Using full REST downloader")

        download_http.run(filtered_cfg)
        download_atom.run(filtered_cfg)
        download_ogc.run(filtered_cfg)
        download_wfs.run(filtered_cfg)
        download_rest.run(filtered_cfg)

    logging.info("Starting staging process...")
    use_simplified = bool(filtered_cfg.get("use_simplified_staging", False))
    if use_simplified:
        from etl.stage_simple import stage_all_downloads
        logging.info("Using simplified staging module")
    else:
        from etl.stage_files import stage_all_downloads
        logging.info("Using full staging module")
    stage_all_downloads(filtered_cfg)

    from etl.monitoring import get_error_patterns, log_pipeline_summary, save_pipeline_metrics
    log_pipeline_summary()

    metrics_file = Path("logs") / f"pipeline_metrics_{Path().resolve().name}_{int(time.time())}.json"
    save_pipeline_metrics(metrics_file)

    patterns = get_error_patterns()
    if patterns['recursion_errors']:
        logging.warning(f"Detected recursion errors in: {patterns['recursion_errors']}")
    if patterns['timeout_errors']:
        logging.warning(f"Detected timeout errors in: {patterns['timeout_errors']}")

    logging.info("Download process finished.")


# Generic step runner to avoid duplicate log lines
def _run_step(start_msg, runner, cfg, end_msg):
    logging.info(start_msg)
    runner(cfg)
    logging.info(end_msg)


def main():
    """Run the ETL pipeline with improved geodatabase management."""
    # Set increased recursion limit to handle deeply nested API responses
    sys.setrecursionlimit(3000)

    # 1) absolutely no logging.basicConfig here
    Path("logs").mkdir(exist_ok=True)  # safe to prep early

    # Parse arguments first to get config path
    p = argparse.ArgumentParser()
    p.add_argument("--config", default=None, help="Path to config.yaml")
    p.add_argument("--sources", default=None, help="Path to sources.yaml")
    p.add_argument("--download", action="store_true")
    p.add_argument("--process", action="store_true")
    p.add_argument("--load_sde", action="store_true")
    p.add_argument("--authority", help="Filter by authority")
    p.add_argument("--type", help="Filter by source type")
    args = p.parse_args()

    # Load configuration first
    try:
        cfg = load_config(args.config, args.sources)
    except ConfigError as e:
        # Use basic logging for config errors since we haven't configured logging yet
        print(f"Config error: {e}", file=sys.stderr)
        raise SystemExit(f"Config error: {e}") from e

    # 2) configure unified logging
    from etl.logging import setup_pipeline_logging
    # Map old YAML options if present; default to console INFO and file logs to logs/etl.log
    log_cfg = cfg.get("logging") or {}
    console_level = (log_cfg.get("level") or log_cfg.get("console_level") or "INFO").upper()
    file_path = None
    file_cfg = log_cfg.get("file") or {}
    if file_cfg.get("enabled", True):
        file_name = file_cfg.get("name") or "etl.log"
        file_path = Path("logs") / file_name
        file_level = (file_cfg.get("level") or "DEBUG").upper()
    else:
        file_level = "DEBUG"
    setup_pipeline_logging(console_level=console_level, file_path=file_path, file_level=file_level)

    # Suppress noisy urllib3 retry WARNINGs (e.g., connectionpool Retrying ...)
    for name in (
        "urllib3",
        "urllib3.connectionpool",
        "requests.packages.urllib3",
        "requests.packages.urllib3.connectionpool",
    ):
        logging.getLogger(name).setLevel(logging.ERROR)

    # 3) proceed with ETL; all modules just use logging.getLogger(__name__)
    logging.info("Starting ETL process...")

    ensure_workspaces(cfg)

    # Handle staging geodatabase cleanup and creation
    if cfg.get("cleanup_staging_before_run", False):
        staging_gdb_path = Path(cfg["workspaces"]["staging_gdb"]).resolve()

        # Remove existing geodatabase
        success = remove_geodatabase_safely(staging_gdb_path)
        if not success:
            logging.warning("Geodatabase removal had issues, but continuing...")

        # Create fresh geodatabase
        if not create_clean_staging_gdb(staging_gdb_path):
            raise SystemExit("Failed to create staging geodatabase")

    do_all = not any((args.download, args.process, args.load_sde))

    if args.download or do_all:
        _run_download(cfg, args)

    if args.process or do_all:
        from etl import process
        _run_step("Starting processing step...", process.run, cfg, "Processing step finished.")

    if args.load_sde or do_all:
        from etl import load_sde
        _run_step("Starting SDE loading process...", load_sde.run, cfg, "SDE loading process finished.")

    logging.info("ETL process finished successfully.")


if __name__ == "__main__":
    main()
