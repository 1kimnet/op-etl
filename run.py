import argparse
import logging
import os
import shutil
import stat
import sys
import time
from pathlib import Path

from etl.config import ConfigError, load_config
from etl.paths import ensure_workspaces


def _remove_geodatabase_safely(gdb_path):
    """
    Safely remove a geodatabase directory, handling lock files and permissions.

    Args:
        gdb_path (Path): Path to the geodatabase directory
    """
    def handle_remove_readonly(func, path, exc):
        """
        Error handler for shutil.rmtree to handle read-only and locked files.
        """
        if os.path.exists(path):
            # Try to make the file writable and remove it
            try:
                os.chmod(path, stat.S_IWRITE)
                func(path)
            except (OSError, PermissionError):
                # If it's still locked, log and continue
                logging.warning(f"Could not remove locked file: {path}")

    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            shutil.rmtree(gdb_path, onerror=handle_remove_readonly)
            if not gdb_path.exists():
                return  # Successfully removed
        except Exception as e:
            logging.warning(f"Attempt {attempt + 1}: Error removing geodatabase {gdb_path}: {e}")

        # If removal failed, try waiting a bit for locks to release
        if attempt < max_attempts - 1:
            time.sleep(1)

    # If we get here, direct removal failed. Try rename strategy
    try:
        temp_path = gdb_path.with_suffix(f".{int(time.time())}.bak")
        gdb_path.rename(temp_path)
        logging.info(f"Renamed geodatabase to: {temp_path}")

        # Try to remove the renamed directory
        try:
            shutil.rmtree(temp_path, onerror=handle_remove_readonly)
            logging.info("Successfully removed renamed geodatabase")
        except Exception:
            logging.warning(f"Could not remove {temp_path}, but it's renamed out of the way")

    except Exception as rename_error:
        logging.error(f"Could not rename geodatabase: {rename_error}")
        # As a last resort, try to clear the directory contents
        try:
            for item in gdb_path.iterdir():
                try:
                    if item.is_file():
                        item.chmod(stat.S_IWRITE)
                        item.unlink()
                    elif item.is_dir():
                        shutil.rmtree(item, ignore_errors=True)
                except Exception as item_error:
                    logging.warning(f"Could not remove item {item}: {item_error}")
            # Try to remove the now-empty directory
            gdb_path.rmdir()
            logging.info("Successfully cleared geodatabase directory contents")
        except Exception as clear_error:
            logging.error(f"Final cleanup attempt failed: {clear_error}")
            raise OSError(f"Could not remove geodatabase {gdb_path}. Manual cleanup may be required.")


def main():
    """Run the ETL pipeline with fixed downloader modules."""
    # Set increased recursion limit to handle deeply nested API responses
    sys.setrecursionlimit(3000)

    # Ensure logs directory exists
    Path("logs").mkdir(exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler("logs/etl.log", encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    logging.info("Starting ETL process...")

    p = argparse.ArgumentParser()
    p.add_argument("--config", default=None, help="Path to config.yaml")
    p.add_argument("--sources", default=None, help="Path to sources.yaml")
    p.add_argument("--download", action="store_true")
    p.add_argument("--process", action="store_true")
    p.add_argument("--load_sde", action="store_true")
    p.add_argument("--authority", help="Filter by authority")
    p.add_argument("--type", help="Filter by source type")
    args = p.parse_args()

    try:
        cfg = load_config(args.config, args.sources)
    except ConfigError as e:
        logging.error(f"Config error: {e}")
        raise SystemExit(f"Config error: {e}")

    ensure_workspaces(cfg)

    # Clean and recreate staging geodatabase if configured
    if cfg.get("cleanup_staging_before_run", False):
        staging_gdb_path = Path(cfg["workspaces"]["staging_gdb"]).resolve()
        if staging_gdb_path.exists():
            logging.info(f"Cleaning staging geodatabase: {staging_gdb_path}")
            _remove_geodatabase_safely(staging_gdb_path)
            logging.info("Staging geodatabase cleaned")

        # Recreate empty staging geodatabase
        logging.info(f"Creating new staging geodatabase: {staging_gdb_path}")
        staging_dir = staging_gdb_path.parent
        staging_gdb_name = staging_gdb_path.name

        arcpy.management.CreateFileGDB(str(staging_dir), staging_gdb_name)
        logging.info("New staging geodatabase created")

    do_all = not any((args.download, args.process, args.load_sde))

    if args.download or do_all:
        logging.info("Starting download process...")

        # Apply filters if specified
        sources = cfg["sources"]
        if args.authority:
            sources = [s for s in sources if s.get("authority") == args.authority]
        if args.type:
            sources = [s for s in sources if s.get("type") == args.type]

        # Create filtered config
        filtered_cfg = cfg.copy()
        filtered_cfg["sources"] = sources

        # Import downloaders
        from etl import download_atom, download_http, download_ogc, download_rest, download_wfs

        # Run each downloader separately
        # Each module now handles its own source filtering
        download_http.run(filtered_cfg)
        download_atom.run(filtered_cfg)
        download_ogc.run(filtered_cfg)
        download_wfs.run(filtered_cfg)
        download_rest.run(filtered_cfg)

        # Stage downloaded files
        logging.info("Starting staging process...")
        from etl.stage_files import stage_all_downloads
        stage_all_downloads(filtered_cfg)

        # Log monitoring summary
        from etl.monitoring import get_error_patterns, log_pipeline_summary, save_pipeline_metrics
        log_pipeline_summary()

        # Save metrics to file
        metrics_file = Path("logs") / f"pipeline_metrics_{Path().resolve().name}_{int(time.time())}.json"
        save_pipeline_metrics(metrics_file)

        # Check for error patterns
        patterns = get_error_patterns()
        if patterns['recursion_errors']:
            logging.warning(f"Detected recursion errors in: {patterns['recursion_errors']}")
        if patterns['timeout_errors']:
            logging.warning(f"Detected timeout errors in: {patterns['timeout_errors']}")

        logging.info("Download process finished.")

    if args.process or do_all:
        logging.info("Starting processing step...")
        from etl import process
        process.run(cfg)
        logging.info("Processing step finished.")

    if args.load_sde or do_all:
        logging.info("Starting SDE loading process...")
        from etl import load_sde
        load_sde.run(cfg)
        logging.info("SDE loading process finished.")

    logging.info("ETL process finished successfully.")


if __name__ == "__main__":
    main()
