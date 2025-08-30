import argparse
import logging
import os
import shutil
import stat
import sys
import time
from pathlib import Path

import arcpy

from etl.config import ConfigError, load_config
from etl.paths import ensure_workspaces


def clear_arcpy_caches():
    """Clear ArcPy internal caches and reset workspace to avoid locks."""
    try:
        # Reset workspace to system temp directory to avoid locks
        import tempfile
        temp_dir = tempfile.gettempdir()

        # Use setattr to avoid Pylance warnings about dynamic attributes
        try:
            setattr(arcpy.env, 'workspace', str(temp_dir))
        except (AttributeError, TypeError):
            # If workspace assignment fails, try alternative approach
            pass

        try:
            setattr(arcpy.env, 'scratchWorkspace', str(temp_dir))
        except (AttributeError, TypeError):
            # If scratchWorkspace assignment fails, continue
            pass

        # Clear any cached connections
        try:
            arcpy.ClearWorkspaceCache_management()
        except Exception:
            # ClearWorkspaceCache might not be available in all ArcPy versions
            pass

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
        try:
            os.chmod(path, stat.S_IWRITE)
            func(path)
        except (OSError, PermissionError):
            pass  # Continue with other strategies

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
                try:
                    item.chmod(stat.S_IWRITE)
                    item.unlink(missing_ok=True)
                except Exception:
                    pass

        # Try to remove empty directories
        for item in sorted(gdb_path.rglob("*"), key=str, reverse=True):
            if item.is_dir() and item != gdb_path:
                try:
                    item.rmdir()
                except Exception:
                    pass

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
        raise SystemExit(f"Config error: {e}")

    # 2) now configure logging from YAML
    from etl.logging_config import setup_logging
    setup_logging(cfg.get("logging"))

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
