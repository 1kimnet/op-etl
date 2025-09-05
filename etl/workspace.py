"""
Workspace and geodatabase management for OP-ETL.
"""

import contextlib
import logging
import os
import shutil
import stat
import time
from pathlib import Path


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

def ensure_gdb_exists(gdb_path: str) -> None:
    """Ensure staging geodatabase exists."""
    gdb_path_obj = Path(gdb_path)

    if not gdb_path_obj.exists():
        gdb_path_obj.parent.mkdir(parents=True, exist_ok=True)
        try:
            import arcpy
            arcpy.management.CreateFileGDB(
                str(gdb_path_obj.parent),
                gdb_path_obj.name
            )
        except Exception as e:
            logging.error(f"[STAGE] Failed to create staging GDB: {e}")
        logging.info(f"[STAGE] Created staging GDB: {gdb_path}")


def clear_staging_gdb(gdb_path: str) -> None:
    """Clear all feature classes from staging GDB."""
    try:
        # Use arcpy.da.Walk to list feature classes without changing workspace
        feature_classes = []
        import arcpy
        for dirpath, dirnames, filenames in arcpy.da.Walk(gdb_path, datatype="FeatureClass"):
            feature_classes.extend(filenames)

        # Delete each feature class
        for fc in feature_classes:
            try:
                fc_path = f"{gdb_path}/{fc}"
                if arcpy.Exists(fc_path):
                    arcpy.management.Delete(fc_path)
            except Exception as e:
                logging.debug(f"[STAGE] Failed to delete {fc}: {e}")

        logging.info(f"[STAGE] Cleared {len(feature_classes)} feature classes from staging")

    except Exception as e:
        logging.warning(f"[STAGE] Failed to clear staging GDB: {e}")
