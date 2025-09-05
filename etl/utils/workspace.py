"""
Workspace and geodatabase management utilities for OP-ETL.

Provides comprehensive functions for managing ArcPy workspaces, geodatabases,
and cleaning up locks and caches.
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
    gdb_path = Path(gdb_path)
    
    if not gdb_path.exists():
        logging.info(f"Geodatabase {gdb_path} does not exist, skipping removal")
        return True

    logging.info(f"Attempting to remove geodatabase: {gdb_path}")
    max_attempts = 3

    # Step 1: Clear ArcPy caches first
    clear_arcpy_caches()

    # Step 2: Try ArcPy Delete if available
    try:
        import arcpy
        if arcpy.Exists(str(gdb_path)):
            logging.debug("Attempting ArcPy Delete...")
            arcpy.management.Delete(str(gdb_path))
            
            if not gdb_path.exists():
                logging.info("Successfully removed geodatabase with ArcPy")
                return True
            else:
                logging.debug("ArcPy Delete did not remove directory")
    except Exception as e:
        logging.debug(f"ArcPy Delete failed: {e}")

    # Step 3: Try filesystem removal with multiple attempts
    for attempt in range(max_attempts):
        try:
            clear_arcpy_caches()
            
            # Make all files writable
            for root, dirs, files in os.walk(gdb_path):
                for file in files:
                    file_path = Path(root) / file
                    with contextlib.suppress(Exception):
                        file_path.chmod(stat.S_IWRITE)
            
            # Remove the directory
            shutil.rmtree(gdb_path)
            
            if not gdb_path.exists():
                logging.info(f"Successfully removed geodatabase on attempt {attempt + 1}")
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
        with contextlib.suppress(Exception):
            gdb_path.rmdir()

        if not gdb_path.exists():
            logging.info("Successfully removed geodatabase by clearing contents")
            return True

        # If directory still exists but is empty, that's OK
        if not any(gdb_path.iterdir()):
            logging.info("Geodatabase directory exists but is empty")
            return True

    except Exception as e:
        logging.error(f"Content clearing failed: {e}")

    logging.error(f"Failed to completely remove geodatabase: {gdb_path}")
    return False


def create_clean_staging_gdb(staging_gdb_path):
    """Create a fresh staging geodatabase."""
    staging_gdb_path = Path(staging_gdb_path)
    
    try:
        # Lazy import to avoid issues in non-ArcPy environments
        import arcpy
        
        parent_dir = staging_gdb_path.parent
        gdb_name = staging_gdb_path.name
        
        # Ensure parent directory exists
        parent_dir.mkdir(parents=True, exist_ok=True)
        
        logging.info(f"Creating staging geodatabase: {staging_gdb_path}")
        arcpy.management.CreateFileGDB(str(parent_dir), gdb_name)
        
        if staging_gdb_path.exists():
            logging.info("Successfully created staging geodatabase")
            return True
        else:
            logging.error("CreateFileGDB completed but geodatabase not found")
            return False
            
    except Exception as e:
        logging.error(f"Failed to create staging geodatabase: {e}")
        return False