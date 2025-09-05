# Replace etl/process.py with this simplified version:

import contextlib
import json
import logging
from pathlib import Path
from typing import Optional


def run(cfg):
    """Process all feature classes found in staging GDB."""
    import arcpy  # lazy import
    gp = cfg.get("geoprocess", {})
    if not gp.get("enabled"):
        logging.info("[PROCESS] Geoprocessing disabled")
        return

    staging_gdb = cfg["workspaces"]["staging_gdb"]
    aoi = gp.get("aoi_boundary")
    target_wkid = gp.get("target_wkid") or gp.get("target_srid")

    # Validate AOI boundary exists if configured
    if aoi and not arcpy.Exists(aoi):
        logging.warning(f"[PROCESS] AOI boundary not found: {aoi}")
        aoi = None  # Disable clipping if AOI doesn't exist

    # List actual feature classes in staging using arcpy.da.Walk
    # Collect tuples of (full_path, relative_path, name)
    feature_classes = []
    try:
        if not arcpy.Exists(staging_gdb):
            logging.error(f"[PROCESS] Staging GDB not found: {staging_gdb}")
            return

        for dirpath, dirnames, filenames in arcpy.da.Walk(staging_gdb, datatype="FeatureClass"):
            for name in filenames:
                fc_path = f"{dirpath}/{name}"
                # Build relative path inside the GDB (dataset/feature or just feature)
                rel_dir = dirpath[len(staging_gdb):] if str(dirpath).startswith(str(staging_gdb)) else ""
                rel_dir = str(rel_dir).strip("/\\")
                rel_path = f"{rel_dir}/{name}" if rel_dir else name
                feature_classes.append((fc_path, rel_path, name))
    except Exception as e:
        logging.error(f"[PROCESS] Cannot access staging GDB: {e}")
        return

    if not feature_classes:
        logging.info("[PROCESS] No feature classes found in staging")
        return

    processed_count = 0
    successfully_processed: list[str] = []  # Track relative paths that were successfully processed

    for fc_path, rel_fc_path, fc_name in feature_classes:

        try:
            if not arcpy.Exists(fc_path):
                continue

            if process_feature_class(fc_path, aoi, target_wkid):
                processed_count += 1
                successfully_processed.append(rel_fc_path)
                logging.info(f"[PROCESS] ✓ {fc_name}")
            elif aoi:
                logging.info(f"[PROCESS] ⤬ {fc_name} (no features within AOI – skipped)")
            else:
                logging.info(f"[PROCESS] ⤬ {fc_name} (no processing applied – skipped)")

        except Exception as e:
            logging.error(f"[PROCESS] ✗ {fc_name}: {e}")

    # Save list of successfully processed feature classes only when AOI is provided
    processed_file = Path(staging_gdb).parent / "processed_feature_classes.json"
    if aoi is not None:
        try:
            with open(processed_file, 'w') as f:
                json.dump(successfully_processed, f, indent=2)
            logging.info(f"[PROCESS] Saved {len(successfully_processed)} successfully processed feature classes to {processed_file}")
        except IOError as e:
            logging.warning(f"[PROCESS] Failed to save processed feature classes list: {e}")
    else:
        # AOI disabled: ensure no stale processed file exists
        try:
            if processed_file.exists():
                processed_file.unlink()
                logging.info("[PROCESS] AOI disabled; removed existing processed feature classes list")
        except Exception as e:
            logging.debug(f"[PROCESS] Could not remove processed list: {e}")

    logging.info(f"[PROCESS] Processed {processed_count} feature classes")

def process_feature_class(fc_path: str, aoi_fc: Optional[str] = None, target_wkid: Optional[int] = None) -> bool:
    """Process a feature class with clipping and reprojection to EPSG:3010."""
    import arcpy
    needs_processing = False
    temp_fcs = []
    current_fc = fc_path

    try:
        # Check feature count
        feature_count = int(str(arcpy.management.GetCount(current_fc)[0]))
        if feature_count == 0:
            logging.info(f"[PROCESS] Skipping empty feature class: {fc_path}")
            return False

        # Apply AOI clipping for Strängnäs area if configured
        if aoi_fc and arcpy.Exists(aoi_fc):
            try:
                temp_clip = f"{fc_path}_temp_clip"
                logging.debug("[PROCESS] Clipping to Strängnäs area")
                arcpy.analysis.Clip(current_fc, aoi_fc, temp_clip)

                clip_count = int(str(arcpy.management.GetCount(temp_clip)[0]))
                if clip_count > 0:
                    temp_fcs.append(temp_clip)
                    current_fc = temp_clip
                    needs_processing = True
                    logging.debug(f"[PROCESS] Clipped {feature_count} -> {clip_count} features")
                else:
                    logging.info(f"[PROCESS] No features in Strängnäs area for {fc_path}")
                    if arcpy.Exists(temp_clip):
                        arcpy.management.Delete(temp_clip)
                    return False
            except Exception as e:
                # If clipping fails while AOI is provided, stop further processing to avoid un-clipped data
                logging.error(f"[PROCESS] Clipping failed: {e}")
                return False

        # Project to SWEREF99 16 30 (EPSG:3010) if needed
        if target_wkid:
            try:
                desc = arcpy.Describe(current_fc)
                current_wkid = desc.spatialReference.factoryCode

                if current_wkid != target_wkid:
                    temp_proj = f"{fc_path}_temp_proj"
                    target_sr = arcpy.SpatialReference(target_wkid)

                    logging.debug(f"[PROCESS] Reprojecting from EPSG:{current_wkid} to EPSG:{target_wkid}")

                    # Simplified reprojection rule:
                    # If WGS84 (EPSG:4326) to SWEREF99 16 30 (EPSG:3010), use explicit transformation.
                    # Otherwise, rely on ArcPy defaults.
                    if current_wkid == 4326 and target_wkid == 3010:
                        transform = "WGS_1984_To_SWEREF99"
                        arcpy.management.Project(current_fc, temp_proj, target_sr, transform)
                    else:
                        arcpy.management.Project(current_fc, temp_proj, target_sr)
                    temp_fcs.append(temp_proj)
                    current_fc = temp_proj
                    needs_processing = True
                else:
                    logging.debug(f"[PROCESS] Already in target SR EPSG:{target_wkid}")

            except Exception as e:
                logging.warning(f"[PROCESS] Reprojection failed: {e}")

        # Replace original with processed version
        if needs_processing and current_fc != fc_path:
            try:
                arcpy.management.Delete(fc_path)
                arcpy.management.Rename(current_fc, fc_path)
                if current_fc in temp_fcs:
                    temp_fcs.remove(current_fc)
            except Exception as e:
                logging.error(f"[PROCESS] Failed to replace original: {e}")
                return False

        return needs_processing

    except Exception as e:
        logging.error(f"Processing failed for {fc_path}: {e}")
        return False
    finally:
        # Cleanup
        for temp_fc in temp_fcs:
            with contextlib.suppress(Exception):
                if arcpy.Exists(temp_fc):
                    arcpy.management.Delete(temp_fc)
