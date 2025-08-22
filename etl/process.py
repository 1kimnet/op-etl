# Replace etl/process.py with this simplified version:

import logging
import arcpy
from typing import Optional

def run(cfg):
    """Process all feature classes found in staging GDB."""
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
    feature_classes = []
    try:
        if not arcpy.Exists(staging_gdb):
            logging.error(f"[PROCESS] Staging GDB not found: {staging_gdb}")
            return
            
        for dirpath, dirnames, filenames in arcpy.da.Walk(staging_gdb, datatype="FeatureClass"):
            feature_classes.extend(filenames)
    except Exception as e:
        logging.error(f"[PROCESS] Cannot access staging GDB: {e}")
        return

    if not feature_classes:
        logging.info("[PROCESS] No feature classes found in staging")
        return

    processed_count = 0

    for fc_name in feature_classes:
        fc_path = f"{staging_gdb}/{fc_name}"

        try:
            if not arcpy.Exists(fc_path):
                continue

            processed = process_feature_class(fc_path, aoi, target_wkid)
            if processed:
                processed_count += 1
                logging.info(f"[PROCESS] ✓ {fc_name}")
            else:
                logging.info(f"[PROCESS] - {fc_name} (no processing needed)")

        except Exception as e:
            logging.error(f"[PROCESS] ✗ {fc_name}: {e}")

    logging.info(f"[PROCESS] Processed {processed_count} feature classes")

def process_feature_class(fc_path: str, aoi_fc: Optional[str] = None, target_wkid: Optional[int] = None) -> bool:
    """Process a single feature class with clipping and reprojection."""
    needs_processing = False
    temp_fcs = []
    current_fc = fc_path

    try:
        # Check if feature class has any features before processing
        feature_count = int(arcpy.management.GetCount(current_fc)[0])
        if feature_count == 0:
            logging.info(f"[PROCESS] Skipping empty feature class: {fc_path}")
            return False

        # Apply AOI clipping if configured and AOI exists
        if aoi_fc and arcpy.Exists(aoi_fc):
            try:
                temp_clip = f"{fc_path}_temp_clip"
                logging.debug(f"[PROCESS] Clipping {fc_path} with AOI {aoi_fc}")
                arcpy.analysis.Clip(current_fc, aoi_fc, temp_clip)
                
                # Check if clipping produced any features
                clip_count = int(arcpy.management.GetCount(temp_clip)[0])
                if clip_count > 0:
                    temp_fcs.append(temp_clip)
                    current_fc = temp_clip
                    needs_processing = True
                    logging.debug(f"[PROCESS] Clipped {feature_count} -> {clip_count} features")
                else:
                    logging.info(f"[PROCESS] Clipping resulted in no features for {fc_path}")
                    # Clean up empty result
                    if arcpy.Exists(temp_clip):
                        arcpy.management.Delete(temp_clip)
                    return False
            except Exception as e:
                logging.warning(f"[PROCESS] Clipping failed for {fc_path}: {e}")
                # Continue without clipping if it fails

        # Apply reprojection if needed
        if target_wkid:
            try:
                desc = arcpy.Describe(current_fc)
                current_wkid = desc.spatialReference.factoryCode
                if current_wkid != target_wkid:
                    temp_proj = f"{fc_path}_temp_proj"
                    target_sr = arcpy.SpatialReference(target_wkid)
                    logging.debug(f"[PROCESS] Reprojecting {current_fc} from {current_wkid} to {target_wkid}")
                    arcpy.management.Project(current_fc, temp_proj, target_sr)
                    temp_fcs.append(temp_proj)
                    current_fc = temp_proj
                    needs_processing = True
            except Exception as e:
                logging.warning(f"[PROCESS] Reprojection failed for {fc_path}: {e}")
                # Skip reprojection if we can't determine current SRID

        # Replace original with processed version if processing was done
        if needs_processing and current_fc != fc_path:
            try:
                arcpy.management.Delete(fc_path)
                arcpy.management.Rename(current_fc, fc_path)
                # Remove renamed FC from cleanup list
                if current_fc in temp_fcs:
                    temp_fcs.remove(current_fc)
            except Exception as e:
                logging.error(f"[PROCESS] Failed to replace original feature class {fc_path}: {e}")
                return False

        return needs_processing

    except Exception as e:
        logging.error(f"Processing failed for {fc_path}: {e}")
        return False
    finally:
        # Cleanup temporary feature classes
        for temp_fc in temp_fcs:
            try:
                if arcpy.Exists(temp_fc):
                    arcpy.management.Delete(temp_fc)
            except Exception:
                pass  # Ignore cleanup errors