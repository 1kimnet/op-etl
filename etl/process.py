import contextlib
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _get_feature_classes(staging_gdb: Path) -> List[Tuple[Path, str]]:
    """List all feature classes in the staging geodatabase."""
    import arcpy
    feature_classes = []
    try:
        for dirpath, _, filenames in arcpy.da.Walk(str(staging_gdb), datatype="FeatureClass"):
            for name in filenames:
                fc_path = Path(dirpath) / name
                feature_classes.append((fc_path, name))
    except Exception as e:
        logging.error(f"[PROCESS] Cannot access staging GDB: {e}")
    return feature_classes


def _save_processed_list(staging_gdb: Path, processed_list: List[str]):
    """Save the list of successfully processed feature classes."""
    processed_file = staging_gdb.parent / "processed_feature_classes.json"
    try:
        with open(processed_file, 'w', encoding='utf-8') as f:
            json.dump(processed_list, f, indent=2, ensure_ascii=False)
        logging.info(f"[PROCESS] Saved {len(processed_list)} successfully processed feature classes to {processed_file}")
    except IOError as e:
        logging.warning(f"[PROCESS] Failed to save processed feature classes list: {e}")


def _clip_feature_class(fc_path: Path, aoi_fc: Path, out_fc: Path) -> bool:
    """Clip a feature class to the AOI."""
    import arcpy
    try:
        logging.debug(f"[PROCESS] Clipping {fc_path.name} to {aoi_fc.name}")
        arcpy.analysis.Clip(str(fc_path), str(aoi_fc), str(out_fc))
        clip_count = int(str(arcpy.management.GetCount(str(out_fc))[0]))
        if clip_count > 0:
            logging.debug(f"[PROCESS] Clipped {fc_path.name} to {clip_count} features")
            return True
        else:
            logging.info(f"[PROCESS] No features in AOI for {fc_path.name}")
            with contextlib.suppress(Exception):
                arcpy.management.Delete(str(out_fc))
            return False
    except Exception as e:
        logging.error(f"[PROCESS] Clipping failed for {fc_path.name}: {e}")
        return False


def _project_feature_class(fc_path: Path, target_wkid: int, out_fc: Path) -> bool:
    """Reproject a feature class to the target WKID."""
    import arcpy
    try:
        desc = arcpy.Describe(str(fc_path))
        current_wkid = desc.spatialReference.factoryCode
        if current_wkid == target_wkid:
            logging.debug(f"[PROCESS] {fc_path.name} is already in target SR EPSG:{target_wkid}")
            return False

        logging.debug(f"[PROCESS] Reprojecting {fc_path.name} from EPSG:{current_wkid} to EPSG:{target_wkid}")
        target_sr = arcpy.SpatialReference(target_wkid)
        transform = "WGS_1984_To_SWEREF99" if current_wkid == 4326 and target_wkid == 3010 else None
        arcpy.management.Project(str(fc_path), str(out_fc), target_sr, transform)
        return True
    except Exception as e:
        logging.warning(f"[PROCESS] Reprojection failed for {fc_path.name}: {e}")
        return False


def process_feature_class(fc_path: Path, aoi_fc: Optional[Path], target_wkid: Optional[int]) -> bool:
    """Process a feature class with clipping and reprojection."""
    import arcpy
    with arcpy.EnvManager(scratchWorkspace=str(Path(arcpy.env.scratchGDB))):
        current_fc = fc_path
        processed = False

        if aoi_fc:
            temp_clip = Path(arcpy.env.scratchGDB) / f"{fc_path.name}_clip"
            if _clip_feature_class(current_fc, aoi_fc, temp_clip):
                current_fc = temp_clip
                processed = True
            else:
                return False  # No features in AOI, so skip

        if target_wkid:
            temp_proj = Path(arcpy.env.scratchGDB) / f"{fc_path.name}_proj"
            if _project_feature_class(current_fc, target_wkid, temp_proj):
                current_fc = temp_proj
                processed = True

        if processed and current_fc != fc_path:
            try:
                arcpy.management.Delete(str(fc_path))
                arcpy.management.Rename(str(current_fc), str(fc_path))
            except Exception as e:
                logging.error(f"[PROCESS] Failed to replace original: {e}")
                return False

        return processed


def run(cfg: Dict[str, Any]):
    """Process all feature classes found in staging GDB."""
    import arcpy  # lazy import
    gp = cfg.get("geoprocess", {})
    if not gp.get("enabled"):
        logging.info("[PROCESS] Geoprocessing disabled")
        return

    staging_gdb = Path(cfg["workspaces"]["staging_gdb"])
    aoi = Path(gp["aoi_boundary"]) if gp.get("aoi_boundary") and arcpy.Exists(gp["aoi_boundary"]) else None
    target_wkid = gp.get("target_wkid") or gp.get("target_srid")

    if gp.get("aoi_boundary") and not aoi:
        logging.warning(f"[PROCESS] AOI boundary not found: {gp['aoi_boundary']}")

    feature_classes = _get_feature_classes(staging_gdb)
    if not feature_classes:
        logging.info("[PROCESS] No feature classes found in staging")
        return

    processed_count = 0
    successfully_processed: List[str] = []

    for fc_path, fc_name in feature_classes:
        try:
            if process_feature_class(fc_path, aoi, target_wkid):
                processed_count += 1
                successfully_processed.append(fc_name)
                logging.info(f"[PROCESS] ✓ {fc_name}")
            elif aoi:
                logging.info(f"[PROCESS] ⤬ {fc_name} (no features within AOI – skipped)")
            else:
                logging.info(f"[PROCESS] ⤬ {fc_name} (no processing applied – skipped)")
        except Exception as e:
            logging.error(f"[PROCESS] ✗ {fc_name}: {e}")

    if aoi:
        _save_processed_list(staging_gdb, successfully_processed)
    else:
        processed_file = staging_gdb.parent / "processed_feature_classes.json"
        with contextlib.suppress(Exception):
            if processed_file.exists():
                processed_file.unlink()
                logging.info("[PROCESS] AOI disabled; removed existing processed feature classes list")

    logging.info(f"[PROCESS] Processed {processed_count} feature classes")