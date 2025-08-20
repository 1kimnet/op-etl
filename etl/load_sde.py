# Replace etl/load_sde.py with this simplified version:

import logging
import arcpy
from pathlib import Path

def run(cfg):
    """Load all staged feature classes to SDE."""
    staging_gdb = cfg["workspaces"]["staging_gdb"]
    sde_conn = cfg["workspaces"].get("sde_conn")

    if not sde_conn:
        logging.warning("[LOAD] No SDE connection configured")
        return

    # List actual feature classes in staging using arcpy.da.Walk
    feature_classes = []
    try:
        for dirpath, dirnames, filenames in arcpy.da.Walk(staging_gdb, datatype="FeatureClass"):
            feature_classes.extend(filenames)
    except Exception as e:
        logging.error(f"[LOAD] Cannot access staging GDB: {e}")
        return

    if not feature_classes:
        logging.info("[LOAD] No feature classes found in staging")
        return

    loaded_count = 0

    for fc_name in feature_classes:
        src_fc = f"{staging_gdb}/{fc_name}"
        dest_fc = f"{sde_conn}/{fc_name}"

        try:
            if not arcpy.Exists(src_fc):
                continue

            success = load_to_sde(src_fc, dest_fc, fc_name)
            if success:
                loaded_count += 1
                logging.info(f"[LOAD] ✓ {fc_name}")
            else:
                logging.warning(f"[LOAD] ✗ {fc_name} failed")

        except Exception as e:
            logging.error(f"[LOAD] Error loading {fc_name}: {e}")

    logging.info(f"[LOAD] Loaded {loaded_count} feature classes to SDE")

def load_to_sde(src_fc: str, dest_fc: str, fc_name: str) -> bool:
    """Load feature class to SDE with truncate-and-load strategy."""
    try:
        if arcpy.Exists(dest_fc):
            # Truncate existing data
            arcpy.management.TruncateTable(dest_fc)
        else:
            # Create new feature class from template
            create_sde_fc(src_fc, dest_fc)

        # Append data
        arcpy.management.Append(
            inputs=src_fc,
            target=dest_fc,
            schema_type="NO_TEST"
        )

        return True

    except Exception as e:
        logging.error(f"[LOAD] Failed to load {fc_name}: {e}")
        return False

def create_sde_fc(template_fc: str, dest_fc: str):
    """Create feature class in SDE using staging template."""
    try:
        desc = arcpy.Describe(template_fc)

        # Extract workspace and feature class name
        sde_workspace = str(Path(dest_fc).parent)
        fc_name = Path(dest_fc).name

        arcpy.management.CreateFeatureclass(
            out_path=sde_workspace,
            out_name=fc_name,
            geometry_type=desc.shapeType,
            template=template_fc,
            spatial_reference=desc.spatialReference
        )

    except Exception as e:
        logging.error(f"[LOAD] Failed to create SDE feature class: {e}")
        raise