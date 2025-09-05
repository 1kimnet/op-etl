import json
import logging
from pathlib import Path


def run(cfg):
    """Load all staged feature classes to SDE."""
    import arcpy  # lazy import
    staging_gdb = cfg["workspaces"]["staging_gdb"]
    sde_conn = cfg["workspaces"].get("sde_conn")

    if not sde_conn:
        logging.warning("[LOAD] No SDE connection configured")
        return

    # Load list of successfully processed feature classes
    processed_file = Path(staging_gdb).parent / "processed_feature_classes.json"
    successfully_processed = []

    try:
        if processed_file.exists():
            with open(processed_file, 'r') as f:
                successfully_processed = json.load(f)
            logging.info(f"[LOAD] Found {len(successfully_processed)} successfully processed feature classes")
        else:
            logging.warning("[LOAD] No processed feature classes list found - will load all feature classes")
    except (json.JSONDecodeError, IOError) as e:
        logging.warning(f"[LOAD] Failed to load processed feature classes list ({type(e).__name__}): {e} - will load all feature classes")

    # List actual feature classes in staging using arcpy.da.Walk
    # Collect tuples of (full_path, relative_path, name)
    feature_classes: list[tuple[str, str, str]] = []
    try:
        for dirpath, dirnames, filenames in arcpy.da.Walk(staging_gdb, datatype="FeatureClass"):
            for name in filenames:
                fc_path = f"{dirpath}/{name}"
                rel_dir = dirpath[len(staging_gdb):] if str(dirpath).startswith(str(staging_gdb)) else ""
                rel_dir = str(rel_dir).strip("/\\")
                rel_path = f"{rel_dir}/{name}" if rel_dir else name
                feature_classes.append((fc_path, rel_path, name))
    except Exception as e:
        logging.error(f"[LOAD] Cannot access staging GDB: {e}")
        return

    if not feature_classes:
        logging.info("[LOAD] No feature classes found in staging")
        return

    # Filter feature classes to only include successfully processed ones
    # Gate on file existence so filtering always applies when the list is present
    if processed_file.exists():
        original_count = len(feature_classes)
        excluded_feature_classes = [name for (_full, rel, name) in feature_classes if rel not in successfully_processed]
        feature_classes = [(full, rel, name) for (full, rel, name) in feature_classes if rel in successfully_processed]
        excluded_count = original_count - len(feature_classes)
        if excluded_count > 0:
            logging.info(f"[LOAD] Excluding {excluded_count} feature classes that were not successfully processed (no regional data)")
            logging.info(f"[LOAD] Excluded feature classes: {excluded_feature_classes}")
        logging.info(f"[LOAD] Loading {len(feature_classes)} feature classes with regional data")

    loaded_count = 0

    for src_fc, rel_fc, fc_name in feature_classes:

        # Determine target feature dataset by authority prefix (before first underscore)
        authority = fc_name.split('_', 1)[0].upper() if '_' in fc_name else None
        dataset_name = f"Underlag_{authority}" if authority else None

        # Strip the authority_ prefix and any file extension from the destination feature class name
        base_name = fc_name.split('_', 1)[1] if '_' in fc_name else fc_name
        clean_fc_name = Path(base_name).stem

        # Resolve destination path: prefer dataset if it exists or can be created
        dest_fc = resolve_sde_destination(sde_conn, dataset_name, clean_fc_name, src_fc)

        try:
            if not arcpy.Exists(src_fc):
                continue

            success = load_to_sde(src_fc, dest_fc, clean_fc_name)
            if success:
                loaded_count += 1
                logging.info(f"[LOAD] ✓ {fc_name}")
            else:
                logging.warning(f"[LOAD] ✗ {fc_name} failed")

        except Exception as e:
            logging.error(f"[LOAD] ✗ {fc_name}: {e}")

    logging.info(f"[LOAD] Loaded {loaded_count} feature classes to SDE")

def load_to_sde(src_fc: str, dest_fc: str, fc_name: str) -> bool:
    """Load feature class to SDE with truncate-and-load strategy."""
    import arcpy  # lazy import
    # Sanitize fc_name by removing file extension for SDE
    sde_fc_name = Path(fc_name).stem

    try:
        if arcpy.Exists(dest_fc):
            logging.info(f"[LOAD] Truncating existing SDE feature class: {sde_fc_name}")
            arcpy.management.TruncateTable(dest_fc)
        else:
            logging.info(f"[LOAD] Creating new SDE feature class: {sde_fc_name}")
            create_sde_fc(src_fc, dest_fc)

        # Append data
        logging.info(f"[LOAD] Appending data to {sde_fc_name}")
        arcpy.management.Append(
            inputs=src_fc,
            target=dest_fc,
            schema_type="NO_TEST"
        )

        return True

    except arcpy.ExecuteError:
        logging.error(f"[LOAD] Failed to load {sde_fc_name}: {arcpy.GetMessages(2)}")
        return False
    except Exception as e:
        logging.error(f"[LOAD] An unexpected error occurred while loading {sde_fc_name}: {e}")
        return False

def create_sde_fc(template_fc: str, dest_fc: str):
    """Create feature class in SDE using staging template."""
    try:
        import arcpy  # lazy import
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

def resolve_sde_destination(sde_conn: str, dataset_name: str | None, fc_name: str, template_fc: str) -> str:
    """Return destination FC path under SDE, inside feature dataset if available.

    Attempts to create missing dataset using template spatial reference if needed.
    Fallbacks to SDE root on failure.
    """
    try:
        import arcpy  # lazy import
        if dataset_name:
            dataset_path = f"{sde_conn}/{dataset_name}"
            if not arcpy.Exists(dataset_path):
                # Try to create feature dataset with same SR as template
                try:
                    sr = arcpy.Describe(template_fc).spatialReference
                    arcpy.management.CreateFeatureDataset(sde_conn, dataset_name, sr)
                except Exception as e:
                    # If creation fails, log and fallback to root
                    logging.warning(f"[LOAD] Could not create dataset {dataset_name}: {e}")
                    return f"{sde_conn}/{fc_name}"

            # If dataset exists now, place FC inside it
            return f"{sde_conn}/{dataset_name}/{fc_name}"

        # No dataset name determined
        return f"{sde_conn}/{fc_name}"

    except Exception as e:
        logging.warning(f"[LOAD] Dataset resolution failed, loading to root: {e}")
        return f"{sde_conn}/{fc_name}"
