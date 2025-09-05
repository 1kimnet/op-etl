import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _load_processed_list(staging_gdb: Path) -> List[str]:
    """Load the list of successfully processed feature classes."""
    processed_file = staging_gdb.parent / "processed_feature_classes.json"
    if not processed_file.exists():
        logging.warning("[LOAD] No processed feature classes list found - will load all feature classes")
        return []
    try:
        with open(processed_file, 'r') as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        logging.warning(f"[LOAD] Failed to load processed feature classes list ({type(e).__name__}): {e} - will load all feature classes")
        return []


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
        logging.error(f"[LOAD] Cannot access staging GDB: {e}")
    return feature_classes


def _filter_feature_classes(feature_classes: List[Tuple[Path, str]], processed_list: List[str]) -> List[Tuple[Path, str]]:
    """Filter feature classes to only include those in the processed list."""
    if not processed_list:
        return feature_classes

    original_count = len(feature_classes)
    filtered_fcs = [(fc_path, name) for fc_path, name in feature_classes if name in processed_list]
    excluded_count = original_count - len(filtered_fcs)

    if excluded_count > 0:
        excluded_names = [name for _, name in feature_classes if name not in processed_list]
        logging.info(f"[LOAD] Excluding {excluded_count} feature classes that were not successfully processed (no regional data)")
        logging.info(f"[LOAD] Excluded feature classes: {excluded_names}")

    logging.info(f"[LOAD] Loading {len(filtered_fcs)} feature classes with regional data")
    return filtered_fcs


def _resolve_sde_destination(sde_conn: Path, dataset_name: Optional[str], fc_name: str, template_fc: Path) -> Path:
    """Return destination FC path under SDE, inside feature dataset if available."""
    import arcpy
    if dataset_name:
        dataset_path = sde_conn / dataset_name
        if not arcpy.Exists(str(dataset_path)):
            try:
                sr = arcpy.Describe(str(template_fc)).spatialReference
                arcpy.management.CreateFeatureDataset(str(sde_conn), dataset_name, sr)
            except Exception as e:
                logging.warning(f"[LOAD] Could not create dataset {dataset_name}: {e}")
                return sde_conn / fc_name
        return dataset_path / fc_name
    return sde_conn / fc_name


def _create_sde_fc(template_fc: Path, dest_fc: Path):
    """Create feature class in SDE using staging template."""
    import arcpy
    try:
        desc = arcpy.Describe(str(template_fc))
        arcpy.management.CreateFeatureclass(
            out_path=str(dest_fc.parent),
            out_name=dest_fc.name,
            geometry_type=desc.shapeType,
            template=str(template_fc),
            spatial_reference=desc.spatialReference
        )
    except Exception as e:
        logging.error(f"[LOAD] Failed to create SDE feature class: {e}")
        raise


def _load_to_sde(src_fc: Path, dest_fc: Path) -> bool:
    """Load feature class to SDE with truncate-and-load strategy."""
    import arcpy
    try:
        if arcpy.Exists(str(dest_fc)):
            logging.info(f"[LOAD] Truncating existing SDE feature class: {dest_fc.name}")
            arcpy.management.TruncateTable(str(dest_fc))
        else:
            logging.info(f"[LOAD] Creating new SDE feature class: {dest_fc.name}")
            _create_sde_fc(src_fc, dest_fc)

        logging.info(f"[LOAD] Appending data to {dest_fc.name}")
        arcpy.management.Append(
            inputs=str(src_fc),
            target=str(dest_fc),
            schema_type="NO_TEST"
        )
        return True
    except arcpy.ExecuteError:
        logging.error(f"[LOAD] Failed to load {dest_fc.name}: {arcpy.GetMessages(2)}")
        return False
    except Exception as e:
        logging.error(f"[LOAD] An unexpected error occurred while loading {dest_fc.name}: {e}")
        return False


def run(cfg: Dict[str, Any]):
    """Load all staged feature classes to SDE."""
    import arcpy  # lazy import
    staging_gdb = Path(cfg["workspaces"]["staging_gdb"])
    sde_conn = cfg["workspaces"].get("sde_conn")

    if not sde_conn:
        logging.warning("[LOAD] No SDE connection configured")
        return

    sde_conn_path = Path(sde_conn)
    processed_list = _load_processed_list(staging_gdb)
    feature_classes = _get_feature_classes(staging_gdb)

    if not feature_classes:
        logging.info("[LOAD] No feature classes found in staging")
        return

    feature_classes_to_load = _filter_feature_classes(feature_classes, processed_list)
    loaded_count = 0

    for src_fc_path, fc_name in feature_classes_to_load:
        authority = fc_name.split('_', 1)[0].upper() if '_' in fc_name else None
        dataset_name = f"Underlag_{authority}" if authority else None
        base_name = fc_name.split('_', 1)[1] if '_' in fc_name else fc_name
        clean_fc_name = Path(base_name).stem

        dest_fc = _resolve_sde_destination(sde_conn_path, dataset_name, clean_fc_name, src_fc_path)

        try:
            if not arcpy.Exists(str(src_fc_path)):
                continue

            if _load_to_sde(src_fc_path, dest_fc):
                loaded_count += 1
                logging.info(f"[LOAD] ✓ {fc_name}")
            else:
                logging.warning(f"[LOAD] ✗ {fc_name} failed")

        except Exception as e:
            logging.error(f"[LOAD] ✗ {fc_name}: {e}")

    logging.info(f"[LOAD] Loaded {loaded_count} feature classes to SDE")