"""
Simple, reliable staging system for OP-ETL.
Replace etl/stage_files.py with this implementation.
"""
import logging
import zipfile
import shutil
from pathlib import Path
import arcpy
import unicodedata
import re

def stage_all_downloads(cfg: dict) -> None:
    """
    Main staging function - discovers and imports all downloaded files.
    Call this instead of stage_files.ingest_downloads()
    """
    downloads_dir = Path(cfg['workspaces']['downloads'])
    gdb_path = cfg['workspaces']['staging_gdb']

    logging.info(f"[STAGE] Starting staging from {downloads_dir}")

    # Ensure staging GDB exists
    ensure_gdb_exists(gdb_path)

    # Clear staging GDB if configured
    if cfg.get('cleanup_staging_before_run', False):
        clear_staging_gdb(gdb_path)

    imported_count = 0

    # Process each authority directory
    for authority_dir in downloads_dir.iterdir():
        if not authority_dir.is_dir():
            continue

        authority_name = authority_dir.name.lower()
        logging.info(f"[STAGE] Processing {authority_name}")

        # Find all importable files
        files_found = discover_files(authority_dir)
        logging.info(f"[STAGE] Found {len(files_found)} files in {authority_name}")

        # Import each file
        for file_path in files_found:
            safe_name = create_safe_name(file_path, authority_name)
            success = import_file_to_staging(file_path, gdb_path, safe_name)

            if success:
                imported_count += 1
                logging.info(f"[STAGE] + {file_path.name} -> {safe_name}")
            else:
                logging.warning(f"[STAGE] ✗ Failed: {file_path.name}")

    logging.info(f"[STAGE] Completed: {imported_count} files imported to staging")

def discover_files(directory: Path) -> list[Path]:
    """Find all files we can import, with smart prioritization."""
    candidates = []

    # Search patterns in priority order
    patterns = [
        '*.gpkg',     # GeoPackage files (usually best quality)
        '*.geojson',  # GeoJSON (from REST/OGC/WFS)
        '*.shp',      # Shapefiles
        '*.zip'       # ZIP archives (may contain shapefiles/gpkg)
    ]

    for pattern in patterns:
        # Search recursively in directory
        found = list(directory.rglob(pattern))
        candidates.extend(found)

    # Remove duplicates and sort by modification time (newest first)
    unique_files = []
    seen_stems = set()

    for file_path in sorted(candidates, key=lambda p: p.stat().st_mtime, reverse=True):
        # Skip legacy paginated files like part_001.geojson
        fname = file_path.name.lower()
        if fname.endswith('.geojson') and fname.startswith('part_'):
            continue
        # Use file stem to avoid duplicate processing of same dataset
        stem_key = file_path.stem.lower()
        if stem_key not in seen_stems:
            unique_files.append(file_path)
            seen_stems.add(stem_key)

    return unique_files

def create_safe_name(file_path: Path, authority: str) -> str:
    """Create ArcPy-safe name with single authority prefix.

    Example: stem 'raa_raa_ri_kulturmiljovard_mb3kap6' with authority 'RAA'
    becomes 'raa_ri_kulturmiljovard_mb3kap6'.
    """
    norm_auth = make_arcpy_safe_name(authority)
    norm_stem = make_arcpy_safe_name(file_path.stem)

    # Strip repeated leading authority tokens from stem
    prefix = f"{norm_auth}_"
    while norm_stem.startswith(prefix):
        norm_stem = norm_stem[len(prefix):]

    if not norm_stem:
        norm_stem = "data"

    return make_arcpy_safe_name(f"{norm_auth}_{norm_stem}")

def make_arcpy_safe_name(name: str, max_length: int = 60) -> str:
    """Create bulletproof ArcPy-compatible feature class names."""
    if not name:
        return "unnamed_fc"

    # Normalize unicode and remove all accents/special chars
    normalized = unicodedata.normalize('NFD', name)
    ascii_name = ''.join(c for c in normalized if unicodedata.category(c) != 'Mn')

    # Ensure ASCII only
    try:
        ascii_name = ascii_name.encode('ascii', 'ignore').decode('ascii')
    except Exception:
        ascii_name = "converted_name"

    # Apply ArcPy naming rules
    clean = ascii_name.lower().strip()
    clean = re.sub(r'[^a-z0-9]', '_', clean)  # Only letters, numbers, underscores
    clean = re.sub(r'_+', '_', clean)         # Collapse multiple underscores
    clean = clean.strip('_')                  # Remove leading/trailing underscores

    # Must start with letter (ArcPy requirement)
    if clean and clean[0].isdigit():
        clean = f"fc_{clean}"

    # Handle empty results
    if not clean or len(clean) < 1:
        clean = "default_fc"

    # Truncate to max length
    clean = clean[:max_length]

    # Handle Windows reserved words
    reserved = {'con', 'prn', 'aux', 'nul', 'com1', 'com2', 'lpt1', 'lpt2'}
    if clean.lower() in reserved:
        clean = f"{clean}_data"

    return clean

def import_file_to_staging(file_path: Path, gdb_path: str, staging_name: str) -> bool:
    """Import any supported file type to staging GDB."""
    out_fc = f"{gdb_path.replace(chr(92), '/')}/{staging_name}"

    # Clean up existing feature class (best effort)
    try:
        if arcpy.Exists(out_fc):
            arcpy.management.Delete(out_fc)
    except Exception:
        pass

    try:
        # Route to appropriate importer based on file type
        suffix = file_path.suffix.lower()

        if suffix == '.gpkg':
            return import_gpkg(file_path, out_fc)
        elif suffix == '.geojson':
            return import_geojson(file_path, out_fc)
        elif suffix == '.shp':
            return import_shapefile(file_path, out_fc)
        elif suffix == '.zip':
            return import_zip(file_path, out_fc)
        else:
            logging.debug(f"[STAGE] Unsupported file type: {suffix}")
            return False

    except Exception as e:
        logging.error(f"[STAGE] Import failed for {file_path.name}: {e}")
        return False

def import_gpkg(gpkg_path: Path, out_fc: str) -> bool:
    """Import GPKG using actual layer discovery."""
    try:
        # Discover actual layers in the GPKG
        layers = discover_gpkg_layers(gpkg_path)

        if not layers:
            logging.error(f"[STAGE] No layers found in {gpkg_path.name}")
            return False

        # Import first valid layer
        for layer_name in layers:
            try:
                # Use backslash format for ArcPy GPKG references
                layer_ref = f"{gpkg_path}\\{layer_name}"

                arcpy.conversion.FeatureClassToFeatureClass(
                    layer_ref,
                    str(Path(out_fc).parent),
                    Path(out_fc).name
                )

                logging.debug(f"[STAGE] Imported GPKG layer: {layer_name}")
                return True

            except Exception as e:
                logging.debug(f"[STAGE] Layer {layer_name} failed: {e}")
                continue

        logging.error(f"[STAGE] No importable layers in {gpkg_path.name}")
        return False

    except Exception as e:
        logging.error(f"[STAGE] GPKG import failed: {e}")
        return False

def discover_gpkg_layers(gpkg_path: Path) -> list[str]:
    """Discover actual layer names in a GPKG file."""
    layers = []

    try:
        # Method 1: Use arcpy.da.Walk (most reliable)
        for dirpath, dirnames, filenames in arcpy.da.Walk(str(gpkg_path), datatype="FeatureClass"):
            for filename in filenames:
                # Clean layer name (remove main. prefix if present)
                clean_name = filename.replace("main.", "")
                if clean_name not in layers:
                    layers.append(clean_name)

        # Method 2: Use arcpy.Describe as fallback
        if not layers:
            try:
                desc = arcpy.Describe(str(gpkg_path))
                if hasattr(desc, 'children'):
                    for child in desc.children:
                        if hasattr(child, 'name'):
                            clean_name = child.name.replace("main.", "")
                            if clean_name not in layers:
                                layers.append(clean_name)
            except Exception:
                pass  # Fallback failed, continue with empty list

        logging.debug(f"[STAGE] GPKG {gpkg_path.name} layers: {layers}")
        return layers

    except Exception as e:
        logging.debug(f"[STAGE] Failed to discover GPKG layers: {e}")
        return []

def import_shapefile(shp_path: Path, out_fc: str) -> bool:
    """Import shapefile directly."""
    try:
        arcpy.conversion.FeatureClassToFeatureClass(
            str(shp_path),
            str(Path(out_fc).parent),
            Path(out_fc).name
        )
        return True
    except Exception as e:
        logging.error(f"[STAGE] Shapefile import failed: {e}")
        return False

def import_geojson(geojson_path: Path, out_fc: str) -> bool:
    """Import GeoJSON via ArcPy JSONToFeatures. Handles CRS if present."""
    try:
        # ArcPy JSONToFeatures expects Esri JSON. However, since ArcGIS Pro 2.9+, it accepts GeoJSON too.
        # Use JSONToFeatures directly; Pro 3.3 environment (per docs) supports GeoJSON.
        arcpy.conversion.JSONToFeatures(str(geojson_path), out_fc)
        return True
    except Exception as e:
        logging.error(f"[STAGE] GeoJSON import failed: {e}")
        return False

def import_zip(zip_path: Path, out_fc: str) -> bool:
    """Extract ZIP and import first valid dataset."""
    extract_dir = zip_path.parent / f"_extract_{zip_path.stem}"

    try:
        # Extract ZIP contents
        with zipfile.ZipFile(zip_path, 'r') as zf:
            zf.extractall(extract_dir)

        # Find importable files in extraction (priority: GPKG > SHP)
        candidates = []
        candidates.extend(extract_dir.rglob('*.gpkg'))
        candidates.extend(extract_dir.rglob('*.shp'))

        # Try importing first valid file
        for candidate in candidates:
            try:
                if candidate.suffix.lower() == '.gpkg':
                    success = import_gpkg(candidate, out_fc)
                else:  # .shp
                    success = import_shapefile(candidate, out_fc)

                if success:
                    logging.debug(f"[STAGE] ZIP imported: {candidate.name}")
                    return True

            except Exception as e:
                logging.debug(f"[STAGE] ZIP candidate failed: {e}")
                continue

        logging.warning(f"[STAGE] No importable data in {zip_path.name}")
        return False

    except Exception as e:
        logging.error(f"[STAGE] ZIP processing failed: {e}")
        return False

    finally:
        # Cleanup extraction directory
        if extract_dir.exists():
            try:
                shutil.rmtree(extract_dir)
            except Exception:
                pass  # Best effort cleanup

def ensure_gdb_exists(gdb_path: str) -> None:
    """Ensure staging geodatabase exists."""
    gdb_path_obj = Path(gdb_path)

    if not gdb_path_obj.exists():
        gdb_path_obj.parent.mkdir(parents=True, exist_ok=True)
        arcpy.management.CreateFileGDB(
            str(gdb_path_obj.parent),
            gdb_path_obj.name
        )
        logging.info(f"[STAGE] Created staging GDB: {gdb_path}")

def clear_staging_gdb(gdb_path: str) -> None:
    """Clear all feature classes from staging GDB."""
    try:
        # Use arcpy.da.Walk to list feature classes without changing workspace
        feature_classes = []
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