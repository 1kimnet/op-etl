"""
Simplified staging system for OP-ETL focused on maintainability.
Handles the 90% use case with clear, straightforward code.
Enhanced with geometry type configuration hints.

Key Features:
- Geometry type hints from source configuration for predictable results
- Simple first-feature detection fallback when no hint provided
- Coordinate validation and spatial reference consistency (projects to SWEREF99 TM)
- Support for GPKG, GeoJSON, Shapefile, and ZIP archive formats
- Clean error handling and descriptive logging

Usage:
    Set use_simplified_staging: true in config.yaml to use this module.
    Add geometry_type in source raw config for optimal performance:
    
    sources:
    - name: Brunnar
      authority: SGU
      type: ogc
      raw:
        geometry_type: Point  # Hint for consistent staging
        collections:
        - brunnar-lager
"""

import json
import logging
import shutil
import zipfile
from pathlib import Path

from .sr_utils import SWEREF99_TM, WGS84_DD, detect_sr_from_geojson, validate_coordinates_magnitude
from .utils import make_arcpy_safe_name

logger = logging.getLogger(__name__)


def _geojson_to_arcpy_geometry_type(geojson_type: str) -> str:
    """Map GeoJSON geometry type to ArcPy geometry type."""
    mapping = {
        "Point": "POINT",
        "MultiPoint": "MULTIPOINT", 
        "LineString": "POLYLINE",
        "MultiLineString": "POLYLINE",
        "Polygon": "POLYGON",
        "MultiPolygon": "POLYGON"
    }
    return mapping.get(geojson_type, "")


def stage_all_downloads(config):
    """
    Stage all downloaded files into staging GDB with SWEREF99 TM SR.

    Args:
        config: Configuration dict with workspaces.downloads, workspaces.staging_gdb and sources
    """

    downloads_dir = Path(config["workspaces"]["downloads"])
    staging_gdb = Path(config["workspaces"]["staging_gdb"])

    if not downloads_dir.exists():
        logger.warning(f"[STAGE] Downloads directory not found: {downloads_dir}")
        return

    logger.info(f"[STAGE] Staging downloads from {downloads_dir} to {staging_gdb}")

    # Build authority to source mapping for geometry type hints
    authority_to_source = {}
    for source in config.get("sources", []):
        authority = source.get("authority", "").upper()
        if authority:
            authority_to_source[authority] = source

    # Discover files to import
    files_to_import = _discover_files(downloads_dir)
    logger.info(f"[STAGE] Found {len(files_to_import)} files to import")

    for file_path, authority in files_to_import:
        try:
            source_config = authority_to_source.get(authority.upper(), {})
            _import_file_to_staging(file_path, authority, staging_gdb, source_config)
        except Exception as e:
            logger.error(f"[STAGE] Failed to import {file_path}: {e}")
            continue


def _discover_files(downloads_dir):
    """
    Discover files to import from downloads directory.

    Returns:
        List of (file_path, authority) tuples
    """
    files_to_import = []

    # Look for authority directories
    for authority_dir in downloads_dir.iterdir():
        if not authority_dir.is_dir():
            continue

        authority = authority_dir.name

        # Find data files in authority directory
        for file_path in authority_dir.rglob("*"):
            if not file_path.is_file():
                continue

            suffix = file_path.suffix.lower()
            if suffix in ['.gpkg', '.geojson', '.json', '.shp', '.zip']:
                files_to_import.append((file_path, authority))

    return files_to_import


def _import_file_to_staging(file_path, authority, staging_gdb, source_config=None):
    """
    Import a single file to staging GDB with appropriate naming and SR.

    Args:
        file_path: Path to file to import
        authority: Authority name for prefixing
        staging_gdb: Path to staging GDB
        source_config: Source configuration dict (optional, for geometry type hints)
    """
    file_path = Path(file_path)
    suffix = file_path.suffix.lower()

    logger.info(f"[STAGE] Importing {file_path} from {authority}")

    if suffix == '.gpkg':
        _import_gpkg(file_path, authority, staging_gdb)
    elif suffix in {'.geojson', '.json'}:
        _import_geojson(file_path, authority, staging_gdb, source_config)
    elif suffix == '.shp':
        _import_shapefile(file_path, authority, staging_gdb)
    elif suffix == '.zip':
        _import_zip(file_path, authority, staging_gdb, source_config)
    else:
        logger.warning(f"[STAGE] Unsupported file type: {suffix}")


def _import_gpkg(gpkg_path, authority, staging_gdb):
    """Import all feature classes from a GeoPackage."""
    import arcpy

    gpkg_path = Path(gpkg_path)
    logger.info(f"[STAGE] Processing GeoPackage: {gpkg_path}")

    try:
        # List feature classes in GPKG
        arcpy.env.workspace = str(gpkg_path)
        feature_classes = arcpy.ListFeatureClasses()

        if not feature_classes:
            logger.warning(f"[STAGE] No feature classes found in {gpkg_path}")
            return

        for fc_name in feature_classes:
            source_fc = str(gpkg_path / fc_name)
            target_name = make_arcpy_safe_name(f"{authority}_{fc_name}")

            logger.info(f"[STAGE] Importing {fc_name} -> {target_name}")

            # Copy with SR transformation
            arcpy.conversion.FeatureClassToFeatureClass(
                source_fc, str(staging_gdb), target_name,
                output_coordinate_system=SWEREF99_TM
            )

    except Exception as e:
        logger.error(f"[STAGE] Failed to process GPKG {gpkg_path}: {e}")
        raise


def _import_geojson(geojson_path, authority, staging_gdb, source_config=None):
    """Import GeoJSON file to staging GDB with geometry type hint support."""
    import arcpy

    geojson_path = Path(geojson_path)
    logger.info(f"[STAGE] Processing GeoJSON: {geojson_path}")

    try:
        # Read and validate GeoJSON format
        with open(geojson_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        if 'features' not in data:
            logger.warning(f"[STAGE] Invalid GeoJSON format: {geojson_path}")
            return

        features = data.get('features', [])
        if not features:
            logger.warning(f"[STAGE] No features found in {geojson_path}")
            return

        # Get geometry type hint from source config
        geometry_type_hint = None
        if source_config:
            raw_config = source_config.get('raw', {})
            geometry_type_hint = raw_config.get('geometry_type')

        # If no hint, detect from first feature (simplified approach)
        geometry_type = geometry_type_hint
        if not geometry_type and features:
            first_geom = features[0].get('geometry', {})
            geometry_type = first_geom.get('type')

        if not geometry_type:
            logger.warning(f"[STAGE] No geometry type found or hinted for {geojson_path}")
            return

        logger.info(f"[STAGE] Using geometry type: {geometry_type} for {geojson_path.name}")

        # Validate coordinates if possible
        if features:
            first_geom = features[0].get('geometry', {})
            coords = first_geom.get('coordinates')
            if coords:
                # Flatten nested coordinates to check first coordinate pair
                flat_coords = _flatten_coordinates_simple(coords)
                if len(flat_coords) >= 2:
                    # Detect spatial reference from GeoJSON
                    detected_sr = detect_sr_from_geojson(data) or WGS84_DD
                    if not validate_coordinates_magnitude(flat_coords[:2], detected_sr):
                        logger.error(f"[STAGE] Invalid coordinate magnitudes in {geojson_path}")
                        return

        # Create output path
        target_name = make_arcpy_safe_name(f"{authority}_{geojson_path.stem}")
        output_fc = str(staging_gdb / target_name)

        # Remove existing feature class if present
        try:
            if arcpy.Exists(output_fc):
                arcpy.management.Delete(output_fc)
        except Exception:
            pass

        # Convert to ArcPy geometry type
        arcpy_geom_type = _geojson_to_arcpy_geometry_type(geometry_type)
        
        logger.info(f"[STAGE] Converting {geojson_path.name} -> {target_name} (geometry type: {arcpy_geom_type})")
        
        # Import with explicit geometry type if available
        if arcpy_geom_type:
            arcpy.conversion.JSONToFeatures(
                str(geojson_path), output_fc,
                geometry_type=arcpy_geom_type, 
                spatial_reference=SWEREF99_TM
            )
        else:
            # Fallback to auto-detection
            arcpy.conversion.JSONToFeatures(
                str(geojson_path), output_fc,
                spatial_reference=SWEREF99_TM
            )

        logger.info(f"[STAGE] Successfully imported {target_name}")

    except Exception as e:
        logger.error(f"[STAGE] Failed to process GeoJSON {geojson_path}: {e}")
        raise


def _flatten_coordinates_simple(coords):
    """Simple coordinate flattening for validation (recursive, preserves order)."""
    if not coords:
        return []
    
    def _flatten(item):
        if isinstance(item, (list, tuple)):
            result = []
            for subitem in item:
                result.extend(_flatten(subitem))
            return result
        else:
            return [item]
    return _flatten(coords)
def _import_shapefile(shp_path, authority, staging_gdb):
    """Import Shapefile to staging GDB."""
    import arcpy

    shp_path = Path(shp_path)
    logger.info(f"[STAGE] Processing Shapefile: {shp_path}")

    try:
        target_name = make_arcpy_safe_name(f"{authority}_{shp_path.stem}")

        logger.info(f"[STAGE] Converting {shp_path.name} -> {target_name}")

        # Copy with SR transformation
        arcpy.conversion.FeatureClassToFeatureClass(
            str(shp_path), str(staging_gdb), target_name,
            output_coordinate_system=SWEREF99_TM
        )

    except Exception as e:
        logger.error(f"[STAGE] Failed to process Shapefile {shp_path}: {e}")
        raise


def _import_zip(zip_path, authority, staging_gdb, source_config=None):
    """Extract and import contents of ZIP file."""
    zip_path = Path(zip_path)
    logger.info(f"[STAGE] Processing ZIP: {zip_path}")

    try:
        # Extract to temporary directory
        extract_dir = zip_path.parent / f"temp_{zip_path.stem}"
        extract_dir.mkdir(exist_ok=True)

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)

        # Process extracted files
        for extracted_file in extract_dir.rglob("*"):
            if not extracted_file.is_file():
                continue

            suffix = extracted_file.suffix.lower()
            if suffix in ['.gpkg', '.geojson', '.json', '.shp']:
                _import_file_to_staging(extracted_file, authority, staging_gdb, source_config)

        # Cleanup
        shutil.rmtree(extract_dir, ignore_errors=True)

    except Exception as e:
        logger.error(f"[STAGE] Failed to process ZIP {zip_path}: {e}")
        # Cleanup on error
        extract_dir = zip_path.parent / f"temp_{zip_path.stem}"
        if extract_dir.exists():
            shutil.rmtree(extract_dir, ignore_errors=True)
        raise


def _ensure_sweref99_tm(feature_class_path):
    """
    Ensure feature class has SWEREF99 TM spatial reference.
    Projects if needed, logs if already correct.

    Args:
        feature_class_path: Path to feature class to check/fix
    """
    import arcpy

    try:
        desc = arcpy.Describe(feature_class_path)
        current_sr = desc.spatialReference

        if current_sr.factoryCode == SWEREF99_TM.factoryCode:
            logger.debug(f"[STAGE] {feature_class_path} already in SWEREF99 TM")
            return

        logger.info(f"[STAGE] Projecting {feature_class_path} to SWEREF99 TM")

        # Create temporary output
        temp_fc = f"{feature_class_path}_temp"

        arcpy.management.Project(
            feature_class_path, temp_fc, SWEREF99_TM
        )

        # Replace original
        arcpy.management.Delete(feature_class_path)
        arcpy.management.Rename(temp_fc, feature_class_path)

        logger.info(f"[STAGE] Successfully projected {feature_class_path}")

    except Exception as e:
        logger.error(f"[STAGE] Failed to ensure SWEREF99 TM for {feature_class_path}: {e}")
        # Cleanup temp if it exists
        temp_fc = f"{feature_class_path}_temp"
        if arcpy.Exists(temp_fc):
            arcpy.management.Delete(temp_fc)
        raise
