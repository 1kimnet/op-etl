"""
Simplified staging system for OP-ETL focused on maintainability.
Handles the 90% use case with clear, straightforward code.
Enhanced with proper GeoJSON geometry type handling.

Key Features:
- Automatic geometry type detection and filtering for mixed-type GeoJSON files
- Dominant geometry type analysis ensures reliable feature class creation  
- Explicit geometry type specification in ArcPy JSONToFeatures calls
- Coordinate validation and spatial reference consistency (projects to SWEREF99 TM)
- Support for GPKG, GeoJSON, Shapefile, and ZIP archive formats
- Clean error handling and descriptive logging

Usage:
    Set use_simplified_staging: true in config.yaml to use this module.
    The enhanced staging automatically handles:
    - Mixed geometry types in GeoJSON (filters to dominant type)
    - Spatial reference detection and projection to SWEREF99 TM
    - Safe feature class naming with authority prefixes
"""

import json
import logging
import shutil
import tempfile
import zipfile
from pathlib import Path
from typing import List, Dict, Any, Optional

from .sr_utils import SWEREF99_TM, WGS84_DD, detect_sr_from_geojson, validate_coordinates_magnitude
from .utils import make_arcpy_safe_name

logger = logging.getLogger(__name__)


def _analyze_geojson_geometry_types(features: List[Dict[str, Any]]) -> Dict[str, int]:
    """Analyze geometry types in GeoJSON features and return counts."""
    geom_counts = {}
    for feature in features:
        try:
            geom = feature.get("geometry", {})
            geom_type = geom.get("type")
            if geom_type:
                geom_counts[geom_type] = geom_counts.get(geom_type, 0) + 1
        except Exception:
            continue
    return geom_counts


def _get_dominant_geometry_type(geom_counts: Dict[str, int]) -> Optional[str]:
    """Get the most frequent geometry type from counts."""
    if not geom_counts:
        return None
    return max(geom_counts.items(), key=lambda x: x[1])[0]


def _filter_features_by_geometry_type(features: List[Dict[str, Any]], target_type: str) -> List[Dict[str, Any]]:
    """Filter GeoJSON features to keep only those matching the target geometry type."""
    filtered = []
    for feature in features:
        try:
            geom = feature.get("geometry", {})
            if geom.get("type") == target_type:
                filtered.append(feature)
        except Exception:
            continue
    return filtered


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
        config: Configuration dict with workspaces.downloads and workspaces.staging_gdb
    """

    downloads_dir = Path(config["workspaces"]["downloads"])
    staging_gdb = Path(config["workspaces"]["staging_gdb"])

    if not downloads_dir.exists():
        logger.warning(f"[STAGE] Downloads directory not found: {downloads_dir}")
        return

    logger.info(f"[STAGE] Staging downloads from {downloads_dir} to {staging_gdb}")

    # Discover files to import
    files_to_import = _discover_files(downloads_dir)
    logger.info(f"[STAGE] Found {len(files_to_import)} files to import")

    for file_path, authority in files_to_import:
        try:
            _import_file_to_staging(file_path, authority, staging_gdb)
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


def _import_file_to_staging(file_path, authority, staging_gdb):
    """
    Import a single file to staging GDB with appropriate naming and SR.

    Args:
        file_path: Path to file to import
        authority: Authority name for prefixing
        staging_gdb: Path to staging GDB
    """
    file_path = Path(file_path)
    suffix = file_path.suffix.lower()

    logger.info(f"[STAGE] Importing {file_path} from {authority}")

    if suffix == '.gpkg':
        _import_gpkg(file_path, authority, staging_gdb)
    elif suffix in {'.geojson', '.json'}:
        _import_geojson(file_path, authority, staging_gdb)
    elif suffix == '.shp':
        _import_shapefile(file_path, authority, staging_gdb)
    elif suffix == '.zip':
        _import_zip(file_path, authority, staging_gdb)
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


def _import_geojson(geojson_path, authority, staging_gdb):
    """Import GeoJSON file to staging GDB with proper geometry type handling."""
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

        # Analyze geometry types
        geom_counts = _analyze_geojson_geometry_types(features)
        dominant_type = _get_dominant_geometry_type(geom_counts)
        
        if not dominant_type:
            logger.warning(f"[STAGE] No valid geometry types found in {geojson_path}")
            return

        logger.info(f"[STAGE] Geometry types in {geojson_path.name}: {geom_counts}")
        logger.info(f"[STAGE] Dominant geometry type: {dominant_type}")

        # Filter to dominant geometry type if mixed types present
        total_features = len(features)
        dominant_count = geom_counts.get(dominant_type, 0)
        
        if len(geom_counts) > 1:
            logger.info(f"[STAGE] Mixed geometry types detected, filtering to {dominant_type}")
            features = _filter_features_by_geometry_type(features, dominant_type)
            logger.info(f"[STAGE] Filtered {total_features} -> {len(features)} features")
        
        if not features:
            logger.warning(f"[STAGE] No features remaining after filtering in {geojson_path}")
            return

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

        # Use explicit geometry type for reliable import
        arcpy_geom_type = _geojson_to_arcpy_geometry_type(dominant_type)
        
        if len(geom_counts) > 1 or not arcpy_geom_type:
            # Write filtered GeoJSON to temporary file for import
            temp_path = None
            try:
                with tempfile.NamedTemporaryFile(mode='w', suffix='.geojson', delete=False, encoding='utf-8') as tmp:
                    filtered_data = {"type": "FeatureCollection", "features": features}
                    json.dump(filtered_data, tmp, ensure_ascii=False)
                    temp_path = tmp.name

                logger.info(f"[STAGE] Converting {geojson_path.name} -> {target_name} (geometry type: {arcpy_geom_type})")
                
                # Import with explicit geometry type if available
                if arcpy_geom_type:
                    arcpy.conversion.JSONToFeatures(
                        temp_path, output_fc,
                        geometry_type=arcpy_geom_type, 
                        spatial_reference=SWEREF99_TM
                    )
                else:
                    arcpy.conversion.JSONToFeatures(
                        temp_path, output_fc,
                        spatial_reference=SWEREF99_TM
                    )
            finally:
                if temp_path and Path(temp_path).exists():
                    try:
                        Path(temp_path).unlink()
                    except Exception:
                        pass
        else:
            # Simple case - single geometry type
            logger.info(f"[STAGE] Converting {geojson_path.name} -> {target_name} (single type: {arcpy_geom_type})")
            arcpy.conversion.JSONToFeatures(
                str(geojson_path), output_fc,
                geometry_type=arcpy_geom_type if arcpy_geom_type else "",
                spatial_reference=SWEREF99_TM
            )

        logger.info(f"[STAGE] Successfully imported {len(features)} features to {target_name}")

    except Exception as e:
        logger.error(f"[STAGE] Failed to process GeoJSON {geojson_path}: {e}")
        raise


def _flatten_coordinates_simple(coords):
    """Simple coordinate flattening for validation (iterative, avoids recursion)."""
    if not coords:
        return []
    
    flat = []
    stack = [coords]
    while stack:
        item = stack.pop()
        if isinstance(item, (list, tuple)):
            # Add items in reverse order so they are processed in original order
            stack.extend(reversed(item))
        else:
            flat.append(item)
    return flat


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


def _import_zip(zip_path, authority, staging_gdb):
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
                _import_file_to_staging(extracted_file, authority, staging_gdb)

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
