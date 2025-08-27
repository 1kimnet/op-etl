"""
Spatial Reference (SR) utilities for ETL pipeline.
Provides validation, sanity checks, and consistency enforcement.
"""
import logging
from typing import Optional, List, Dict, Any, Tuple

log = logging.getLogger(__name__)

# Standard SRs used in the pipeline
SWEREF99_TM = 3006  # Swedish reference system, meters
WGS84_DD = 4326     # World Geodetic System, degrees
CRS84 = "CRS84"     # OGC CRS84 (equivalent to WGS84 but lon/lat order)

def validate_coordinates_magnitude(coords: List[float], expected_sr: int) -> bool:
    """
    Validate that coordinate magnitudes are reasonable for the expected SR.
    
    Args:
        coords: List of coordinates [x/lon, y/lat, ...]
        expected_sr: Expected EPSG code
        
    Returns:
        True if magnitudes are reasonable, False otherwise
    """
    if not coords or len(coords) < 2:
        return False
        
    x, y = coords[0], coords[1]
    
    if expected_sr == SWEREF99_TM:
        return _validate_sweref99_bounds(x, y)
    elif expected_sr == WGS84_DD:
        return _validate_wgs84_bounds(x, y)
        
    return True


def _validate_sweref99_bounds(x: float, y: float) -> bool:
    """Validate SWEREF99 TM coordinate bounds."""
    # SWEREF99 TM (EPSG:3006) - should be in meters
    # Rough bounds for Sweden: X: 200000-900000, Y: 6100000-7700000
    
    # First check for degree-like values (common mistake)
    if (-180 <= x <= 180 and -90 <= y <= 90):
        log.error(f"Coordinates {x}, {y} appear to be degrees but expected SWEREF99 TM meters - possible SR mismatch")
        return False
    
    # Then check proper SWEREF99 TM bounds
    if not (200000 <= x <= 900000 and 6100000 <= y <= 7700000):
        log.warning(f"Coordinates {x}, {y} outside expected SWEREF99 TM bounds")
        return False
    return True


def _validate_wgs84_bounds(x: float, y: float) -> bool:
    """Validate WGS84 coordinate bounds."""
    # WGS84 - should be in degrees
    # Sweden roughly: Lon: 10-25, Lat: 55-70
    if not (-180 <= x <= 180 and -90 <= y <= 90):
        log.warning(f"Coordinates {x}, {y} outside valid WGS84 degree bounds")
        return False
    # More specific check for Sweden
    if not (10 <= x <= 25 and 55 <= y <= 70):
        log.warning(f"Coordinates {x}, {y} outside expected Sweden WGS84 bounds")
        
    return True

def validate_bbox_vs_envelope(bbox: List[float], envelope: Dict[str, float], 
                             tolerance: float = 0.1) -> bool:
    """
    Validate that response envelope roughly matches requested bbox.
    
    Args:
        bbox: Requested bbox [xmin, ymin, xmax, ymax]
        envelope: Response envelope with xmin, ymin, xmax, ymax keys
        tolerance: Tolerance factor (0.1 = 10% difference allowed)
        
    Returns:
        True if envelope matches bbox within tolerance
    """
    if not bbox or len(bbox) < 4:
        return True  # No bbox to compare
        
    if not _validate_envelope_structure(envelope):
        return False
        
    req_coords = bbox[:4]
    resp_coords = [envelope['xmin'], envelope['ymin'], envelope['xmax'], envelope['ymax']]
    
    return _check_coordinate_tolerance(req_coords, resp_coords, tolerance)


def _validate_envelope_structure(envelope: Dict[str, float]) -> bool:
    """Validate envelope has required fields."""
    if not envelope or not all(k in envelope for k in ['xmin', 'ymin', 'xmax', 'ymax']):
        log.warning("Response envelope missing required fields")
        return False
    return True


def _check_coordinate_tolerance(req_coords: List[float], resp_coords: List[float], tolerance: float) -> bool:
    """Check if response coordinates are within tolerance of requested coordinates."""
    req_xmin, req_ymin, req_xmax, req_ymax = req_coords
    resp_xmin, resp_ymin, resp_xmax, resp_ymax = resp_coords
    
    # Calculate width/height for tolerance
    req_width = abs(req_xmax - req_xmin)
    req_height = abs(req_ymax - req_ymin)
    
    # Check if response envelope is within reasonable bounds of request
    x_tolerance = req_width * tolerance
    y_tolerance = req_height * tolerance
    
    if (abs(resp_xmin - req_xmin) > x_tolerance or
        abs(resp_ymin - req_ymin) > y_tolerance or
        abs(resp_xmax - req_xmax) > x_tolerance or
        abs(resp_ymax - req_ymax) > y_tolerance):
        envelope_dict = {'xmin': resp_xmin, 'ymin': resp_ymin, 'xmax': resp_xmax, 'ymax': resp_ymax}
        log.warning(f"Response envelope {envelope_dict} differs significantly from bbox {req_coords}")
        return False
        
    return True

def validate_feature_count_sanity(small_bbox_count: int, large_bbox_count: int,
                                 min_ratio: float = 0.1) -> bool:
    """
    Validate that larger bbox returns more features than smaller bbox.
    
    Args:
        small_bbox_count: Feature count from smaller bbox
        large_bbox_count: Feature count from larger bbox  
        min_ratio: Minimum ratio (small/large) to be considered sane
        
    Returns:
        True if counts are sensible
    """
    if large_bbox_count == 0:
        return small_bbox_count == 0
        
    if small_bbox_count > large_bbox_count:
        log.warning(f"Small bbox returned more features ({small_bbox_count}) than large bbox ({large_bbox_count})")
        return False
        
    ratio = small_bbox_count / large_bbox_count if large_bbox_count > 0 else 0
    if ratio < min_ratio:
        log.warning(f"Feature count ratio {ratio:.2f} seems too low (small: {small_bbox_count}, large: {large_bbox_count})")
        
    return True

def detect_sr_from_geojson(geojson_data: Dict[str, Any]) -> Optional[int]:
    """
    Detect spatial reference from GeoJSON CRS object.
    
    Args:
        geojson_data: GeoJSON feature collection
        
    Returns:
        EPSG code if detected, None otherwise
    """
    if not isinstance(geojson_data, dict):
        return None
        
    crs = geojson_data.get('crs')
    if not crs:
        return WGS84_DD  # GeoJSON defaults to WGS84
        
    properties = crs.get('properties', {})
    if isinstance(properties, dict):
        name = properties.get('name', '')
        if isinstance(name, str):
            # Handle various CRS name formats
            if 'EPSG:' in name:
                try:
                    return int(name.split('EPSG:')[1].split()[0])
                except (ValueError, IndexError):
                    pass
            elif name.upper() == 'CRS84':
                return WGS84_DD
                
    return None

def validate_sr_consistency(data: Dict[str, Any], expected_sr: Optional[int]) -> Tuple[bool, Optional[int]]:
    """
    Validate spatial reference consistency in response data.
    
    Args:
        data: Response data (GeoJSON or ArcGIS REST response)
        expected_sr: Expected EPSG code
        
    Returns:
        Tuple of (is_valid, detected_sr)
    """
    detected_sr = _detect_sr_from_data(data)
    
    # Validate coordinate magnitudes if we have features and expected SR
    if data.get('type') == 'FeatureCollection' and expected_sr:
        coord_valid = _validate_feature_coordinates(data, expected_sr)
        if not coord_valid:
            return False, detected_sr
    
    # Check consistency between expected and detected SR
    return _check_sr_consistency(expected_sr, detected_sr)


def _detect_sr_from_data(data: Dict[str, Any]) -> Optional[int]:
    """Detect spatial reference from data."""
    if data.get('type') == 'FeatureCollection':
        return detect_sr_from_geojson(data)
    elif 'spatialReference' in data:
        sr_info = data['spatialReference']
        if isinstance(sr_info, dict) and 'wkid' in sr_info:
            return sr_info['wkid']
    return None


def _validate_feature_coordinates(data: Dict[str, Any], expected_sr: int) -> bool:
    """Validate coordinates in first feature of FeatureCollection."""
    features = data.get('features', [])
    if not features:
        return True
        
    first_feature = features[0]
    geometry = first_feature.get('geometry', {})
    coordinates = geometry.get('coordinates')
    
    if not coordinates:
        return True
        
    flat_coords = _flatten_coordinates(coordinates)
    if flat_coords and len(flat_coords) >= 2:
        return validate_coordinates_magnitude(flat_coords[:2], expected_sr)
    
    return True


def _check_sr_consistency(expected_sr: Optional[int], detected_sr: Optional[int]) -> Tuple[bool, Optional[int]]:
    """Check consistency between expected and detected spatial reference."""
    # Check for unknown SR
    if detected_sr is None:
        log.warning("Unknown spatial reference detected")
        return False, None
    
    # Check consistency
    if expected_sr and detected_sr and expected_sr != detected_sr:
        log.warning(f"SR mismatch: expected {expected_sr}, detected {detected_sr}")
        return False, detected_sr
        
    return True, detected_sr


def _infer_sr_from_coords(xy: List[float]) -> Optional[int]:
    """
    Best-effort SR inference from a single [x, y] pair.
    Returns EPSG code when confidently inferred, otherwise None.
    """
    x, y = xy[0], xy[1]
    # Degrees plausibility (global)
    if -180 <= x <= 180 and -90 <= y <= 90:
        return WGS84_DD
    # SWEREF99 TM rough bounds (meters, Sweden)
    if 200000 <= x <= 900000 and 6100000 <= y <= 7700000:
        return SWEREF99_TM
    return None


def _flatten_coordinates(coords) -> List[float]:
    """Helper to flatten nested coordinate arrays to get first coordinate pair."""
    if not coords:
        return []
        
    # Handle different geometry types
    if isinstance(coords[0], (int, float)):
        return coords  # Point coordinates
    elif isinstance(coords[0], list):
        if len(coords[0]) >= 2 and isinstance(coords[0][0], (int, float)):
            return coords[0]  # First coordinate of LineString/Polygon
        elif len(coords[0]) > 0 and isinstance(coords[0][0], list):
            return _flatten_coordinates(coords[0])  # Nested (Polygon holes, etc.)
            
    return []

def get_sr_config_for_source(source: Dict[str, Any]) -> Dict[str, Any]:
    """
    Get spatial reference configuration for a source.
    
    Args:
        source: Source configuration
        
    Returns:
        Dictionary with SR configuration
    """
    source_type = source.get('type')
    raw = source.get('raw', {})
    
    # Default configurations based on best practices
    if source_type == 'rest':
        # Check response format to determine appropriate SR configuration
        response_format = raw.get('response_format', 'esrijson')  # Default to esrijson for backward compatibility
        
        if response_format == 'geojson':
            # GeoJSON path: assume EPSG:4326, stage in 4326, project to 3006
            return {
                'response_format': 'geojson',
                'bbox_sr': raw.get('bbox_sr', SWEREF99_TM),  # Still filter in 3006 for consistency
                'in_sr': raw.get('in_sr', SWEREF99_TM),
                'out_sr': None,  # Don't set outSR for GeoJSON - servers often ignore it
                'stage_sr': raw.get('stage_sr', WGS84_DD),
                'target_sr': raw.get('target_sr', SWEREF99_TM)
            }
        else:  # 'esrijson' (default)
            # EsriJSON path: use EPSG:3006 throughout
            return {
                'response_format': 'esrijson',
                'bbox_sr': raw.get('bbox_sr', SWEREF99_TM),
                'in_sr': raw.get('in_sr', SWEREF99_TM),
                'out_sr': raw.get('out_sr', SWEREF99_TM),
                'stage_sr': raw.get('stage_sr', SWEREF99_TM),
                'target_sr': raw.get('target_sr', SWEREF99_TM)
            }
    elif source_type == 'ogc':
        # Check if server supports EPSG:3006
        supports_3006 = raw.get('supports_epsg_3006', False)
        if supports_3006:
            return {
                'bbox_crs': f'EPSG:{SWEREF99_TM}',
                'stage_sr': SWEREF99_TM,
                'target_sr': SWEREF99_TM
            }
        else:
            return {
                'bbox_crs': CRS84,
                'stage_sr': WGS84_DD,
                'target_sr': SWEREF99_TM
            }
    elif source_type in ['wfs', 'file', 'atom']:
        # For file-based sources, handle during staging
        return {
            'stage_sr': None,  # Detect from file
            'target_sr': SWEREF99_TM
        }
        
    return {}

def log_sr_validation_summary(source_name: str, validation_results: Dict[str, Any]):
    """
    Log a summary of SR validation results.
    
    Args:
        source_name: Name of the source
        validation_results: Dictionary of validation results
    """
    log.info(f"[SR] Validation summary for {source_name}:")
    
    for check, result in validation_results.items():
        status = "✓" if result else "✗"
        log.info(f"[SR]   {check}: {status}")
        
    # Count passed/failed
    passed = sum(1 for r in validation_results.values() if r)
    total = len(validation_results)
    log.info(f"[SR] Overall: {passed}/{total} checks passed")
    
    if passed < total:
        log.warning(f"[SR] {source_name} failed {total - passed} SR validation checks")