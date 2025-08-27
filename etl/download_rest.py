"""
REST API downloader for OP-ETL pipeline.
Enhanced implementation with recursion depth protection and SR consistency.
"""

import json
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple, TypeAlias

from .http_utils import RecursionSafeSession, safe_json_parse, validate_response_content
from .monitoring import end_monitoring_source, start_monitoring_source
from .sr_utils import (
    SWEREF99_TM,
    WGS84_DD,
)

log = logging.getLogger(__name__)

# Constants for parallel processing and retry handling
MAX_CONCURRENT_REQUESTS = 8  # Hard cap on concurrent requests per layer
MAX_RETRY_AFTER_SECONDS = 30  # Maximum wait time for Retry-After headers

# Type aliases
BBox: TypeAlias = Tuple[float, float, float, float]


class TransferLimitExceededError(Exception):
    """Raised when REST service hits transfer limits and needs alternative pagination."""
    pass


def sanitize_layer_name(name: str) -> str:
    """Sanitize layer name to make it safe for use as a filename."""
    if not name:
        return "unknown_layer"

    # Replace problematic characters with safe alternatives
    sanitized = re.sub(r'[<>:"/\\|?*]', "_", name)  # Windows problematic chars
    sanitized = re.sub(r"[\x00-\x1f\x7f-\x9f]", "_", sanitized)  # Control chars
    sanitized = re.sub(r"\s+", "_", sanitized)  # Multiple spaces to single underscore
    sanitized = sanitized.strip("._")  # Remove leading/trailing dots and underscores

    # Ensure it's not empty after sanitization
    if not sanitized:
        return "unknown_layer"

    # Limit length to avoid filesystem issues
    if len(sanitized) > 200:
        sanitized = sanitized[:200].rstrip("_")

    return sanitized


def _extract_global_bbox(cfg: dict) -> Tuple[Optional[List[float]], Optional[int]]:
    """Read a global bbox and SR from config.
    Supports keys: cfg['global_bbox'] or cfg['global_ogc_bbox'] when cfg['use_bbox_filter'] is true.
    Returns (coords, sr) where coords is [xmin,ymin,xmax,ymax] and sr is EPSG int (default None).
    """
    try:
        if not cfg.get("use_bbox_filter", False):
            return None, None
        gb = cfg.get("global_bbox") or cfg.get("global_ogc_bbox") or {}
        coords = gb.get("coords")
        crs = gb.get("crs")
        sr: Optional[int] = None
        if isinstance(crs, int):
            sr = crs
        elif isinstance(crs, str):
            if crs.upper() in ("WGS84", "CRS84"):
                sr = 4326
            elif crs.upper().startswith("EPSG:"):
                try:
                    sr = int(crs.split(":", 1)[1])
                except Exception:
                    sr = None
            elif "/EPSG/" in crs:
                try:
                    sr = int(crs.rstrip("/").split("/")[-1])
                except Exception:
                    sr = None
        return coords, sr
    except Exception:
        return None, None


def build_rest_params(cfg: Dict[str, Any], bbox_3006: Optional[BBox] = None) -> Dict[str, Any]:
    """
    Build REST API parameters with explicit format handling.

    Args:
        cfg: Raw configuration dictionary
        bbox_3006: Optional bounding box in SWEREF99 TM (3006) coordinates as (xmin, ymin, xmax, ymax)

    Returns:
        Dictionary of REST API parameters
    """
    fmt = cfg.get("response_format", "esrijson").lower()
    params = {
        "where": cfg.get("where", "1=1"),
        "outFields": "*",
        "returnGeometry": "true",
        "orderByFields": "OBJECTID ASC",
    }

    # Geometry filter: always send the envelope in SWEREF99 TM; REST expects JSON
    if bbox_3006:
        xmin, ymin, xmax, ymax = bbox_3006
        geom = {
            "xmin": xmin, "ymin": ymin, "xmax": xmax, "ymax": ymax,
            "spatialReference": {"wkid": SWEREF99_TM}
        }
        params.update({
            "geometry": json.dumps(geom),
            "geometryType": "esriGeometryEnvelope",
            "inSR": SWEREF99_TM,
            "spatialRel": "esriSpatialRelIntersects",
        })

    if fmt == "esrijson":
        params["f"] = "json"
        params["outSR"] = cfg.get("stage_sr", SWEREF99_TM)
    else:
        # GeoJSON path: outSR is ignored; assume WGS84 degrees
        params["f"] = "geojson"
        params.pop("outSR", None)

    return params


def create_staging_fc(workspace: str, name: str, geometry_type: str, response_format: str, stage_sr: int) -> str:
    """
    Create a staging feature class with the appropriate spatial reference.

    Args:
        workspace: Path to workspace (typically arcpy.env.scratchGDB)
        name: Feature class name
        geometry_type: Geometry type (e.g., "POLYLINE", "POLYGON", "POINT")
        response_format: Format used ("esrijson" or "geojson")
        stage_sr: Staging spatial reference EPSG code

    Returns:
        Full path to created feature class
    """
    try:
        import arcpy
        sr = arcpy.SpatialReference(stage_sr)
        arcpy.management.CreateFeatureclass(workspace, name, geometry_type, spatial_reference=sr)
        fc_path = f"{workspace}\\{name}"
        log.info(f"[REST] Created staging FC {name} with SR {stage_sr} ({response_format} format)")
        return fc_path
    except Exception as e:
        log.error(f"[REST] Failed to create staging FC {name}: {e}")
        raise


def ensure_target_sr(in_fc: str, target_sr: int, transform: Optional[str] = None) -> str:
    """
    Ensure feature class is in target spatial reference, projecting if needed.

    Args:
        in_fc: Input feature class path
        target_sr: Target spatial reference EPSG code
        transform: Optional transformation method

    Returns:
        Path to feature class in target SR (may be same as input if no projection needed)
    """
    try:
        import arcpy
        sr_in = arcpy.Describe(in_fc).spatialReference
        if sr_in and sr_in.factoryCode == target_sr:
            log.debug(f"[REST] FC already in target SR {target_sr}")
            return in_fc

        out_fc = in_fc + f"_{target_sr}"
        log.info(f"[REST] Projecting from SR {sr_in.factoryCode} to {target_sr}")

        project_params = {
            "in_dataset": in_fc,
            "out_dataset": out_fc,
            "out_coor_system": arcpy.SpatialReference(target_sr)
        }

        if transform:
            project_params["transform_method"] = transform

        arcpy.management.Project(**project_params)
        return out_fc
    except Exception as e:
        log.error(f"[REST] Failed to project FC to target SR {target_sr}: {e}")
        raise


def validate_staged_fc(fc: str, response_format: str, target_sr: int) -> None:
    """
    Validate staged feature class spatial reference and coordinate magnitudes.

    Args:
        fc: Feature class path
        response_format: Format used ("esrijson" or "geojson")
        target_sr: Expected target spatial reference

    Raises:
        RuntimeError: If validation fails
    """
    try:
        import arcpy
        sr = arcpy.Describe(fc).spatialReference
        if not sr or sr.factoryCode in (0, None):
            raise RuntimeError(f"{fc}: Unknown spatial reference")

        if response_format.lower() == "geojson":
            expected = WGS84_DD
        else:
            expected = target_sr  # esrijson path should already be in target SR

        if sr.factoryCode != expected:
            raise RuntimeError(f"{fc}: SR={sr.factoryCode}, expected {expected}")

        # Magnitude sanity check (sample 1 row)
        with arcpy.da.SearchCursor(fc, ["SHAPE@X", "SHAPE@Y"]) as cur:
            for x, y in cur:
                if expected == SWEREF99_TM and (-180 <= x <= 180 and -90 <= y <= 90):
                    raise RuntimeError(f"{fc}: Degrees detected in meter-based SR {expected}")
                elif expected == WGS84_DD and (abs(x) > 180 or abs(y) > 90):
                    raise RuntimeError(f"{fc}: Meter coordinates detected in degree-based SR {expected}")
                break  # Only check first row

        log.info(f"[REST] Validation passed for {fc} (SR={sr.factoryCode}, format={response_format})")

    except Exception as e:
        log.error(f"[REST] Validation failed for {fc}: {e}")
        raise


def run(cfg: dict) -> None:
    """Process all REST API sources."""
    global_bbox, global_sr = _extract_global_bbox(cfg)
    # Extract sources cleanly
    rest_sources = []
    for source in cfg.get("sources", []):
        if source.get("type") == "rest" and source.get("enabled", True):
            # Create clean copy
            rest_sources.append(
                {
                    "name": source.get("name"),
                    "url": source.get("url"),
                    "authority": source.get("authority", "unknown"),
                    "raw": source.get("raw", {}).copy() if source.get("raw") else {},
                }
            )

    if not rest_sources:
        log.info("[REST] No REST sources to process")
        return

    downloads_dir = Path(cfg["workspaces"]["downloads"])

    for source in rest_sources:
        _metric = start_monitoring_source(source["name"], source["authority"], "rest")  # noqa: F841

        try:
            log.info(f"[REST] Processing {source['name']}")
            success, feature_count = process_rest_source(source, downloads_dir, global_bbox, global_sr)
            end_monitoring_source(success, features=feature_count)  # Features counted in process_rest_source
        except RecursionError as e:
            log.error(f"[REST] Recursion error in {source['name']}: {e}")
            end_monitoring_source(False, "RecursionError", str(e))
        except Exception as e:
            log.error(f"[REST] Failed {source['name']}: {e}")
            end_monitoring_source(False, type(e).__name__, str(e))


def process_rest_source(
    source: Dict, downloads_dir: Path, global_bbox: Optional[List[float]], global_sr: Optional[int]
) -> Tuple[bool, int]:
    """Process a single REST API source."""
    base_url = source["url"].rstrip("/")
    authority = source["authority"]
    name = source["name"]
    raw = source.get("raw", {})

    # Create output directory
    out_dir = downloads_dir / authority / name
    out_dir.mkdir(parents=True, exist_ok=True)

    # Get layer IDs from config
    layer_ids = raw.get("layer_ids", [])

    # If no layer IDs specified, discover them (respect optional include patterns)
    if not layer_ids:
        raw_include = raw.get("include") or raw.get("includes")
        include_patterns = raw_include if isinstance(raw_include, list) else None
        layer_info = discover_layers(base_url, include_patterns)
        if not layer_info:
            log.info(f"[REST] No layers found in {name}, skipping download")
            return True, 0  # Not a failure - just no data in BBOX/filters
    else:
        # Convert configured layer IDs to layer info format
        # We'll need to discover layer names for these IDs
        layer_info = []
        all_discovered = discover_layers(base_url)
        discovered_by_id = {layer["id"]: layer for layer in all_discovered}

        for layer_id in layer_ids:
            if layer_id in discovered_by_id:
                layer_info.append(discovered_by_id[layer_id])
            else:
                # Layer ID not found in metadata, use fallback name
                log.warning(f"[REST] Layer ID {layer_id} not found in service metadata, using fallback name")
                layer_info.append({"id": layer_id, "name": f"layer_{layer_id}"})

    total_features = 0

    for layer in layer_info:
        layer_id = layer["id"]
        layer_name = layer["name"]
        try:
            sanitized_name = sanitize_layer_name(layer_name)

            layer_url = f"{base_url}/{layer_id}"
            feature_count = download_layer(layer_url, out_dir, sanitized_name, raw, global_bbox, global_sr)
            total_features += feature_count
            log.info(f"[REST] Layer {layer_id} ({layer_name}): {feature_count} features")
        except Exception as e:
            log.warning(f"[REST] Failed to download layer {layer_id} ({layer_name}): {e}")

    log.info(f"[REST] Total features from {name}: {total_features}")
    return total_features > 0, total_features


def discover_layers(base_url: str, include: list | None = None) -> List[Dict[str, Any]]:
    """Discover available layers in the service with enhanced error handling.

    Returns:
        List of dictionaries with 'id' and 'name' keys for each layer.
    """
    session = RecursionSafeSession()

    try:
        log.info(f"[REST] Discovering layers: {base_url}")

        # Query service info
        params = {"f": "json"}
        response = session.safe_get(base_url, params=params, timeout=30)

        if not response:
            log.warning(f"[REST] Failed to get service info from {base_url}")
            return []

        if not validate_response_content(response):
            log.warning(f"[REST] Invalid response content from {base_url}")
            return []

        data = safe_json_parse(response.content)
        if not data:
            log.warning(f"[REST] Failed to parse service info from {base_url}")
            return []

        # Extract layer info with both ID and name, optionally filtered by include patterns
        layer_info = []
        layers = data.get("layers", [])
        patterns = [p.lower() for p in include] if include else None

        import fnmatch

        for layer in layers:
            if isinstance(layer, dict) and "id" in layer:
                layer_id = layer["id"]
                layer_name = layer.get("name", f"layer_{layer_id}")

                if patterns:
                    lname = str(layer_name).lower()
                    if not any(fnmatch.fnmatchcase(lname, p) for p in patterns):
                        continue

                layer_info.append({"id": layer_id, "name": layer_name})

        # Handle single-layer FeatureServers
        if not layer_info and data.get("type") == "Feature Layer":
            log.info("[REST] Service appears to be a single-layer FeatureServer")
            layer_id = data.get("id", 0)
            layer_name = data.get("name", f"layer_{layer_id}")
            layer_info.append({"id": layer_id, "name": layer_name})

        log.info(f"[REST] Discovered {len(layer_info)} layers")
        return layer_info

    except RecursionError as e:
        log.error(f"[REST] Recursion error discovering layers: {e}")
        return []
    except Exception as e:
        log.warning(f"[REST] Failed to discover layers: {e}")
        return []


def download_layer(
    layer_url: str,
    out_dir: Path,
    layer_name: str,
    raw_config: Dict,
    global_bbox: Optional[List[float]],
    global_sr: Optional[int],
) -> int:
    """Download all features from a REST layer with enhanced error handling and pagination."""
    session = RecursionSafeSession()

    try:
        log.info(f"[REST] Downloading layer: {layer_url}")

        # Get layer info first
        info_params = {"f": "json"}
        info_response = session.safe_get(f"{layer_url}", params=info_params, timeout=30)

        if not info_response:
            log.warning(f"[REST] Failed to get layer info: {layer_url}")
            return 0

        if not validate_response_content(info_response):
            log.warning(f"[REST] Invalid layer info response: {layer_url}")
            return 0

        layer_info = safe_json_parse(info_response.content)
        if not layer_info:
            log.warning(f"[REST] Failed to parse layer info: {layer_url}")
            return 0

        # Check if layer supports queries
        if not layer_info.get("supportsQuery", True):
            log.warning(f"[REST] Layer doesn't support queries: {layer_url}")
            return 0

        # Get response format and SR configuration
        fmt = raw_config.get("response_format", "esrijson").lower()
        stage_sr = int(raw_config.get("stage_sr", SWEREF99_TM if fmt == "esrijson" else WGS84_DD))
        target_sr = int(raw_config.get("target_sr", SWEREF99_TM))
        transform = raw_config.get("geo_transform", "WGS_1984_To_ETRS_1989") if stage_sr != target_sr else None

        # Convert bbox to SWEREF99 TM tuple if needed
        # bbox = raw_config.get("bbox") or global_bbox
        # bbox_3006 = None
        # if bbox and len(bbox) >= 4:
        #     bbox_3006 = tuple(bbox[:4])
        # Replace with explicit coercion and validation
        bbox_raw: Optional[Sequence[float]] = raw_config.get("bbox") or global_bbox
        bbox_3006: Optional[BBox] = None
        try:
            bbox_3006 = coerce_bbox4(bbox_raw)
        except ValueError as e:
            log.warning(f"[REST] {layer_name}: invalid bbox {bbox_raw!r}: {e}")
            bbox_3006 = None

        # Build REST parameters using new helper
        base_params = build_rest_params(raw_config, bbox_3006)

        log.info(f"[REST] Using format={fmt}, stage_sr={stage_sr}, target_sr={target_sr}, transform={transform}")

        # Check if the service supports OID-based pagination: must support advanced queries and have an objectIdField
        supports_oids = layer_info.get("supportsAdvancedQueries", False) and bool(layer_info.get("objectIdField"))
        oid_field = layer_info.get("objectIdField", "OBJECTID")

        # Check for use_oid_sweep flag and related parameters
        use_oid_sweep = raw_config.get("use_oid_sweep", False)
        page_size = raw_config.get("page_size", 1000)
        max_workers = raw_config.get("max_workers", 6)

        # Download features using the appropriate method
        all_features = []
        request_count = 0
        metrics = {}

        if use_oid_sweep and supports_oids:
            # Use parallel OID-based pagination when explicitly requested
            log.info(f"[REST] {layer_name}: using parallel OID sweep (use_oid_sweep=true)")
            all_features, metrics = fetch_rest_layer_parallel(
                session, layer_url, base_params, layer_name, page_size, max_workers
            )
            request_count = metrics.get("request_count", 0)

            # Log metrics as requested in the acceptance criteria
            log.info(f"[REST] {layer_name}: OID sweep metrics - oids_total: {metrics.get('oids_total', 0)}, "
                    f"batches_total: {metrics.get('batches_total', 0)}, batches_ok: {metrics.get('batches_ok', 0)}, "
                    f"features_total: {metrics.get('features_total', 0)}")

        elif use_oid_sweep and not supports_oids:
            log.warning(f"[REST] {layer_name}: use_oid_sweep=true but OID pagination not supported, falling back to offset pagination")
            # Fall back to offset-based pagination
            try:
                all_features, request_count = _download_with_offset_pagination(
                    session, layer_url, base_params, layer_name, fmt, stage_sr
                )
            except TransferLimitExceededError:
                log.warning(f"[REST] {layer_name}: offset pagination failed with transfer limits and OID pagination not supported")
        else:
            # Default behavior: try offset-based pagination first, fall back to sequential OID-based if transfer limits hit
            try:
                all_features, request_count = _download_with_offset_pagination(
                    session, layer_url, base_params, layer_name, fmt, stage_sr
                )
            except TransferLimitExceededError:
                if supports_oids:
                    log.info(f"[REST] Transfer limit exceeded, switching to sequential OID-based pagination for {layer_name}")
                    all_features, request_count = _download_with_oid_pagination(
                        session, layer_url, base_params, oid_field, layer_name
                    )
                else:
                    log.warning(f"[REST] Transfer limit exceeded but OID pagination not supported for {layer_name}")
                    # Continue with what we got from offset pagination

        # Save all features as GeoJSON
        if all_features:
            geojson = {"type": "FeatureCollection", "features": all_features}

            out_file = out_dir / f"{layer_name}.geojson"
            with open(out_file, "w", encoding="utf-8") as f:
                json.dump(geojson, f, ensure_ascii=False, separators=(",", ":"))

            log.info(f"[REST] Completed {layer_name}: paged {len(all_features)} features in {request_count} requests")
        else:
            log.info(f"[REST] Completed {layer_name}: no features found in {request_count} requests")

        return len(all_features)

    except RecursionError as e:
        log.error(f"[REST] Recursion error downloading layer: {e}")
        return 0
    except Exception as e:
        log.error(f"[REST] Failed to download layer: {e}")
        return 0


def _download_with_offset_pagination(
    session: RecursionSafeSession,
    layer_url: str,
    base_params: Dict,
    layer_name: str,
    response_format: str,
    stage_sr: int
) -> Tuple[List[Dict], int]:
    """Download features using offset-based pagination with transfer limit detection."""
    all_features = []
    offset = 0
    page_size = 1000
    request_count = 0
    page_num = 1

    # Add pagination parameters to base params
    params = base_params.copy()
    params.update({
        "resultOffset": 0,
        "resultRecordCount": page_size,
    })

    while True:
        params["resultOffset"] = offset
        query_url = f"{layer_url}/query"

        response = session.safe_get(query_url, params=params, timeout=60)
        request_count += 1

        if not response:
            log.warning(f"[REST] Failed to query {layer_name} at offset {offset}")
            break

        if not validate_response_content(response):
            log.warning(f"[REST] Invalid query response for {layer_name} at offset {offset}")
            break

        data = safe_json_parse(response.content)
        if not data:
            log.warning(f"[REST] Failed to parse query response for {layer_name} at offset {offset}")
            break

        # Check for transfer limit exceeded
        exceeded_transfer_limit = data.get("exceededTransferLimit", False)
        features = data.get("features", [])

        if features:
            all_features.extend(features)
            log.debug(f"[REST] {layer_name} page {page_num}: {len(features)} features (offset {offset})")
            page_num += 1

        # Stop conditions per acceptance criteria:
        # 1. No features returned
        # 2. Page size is short (less than requested) AND exceededTransferLimit is False
        if not features:
            log.debug(f"[REST] {layer_name}: no more features, stopping pagination")
            break
        elif len(features) < page_size and not exceeded_transfer_limit:
            log.debug(f"[REST] {layer_name}: short page ({len(features)} < {page_size}) and no transfer limit, stopping")
            break
        elif exceeded_transfer_limit and len(features) == page_size:
            # Continue paging while transfer limit is exceeded and page is full
            log.debug(f"[REST] {layer_name}: transfer limit exceeded, continuing pagination")
        elif exceeded_transfer_limit and len(features) < page_size:
            # This shouldn't normally happen, but raise error to trigger OID pagination
            log.warning(f"[REST] {layer_name}: transfer limit exceeded with short page, switching to OID pagination")
            raise TransferLimitExceededError("Transfer limit exceeded with incomplete results")

        offset += page_size

        # Safety guard against infinite loops
        if offset > 1000000:  # Increased from 100k to 1M for large datasets
            log.warning(f"[REST] {layer_name}: stopping at {offset} features (safety limit)")
            break

    return all_features, request_count


def _download_with_oid_pagination(
    session: RecursionSafeSession,
    layer_url: str,
    base_params: Dict,
    oid_field: str,
    layer_name: str
) -> Tuple[List[Dict], int]:
    """Download features using OID-based pagination for large datasets."""
    log.info(f"[REST] {layer_name}: using OID-based pagination with field '{oid_field}'")

    # First, get all object IDs
    oid_params = base_params.copy()
    oid_params.update({
        "returnIdsOnly": "true",
        "f": "json"  # Use JSON for IDs, not GeoJSON
    })

    query_url = f"{layer_url}/query"
    response = session.safe_get(query_url, params=oid_params, timeout=60)
    request_count = 1

    if not response or not validate_response_content(response):
        log.warning(f"[REST] {layer_name}: failed to get object IDs for OID pagination")
        return [], request_count

    oid_data = safe_json_parse(response.content)
    if not oid_data:
        log.warning(f"[REST] {layer_name}: failed to parse object IDs response")
        return [], request_count

    # Extract object IDs
    object_ids = oid_data.get("objectIds", [])
    if not object_ids:
        log.info(f"[REST] {layer_name}: no object IDs found")
        return [], request_count

    log.info(f"[REST] {layer_name}: found {len(object_ids)} object IDs, fetching in batches")

    # Download features in batches using object IDs
    all_features = []
    batch_num = 1
    batch_size = 1000  # Number of object IDs to fetch per batch

    # Prepare parameters for feature queries - use the same format as base_params
    feature_params = base_params.copy()

    for i in range(0, len(object_ids), batch_size):
        batch_ids = object_ids[i:i + batch_size]
        oid_where = f"{oid_field} IN ({','.join(map(str, batch_ids))})"

        # Combine with existing where clause if present
        original_where = feature_params.get("where", "1=1")
        if original_where and original_where != "1=1":
            feature_params["where"] = f"({original_where}) AND {oid_where}"
        else:
            feature_params["where"] = oid_where

        response = session.safe_get(query_url, params=feature_params, timeout=60)
        request_count += 1

        if not response or not validate_response_content(response):
            log.warning(f"[REST] {layer_name}: failed to fetch OID batch {batch_num}")
            continue

        data = safe_json_parse(response.content)
        if not data:
            log.warning(f"[REST] {layer_name}: failed to parse OID batch {batch_num}")
            continue

        features = data.get("features", [])
        if features:
            all_features.extend(features)
            log.debug(f"[REST] {layer_name} OID batch {batch_num}: {len(features)} features")

        batch_num += 1

    return all_features, request_count


def _rest_get_all_oids(
    session: RecursionSafeSession,
    layer_url: str,
    base_params: Dict,
    layer_name: str
) -> Tuple[List[int], str, int]:
    """
    Get all object IDs from a REST layer.

    Returns:
        Tuple of (object_ids, oid_field_name, request_count)
    """
    log.debug(f"[REST] {layer_name}: discovering object IDs")

    # Get all object IDs
    oid_params = base_params.copy()
    oid_params.update({
        "returnIdsOnly": "true",
        "f": "json"  # Use JSON for IDs, not GeoJSON
    })

    query_url = f"{layer_url}/query"
    response = session.safe_get(query_url, params=oid_params, timeout=60)
    request_count = 1

    if not response or not validate_response_content(response):
        log.warning(f"[REST] {layer_name}: failed to get object IDs")
        return [], "OBJECTID", request_count

    oid_data = safe_json_parse(response.content)
    if not oid_data:
        log.warning(f"[REST] {layer_name}: failed to parse object IDs response")
        return [], "OBJECTID", request_count

    # Extract object IDs and field name
    object_ids = oid_data.get("objectIds", [])
    oid_field = oid_data.get("objectIdFieldName", "OBJECTID")

    log.info(f"[REST] {layer_name}: discovered {len(object_ids)} object IDs (field: {oid_field})")
    return object_ids, oid_field, request_count


def _rest_fetch_oid_batch(
    session: RecursionSafeSession,
    layer_url: str,
    base_params: Dict,
    oid_field: str,
    batch_ids: List[int],
    batch_num: int,
    layer_name: str
) -> Tuple[List[Dict], bool, int]:
    """
    Fetch a single batch of features by object IDs.

    Returns:
        Tuple of (features, success, request_count)
    """
    # Build OID WHERE clause
    oid_where = f"{oid_field} IN ({','.join(map(str, batch_ids))})"

    # Prepare parameters for feature queries - use the same format as base_params
    feature_params = base_params.copy()

    # Combine with existing where clause if present
    original_where = feature_params.get("where", "1=1")
    if original_where and original_where != "1=1":
        feature_params["where"] = f"({original_where}) AND {oid_where}"
    else:
        feature_params["where"] = oid_where

    query_url = f"{layer_url}/query"

    # Handle potential retry after
    retry_count = 0
    max_retries = 3

    while retry_count <= max_retries:
        response = session.safe_get(query_url, params=feature_params, timeout=60)

        if not response or not validate_response_content(response):
            retry_count += 1
            if retry_count <= max_retries:
                wait_time = 2 ** retry_count  # Exponential backoff
                log.debug(f"[REST] {layer_name} batch {batch_num}: retry {retry_count} after {wait_time}s")
                time.sleep(wait_time)
                continue
            else:
                log.warning(f"[REST] {layer_name}: failed to fetch OID batch {batch_num} after {max_retries} retries")
                return [], False, retry_count

        # Check for Retry-After header (server back-pressure)
        if hasattr(response, 'headers') and 'Retry-After' in response.headers:
            retry_after = response.headers.get('Retry-After')
            try:
                if retry_after is not None:
                    wait_time = int(retry_after)
                    if wait_time > 0 and wait_time <= MAX_RETRY_AFTER_SECONDS:  # Reasonable limit
                        log.info(f"[REST] {layer_name} batch {batch_num}: server requested {wait_time}s delay")
                        time.sleep(wait_time)
            except (ValueError, TypeError):
                pass  # Invalid Retry-After value, ignore

        data = safe_json_parse(response.content)
        if not data:
            retry_count += 1
            if retry_count <= max_retries:
                wait_time = 2 ** retry_count
                log.debug(f"[REST] {layer_name} batch {batch_num}: parse error, retry {retry_count} after {wait_time}s")
                time.sleep(wait_time)
                continue
            else:
                log.warning(f"[REST] {layer_name}: failed to parse OID batch {batch_num}")
                return [], False, retry_count

        features = data.get("features", [])
        log.debug(f"[REST] {layer_name} batch {batch_num}: {len(features)} features")
        return features, True, retry_count + 1

    return [], False, retry_count


def fetch_rest_layer_parallel(
    session: RecursionSafeSession,
    layer_url: str,
    base_params: Dict,
    layer_name: str,
    page_size: int = 1000,
    max_workers: int = 6
) -> Tuple[List[Dict], Dict[str, int]]:
    """
    Download features using parallel OID-based pagination.

    Returns:
        Tuple of (all_features, metrics_dict)
    """
    log.info(f"[REST] {layer_name}: using parallel OID-based pagination (page_size={page_size}, max_workers={max_workers})")

    # Initialize metrics
    metrics = {
        "oids_total": 0,
        "batches_total": 0,
        "batches_ok": 0,
        "features_total": 0,
        "request_count": 0
    }

    # Step 1: Get all object IDs
    object_ids, oid_field, request_count = _rest_get_all_oids(session, layer_url, base_params, layer_name)
    metrics["request_count"] += request_count
    metrics["oids_total"] = len(object_ids)

    if not object_ids:
        log.info(f"[REST] {layer_name}: no object IDs found")
        return [], metrics

    # Step 2: Create batches
    batches = []
    for i in range(0, len(object_ids), page_size):
        batch_ids = object_ids[i:i + page_size]
        batches.append((batch_ids, i // page_size + 1))

    metrics["batches_total"] = len(batches)
    log.info(f"[REST] {layer_name}: created {len(batches)} batches, fetching with {max_workers} workers")

    # Step 3: Fetch batches in parallel
    all_features = []

    # Use conservative max_workers to avoid overwhelming the server
    actual_max_workers = min(max_workers, MAX_CONCURRENT_REQUESTS)  # Hard cap at MAX_CONCURRENT_REQUESTS concurrent requests

    with ThreadPoolExecutor(max_workers=actual_max_workers) as executor:
        # Submit all batch fetch tasks
        future_to_batch = {}
        for batch_ids, batch_num in batches:
            future = executor.submit(
                _rest_fetch_oid_batch,
                session, layer_url, base_params, oid_field, batch_ids, batch_num, layer_name
            )
            future_to_batch[future] = batch_num

        # Collect results as they complete
        for future in as_completed(future_to_batch):
            batch_num = future_to_batch[future]
            try:
                features, success, batch_request_count = future.result()
                metrics["request_count"] += batch_request_count

                if success:
                    metrics["batches_ok"] += 1
                    if features:
                        all_features.extend(features)
                        metrics["features_total"] += len(features)
                        log.debug(f"[REST] {layer_name}: completed batch {batch_num} with {len(features)} features")
                else:
                    log.warning(f"[REST] {layer_name}: failed batch {batch_num}")

            except Exception as e:
                log.error(f"[REST] {layer_name}: batch {batch_num} raised exception: {e}")

    log.info(f"[REST] {layer_name}: parallel fetch completed - {metrics['batches_ok']}/{metrics['batches_total']} batches successful, {metrics['features_total']} total features in {metrics['request_count']} requests")

    return all_features, metrics


def coerce_bbox4(bbox: Optional[Sequence[float]]) -> Optional[BBox]:
    """Ensure bbox is exactly 4 numbers (xmin, ymin, xmax, ymax)."""
    if bbox is None:
        return None
    if len(bbox) != 4:
        raise ValueError(f"bbox must have 4 elements [xmin, ymin, xmax, ymax], got {len(bbox)}")
    xmin = float(bbox[0])
    ymin = float(bbox[1])
    xmax = float(bbox[2])
    ymax = float(bbox[3])
    return (xmin, ymin, xmax, ymax)
