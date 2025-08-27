"""
REST API downloader for OP-ETL pipeline.
Enhanced implementation with recursion depth protection and SR consistency.
"""

import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .http_utils import RecursionSafeSession, safe_json_parse, validate_response_content
from .monitoring import end_monitoring_source, start_monitoring_source
from .sr_utils import (
    SWEREF99_TM, get_sr_config_for_source, 
    validate_sr_consistency, validate_bbox_vs_envelope,
    log_sr_validation_summary
)

log = logging.getLogger(__name__)


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

        # Get SR configuration for source (enforce best practices)
        source_info = {"type": "rest", "raw": raw_config}
        sr_config = get_sr_config_for_source(source_info)
        
        # Build base query parameters with enforced SR consistency
        base_params = {
            "f": "geojson",
            "where": raw_config.get("where", "1=1"),
            "outFields": raw_config.get("out_fields", "*"),
            "returnGeometry": "true",
            # Enforce SR 3006 for REST APIs (best practice)
            "inSR": sr_config.get("in_sr", SWEREF99_TM),
            "outSR": sr_config.get("out_sr", SWEREF99_TM),
        }

        # Add bbox if configured (prefer raw, else global) - express in SR 3006
        bbox = raw_config.get("bbox") or global_bbox
        bbox_sr = sr_config.get("bbox_sr", SWEREF99_TM)
        if bbox and len(bbox) >= 4:
            base_params["geometry"] = f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}"
            base_params["geometryType"] = "esriGeometryEnvelope"
            base_params["geometrySR"] = bbox_sr
            
        log.info(f"[REST] Using SR config - bbox_sr: {bbox_sr}, inSR: {base_params['inSR']}, outSR: {base_params['outSR']}")

        # Check if the service supports OID-based pagination: must support advanced queries and have an objectIdField
        supports_oids = layer_info.get("supportsAdvancedQueries", False) and bool(layer_info.get("objectIdField"))
        oid_field = layer_info.get("objectIdField", "OBJECTID")

        # Try offset-based pagination first, fall back to OID-based if transfer limits hit
        all_features = []
        request_count = 0

        try:
            all_features, request_count = _download_with_offset_pagination(
                session, layer_url, base_params, layer_name, sr_config, bbox
            )
        except TransferLimitExceededError:
            if supports_oids:
                log.info(f"[REST] Transfer limit exceeded, switching to OID-based pagination for {layer_name}")
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
    sr_config: Dict,
    bbox: Optional[List[float]]
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

        # Validate SR consistency on first page
        if offset == 0:
            expected_sr = sr_config.get("out_sr", SWEREF99_TM)
            sr_valid, detected_sr = validate_sr_consistency(data, expected_sr)
            if not sr_valid:
                log.warning(f"[REST] SR validation failed - expected {expected_sr}, detected {detected_sr}")
            
            # Validate bbox vs envelope if applicable
            if bbox and 'extent' in data:
                bbox_valid = validate_bbox_vs_envelope(bbox, data['extent'])
                if not bbox_valid:
                    log.warning("Bbox validation failed")

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

    # Prepare parameters for feature queries
    feature_params = base_params.copy()
    feature_params["f"] = "geojson"  # Back to GeoJSON for actual features

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
