"""
REST API downloader for OP-ETL pipeline.
Enhanced implementation with recursion depth protection.
"""

import logging
import json
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

from .http_utils import RecursionSafeSession, safe_json_parse, validate_response_content
from .monitoring import start_monitoring_source, end_monitoring_source

log = logging.getLogger(__name__)


def sanitize_layer_name(name: str) -> str:
    """Sanitize layer name to make it safe for use as a filename."""
    if not name:
        return "unknown_layer"

    # Replace problematic characters with safe alternatives
    sanitized = re.sub(r'[<>:"/\\|?*]', "_", name)  # Windows problematic chars: < > : " / \ | ? *
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
            # Try with just layer 0 as fallback
            layer_info = [{"id": 0, "name": "layer_0"}]
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
        try:
            layer_id = layer["id"]
            layer_name = layer["name"]
            sanitized_name = sanitize_layer_name(layer_name)

            layer_url = f"{base_url}/{layer_id}"
            feature_count = download_layer(layer_url, out_dir, sanitized_name, raw, global_bbox, global_sr)
            total_features += feature_count
            log.info(f"[REST] Layer {layer_id} ({layer_name}): {feature_count} features")
        except Exception as e:
            log.warning(f"[REST] Failed to download layer {layer_id} ({layer.get('name', 'unknown')}): {e}")

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
    """Download all features from a REST layer with enhanced error handling."""
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

        # Build query parameters
        params = {
            "f": "geojson",
            "where": raw_config.get("where", "1=1"),
            "outFields": raw_config.get("out_fields", "*"),
            "returnGeometry": "true",
            "resultOffset": 0,
            "resultRecordCount": 1000,
        }

        # Add bbox if configured (prefer raw, else global)
        bbox = raw_config.get("bbox") or global_bbox
        if bbox and len(bbox) >= 4:
            params["geometry"] = f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}"
            params["geometryType"] = "esriGeometryEnvelope"
            in_sr = raw_config.get("bbox_sr") or global_sr or 4326
            params["inSR"] = in_sr

        # Download features with pagination
        all_features = []
        offset = 0
        page_size = 1000

        while True:
            params["resultOffset"] = offset

            query_url = f"{layer_url}/query"
            response = session.safe_get(query_url, params=params, timeout=60)

            if not response:
                log.warning(f"[REST] Failed to query layer at offset {offset}")
                break

            if not validate_response_content(response):
                log.warning(f"[REST] Invalid query response at offset {offset}")
                break

            data = safe_json_parse(response.content)
            if not data:
                log.warning(f"[REST] Failed to parse query response at offset {offset}")
                break

            features = data.get("features", [])

            if not features:
                break

            all_features.extend(features)
            log.debug(f"[REST] Downloaded {len(features)} features (offset {offset})")

            # Check if we got all features
            if len(features) < page_size:
                break

            offset += page_size

            # Avoid infinite loops
            if offset > 100000:
                log.warning(f"[REST] Stopping at {offset} features (safety limit)")
                break

        # Save all features as GeoJSON
        if all_features:
            geojson = {"type": "FeatureCollection", "features": all_features}

            out_file = out_dir / f"{layer_name}.geojson"
            with open(out_file, "w", encoding="utf-8") as f:
                json.dump(geojson, f, ensure_ascii=False, separators=(",", ":"))

            log.info(f"[REST] Saved {len(all_features)} features to {out_file.name}")

        return len(all_features)

    except RecursionError as e:
        log.error(f"[REST] Recursion error downloading layer: {e}")
        return 0
    except Exception as e:
        log.error(f"[REST] Failed to download layer: {e}")
        return 0
