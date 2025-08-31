"""
REST API downloader for OP-ETL pipeline.
Enhanced implementation with recursion depth protection and SR consistency.
"""

import contextlib
import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .http_utils import RecursionSafeSession, safe_json_parse, validate_response_content
from .monitoring import end_monitoring_source, start_monitoring_source
from .sr_utils import SWEREF99_TM

log = logging.getLogger(__name__)

# Constants
MAX_CONCURRENT_REQUESTS = 8
MAX_RETRY_AFTER_SECONDS = 30

# Fixed type alias - explicit 4-tuple
BBox = Tuple[float, float, float, float]


class TransferLimitExceededError(Exception):
    """Raised when REST service hits transfer limits."""
    pass


def sanitize_layer_name(name: str) -> str:
    """Sanitize layer name for filesystem."""
    if not name:
        return "unknown_layer"

    sanitized = re.sub(r'[<>:"/\\|?*\x00-\x1f\x7f-\x9f]', "_", name)
    sanitized = re.sub(r"\s+", "_", sanitized).strip("._")

    return sanitized[:200] if len(sanitized) > 200 else sanitized or "unknown_layer"


def _extract_global_bbox(cfg: dict) -> Tuple[Optional[Sequence[float | int]], Optional[int]]:
    """Extract global bbox configuration."""
    if not cfg.get("use_bbox_filter", False):
        return None, None

    gb = cfg.get("global_bbox", {})
    coords = gb.get("coords")
    crs = gb.get("crs")

    sr = None
    if isinstance(crs, int):
        sr = crs
    elif isinstance(crs, str):
        crs_upper = crs.upper()
        if crs_upper in ("WGS84", "CRS84"):
            sr = 4326
        elif crs_upper.startswith("EPSG:"):
            with contextlib.suppress(ValueError, IndexError):
                sr = int(crs_upper.split(":", 1)[1])
    return coords, sr

def coerce_bbox4(bbox: Optional[Sequence[float | int]]) -> Optional[BBox]:
    """Convert any indexable 4+ sequence of numbers to a 4-tuple bbox of floats."""
    if bbox is None or len(bbox) < 4:
        return None
    x0, y0, x1, y1 = bbox[0], bbox[1], bbox[2], bbox[3]
    return float(x0), float(y0), float(x1), float(y1)


def build_rest_params(cfg: Dict[str, Any], bbox_3006: Optional[BBox] = None) -> Dict[str, Any]:
    """Build REST API parameters."""
    fmt = (cfg.get("response_format") or cfg.get("format") or "json").lower()

    params = {
        "where": cfg.get("where") or cfg.get("where_clause") or "1=1",
        "outFields": cfg.get("out_fields") or "*",
        "returnGeometry": "true",
        "f": "geojson" if fmt == "geojson" else "json"
    }

    if fmt != "geojson":
        params["outSR"] = cfg.get("out_sr") or cfg.get("stage_sr") or SWEREF99_TM

    if bbox_3006:
        xmin, ymin, xmax, ymax = bbox_3006
        params |= {
            "geometry": json.dumps({
                "xmin": xmin, "ymin": ymin,
                "xmax": xmax, "ymax": ymax,
                "spatialReference": {"wkid": SWEREF99_TM}
            }),
            "geometryType": "esriGeometryEnvelope",
            "inSR": SWEREF99_TM,
            "spatialRel": "esriSpatialRelIntersects"
        }
        log.debug(f"[REST] Using bbox: {bbox_3006}")

    return params


def diagnose_rest_response(layer_url: str, raw_config: Dict) -> None:
    """Debug REST API responses."""
    session = RecursionSafeSession()

    try:
        if response := session.safe_get(
            f"{layer_url}/query",
            params={"where": "1=1", "returnCountOnly": "true", "f": "json"},
            timeout=30,
        ):
            if data := safe_json_parse(response.content):
                log.info(f"[REST DEBUG] Total features: {data.get('count', 0)}")

        # Test with bbox
        bbox_raw = raw_config.get("bbox") or [585826, 6550189, 648593, 6611661]
        if bbox_3006 := coerce_bbox4(bbox_raw):
            params = build_rest_params(raw_config, bbox_3006) | {"returnCountOnly": "true"}

            response = session.safe_get(f"{layer_url}/query", params=params, timeout=30)
            if response and (data := safe_json_parse(response.content)):
                count = data.get("count", 0)
                log.info(f"[REST DEBUG] Features in bbox: {count}")
                if count == 0:
                    log.warning(f"[REST DEBUG] BBOX excludes all features: {bbox_3006}")

    except Exception as e:
        log.error(f"[REST DEBUG] Failed: {e}")


def run(cfg: dict) -> None:
    """Process all REST API sources."""
    global_bbox, global_sr = _extract_global_bbox(cfg)

    rest_sources = [
        {
            "name": source.get("name"),
            "url": source.get("url"),
            "authority": source.get("authority", "unknown"),
            "raw": source.get("raw", {}).copy() if source.get("raw") else {}
        }
        for source in cfg.get("sources", [])
        if source.get("type") == "rest" and source.get("enabled", True)
    ]

    if not rest_sources:
        log.info("[REST] No REST sources to process")
        return

    downloads_dir = Path(cfg["workspaces"]["downloads"])

    for source in rest_sources:
        start_monitoring_source(source["name"], source["authority"], "rest")

        try:
            log.info(f"[REST] Processing {source['name']}")
            success, feature_count = process_rest_source(source, downloads_dir, global_bbox, global_sr)
            end_monitoring_source(success, features=feature_count)
        except Exception as e:
            log.error(f"[REST] Failed {source['name']}: {e}")
            end_monitoring_source(False, type(e).__name__, str(e))


def process_rest_source(
    source: Dict,
    downloads_dir: Path,
    global_bbox: Optional[Sequence[float | int]],
    global_sr: Optional[int]
) -> Tuple[bool, int]:
    """Process single REST source."""
    base_url = source["url"].rstrip("/")
    authority = source["authority"]
    name = source["name"]
    raw = source.get("raw", {})

    out_dir = downloads_dir / authority / name
    out_dir.mkdir(parents=True, exist_ok=True)

    if layer_ids := raw.get("layer_ids", []):
        all_discovered = discover_layers(base_url)
        discovered_by_id = {layer["id"]: layer for layer in all_discovered}

        layer_info = [
            discovered_by_id.get(lid, {"id": lid, "name": f"layer_{lid}"})
            for lid in layer_ids
        ]

    else:
        layer_info = discover_layers(base_url, raw.get("include"))
        if not layer_info:
            log.info(f"[REST] No layers found in {name}")
            return True, 0
    total_features = 0

    for layer in layer_info:
        layer_id = layer["id"]
        layer_name = sanitize_layer_name(layer["name"])

        try:
            layer_url = f"{base_url}/{layer_id}"
            feature_count = download_layer(layer_url, out_dir, layer_name, raw, global_bbox, global_sr)
            total_features += feature_count
            log.info(f"[REST] Layer {layer_id} ({layer_name}): {feature_count} features")
        except Exception as e:
            log.warning(f"[REST] Failed layer {layer_id}: {e}")

    log.info(f"[REST] Total features from {name}: {total_features}")
    return total_features > 0, total_features


def discover_layers(base_url: str, include: List[str] | None = None) -> List[Dict[str, Any]]:
    """Discover available layers."""
    import fnmatch
    session = RecursionSafeSession()

    try:
        return _extracted_from_discover_layers_7(session, base_url, include, fnmatch)
    except Exception as e:
        log.warning(f"[REST] Failed to discover layers: {e}")
        return []


# TODO Rename this here and in `discover_layers`
def _extracted_from_discover_layers_7(session, base_url, include, fnmatch):
    response = session.safe_get(base_url, params={"f": "json"}, timeout=30)

    if not response or not validate_response_content(response):
        return []

    if not (data := safe_json_parse(response.content)):
        return []

    layer_info = []
    layers = data.get("layers", [])

    patterns = [p.lower() for p in include] if include else None
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
        layer_id = data.get("id", 0)
        layer_name = data.get("name", f"layer_{layer_id}")
        layer_info.append({"id": layer_id, "name": layer_name})

    log.info(f"[REST] Discovered {len(layer_info)} layers")
    return layer_info


def download_layer(
    layer_url: str,
    out_dir: Path,
    layer_name: str,
    raw_config: Dict,
    global_bbox: Optional[Sequence[float | int]],
    global_sr: Optional[int]
) -> int:
    """Download all features from a REST layer."""
    session = RecursionSafeSession()

    try:
        log.info(f"[REST] Downloading: {layer_url}")

        # Get layer info
        response = session.safe_get(layer_url, params={"f": "json"}, timeout=30)

        if not response or not validate_response_content(response):
            return 0

        if not (layer_info := safe_json_parse(response.content)):
            return 0

        if not layer_info.get("supportsQuery", True):
            log.warning("[REST] Layer doesn't support queries")
            return 0

        # Prepare bbox
        bbox_raw = raw_config.get("bbox") or global_bbox
        bbox_3006 = coerce_bbox4(bbox_raw) if bbox_raw else None

        # Build parameters
        base_params = build_rest_params(raw_config, bbox_3006)

        # Download features
        all_features = []
        request_count = 0

        # Try offset pagination first
        try:
            all_features, request_count = _download_with_offset_pagination(
                session, layer_url, base_params, layer_name
            )
        except TransferLimitExceededError:
            # Fall back to OID pagination if supported
            if layer_info.get("supportsAdvancedQueries", False):
                log.info("[REST] Switching to OID pagination")
                oid_field = layer_info.get("objectIdField", "OBJECTID")
                all_features, request_count = _download_with_oid_pagination(
                    session, layer_url, base_params, oid_field, layer_name
                )

        # Save results
        out_file = out_dir / f"{layer_name}.geojson"
        geojson = {"type": "FeatureCollection", "features": all_features}

        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(geojson, f, ensure_ascii=False, separators=(",", ":"))

        if not all_features:
            log.warning(f"[REST] No features found for {layer_name}")
            diagnose_rest_response(layer_url, raw_config)
        else:
            log.info(f"[REST] Saved {len(all_features)} features")

        return len(all_features)

    except Exception as e:
        log.error(f"[REST] Failed to download: {e}")
        return 0


def _download_with_offset_pagination(
    session: RecursionSafeSession,
    layer_url: str,
    base_params: Dict,
    layer_name: str
) -> Tuple[List[Dict], int]:
    """Download using offset pagination."""
    all_features = []
    offset = 0
    page_size = 1000
    request_count = 0

    params = base_params | {
        "resultOffset": 0,
        "resultRecordCount": page_size
    }

    query_url = f"{layer_url}/query"

    while True:
        params["resultOffset"] = offset
        response = session.safe_get(query_url, params=params, timeout=60)
        request_count += 1

        if not response or not validate_response_content(response):
            break

        if not (data := safe_json_parse(response.content)):
            break

        features = data.get("features", [])
        exceeded_limit = data.get("exceededTransferLimit", False)

        if features:
            all_features.extend(features)
            log.debug(f"[REST] Page {request_count}: {len(features)} features")

        # Stop conditions
        if not features:
            break
        elif len(features) < page_size and not exceeded_limit:
            break
        elif exceeded_limit and len(features) < page_size:
            raise TransferLimitExceededError()

        offset += page_size

        if offset > 1000000:
            log.warning("[REST] Safety limit reached")
            break

    return all_features, request_count


def _download_with_oid_pagination(
    session: RecursionSafeSession,
    layer_url: str,
    base_params: Dict,
    oid_field: str,
    layer_name: str
) -> Tuple[List[Dict], int]:
    """Download using OID pagination."""
    query_url = f"{layer_url}/query"

    # Get all OIDs
    oid_params = base_params.copy()
    oid_params |= {"returnIdsOnly": "true", "f": "json"}

    response = session.safe_get(query_url, params=oid_params, timeout=60)
    request_count = 1

    if not response or not (data := safe_json_parse(response.content)):
        return [], request_count

    object_ids = data.get("objectIds", [])
    if not object_ids:
        return [], request_count

    log.info(f"[REST] Found {len(object_ids)} OIDs")

    # Fetch in batches
    all_features = []
    batch_size = 1000

    for i in range(0, len(object_ids), batch_size):
        batch_ids = object_ids[i:i + batch_size]
        oid_where = f"{oid_field} IN ({','.join(map(str, batch_ids))})"

        feature_params = base_params.copy()
        original_where = feature_params.get("where", "1=1")
        feature_params["where"] = f"({original_where}) AND {oid_where}" if original_where != "1=1" else oid_where

        response = session.safe_get(query_url, params=feature_params, timeout=60)
        request_count += 1

        if response and (data := safe_json_parse(response.content)):
            if features := data.get("features", []):
                all_features.extend(features)

    return all_features, request_count
