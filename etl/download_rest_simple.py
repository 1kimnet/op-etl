"""
Simplified REST API downloader for OP-ETL focused on maintainability.
Handles the 90% use case with clear, straightforward code.
"""

import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, List

from .http_simple import RecursionSafeSession, safe_json_parse, validate_response_content

logger = logging.getLogger(__name__)

# Simple constants
DEFAULT_RECORD_COUNT = 1000
MAX_RECORDS_PER_REQUEST = 2000


def sanitize_layer_name(name: str) -> str:
    """Sanitize layer name for filesystem."""
    if not name:
        return "unknown_layer"

    # Remove invalid filesystem characters
    sanitized = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", name)
    sanitized = re.sub(r"\s+", "_", sanitized).strip("._")

    # Truncate if too long
    return sanitized[:200] if len(sanitized) > 200 else sanitized or "unknown_layer"


def run(cfg: dict) -> None:
    """Process all REST API sources."""
    logger.info("[REST] Starting REST downloads...")

    # Find REST sources
    rest_sources = [
        source for source in cfg.get("sources", [])
        if source.get("type") == "rest" and source.get("enabled", True)
    ]

    if not rest_sources:
        logger.info("[REST] No REST sources to process")
        return

    downloads_dir = Path(cfg["workspaces"]["downloads"])

    for source in rest_sources:
        try:
            logger.info(f"[REST] Processing {source.get('name', 'Unknown')}")
            process_rest_source(source, downloads_dir)
        except Exception as e:
            logger.error(f"[REST] Failed {source.get('name', 'Unknown')}: {e}")


def process_rest_source(source: Dict, downloads_dir: Path) -> None:
    """Process single REST source."""
    base_url = source["url"].rstrip("/")
    authority = source.get("authority", "unknown")
    name = source.get("name", "unknown")

    # Create output directory
    out_dir = downloads_dir / authority / name
    out_dir.mkdir(parents=True, exist_ok=True)

    # Discover layers
    layers = discover_layers(base_url)

    if not layers:
        logger.warning(f"[REST] No layers found at {base_url}")
        return

    # Download each layer
    for layer in layers:
        try:
            download_layer(layer, out_dir)
        except Exception as e:
            logger.error(f"[REST] Failed to download layer {layer.get('name', 'unknown')}: {e}")


def discover_layers(base_url: str) -> List[Dict[str, Any]]:
    """Discover available layers from REST service."""
    session = RecursionSafeSession()

    try:
        # Try to get service metadata
        response = session.safe_get(f"{base_url}?f=json")

        if not response or not validate_response_content(response):
            logger.error(f"[REST] Failed to get service metadata from {base_url}")
            return []

        service_data = safe_json_parse(response.data)
        if not service_data:
            logger.error(f"[REST] Invalid JSON from {base_url}")
            return []

        layers = []

        # Extract layers from service metadata
        for layer_info in service_data.get("layers", []):
            layer_id = layer_info.get("id")
            layer_name = layer_info.get("name", f"layer_{layer_id}")

            if layer_id is not None:
                layers.append({
                    "id": layer_id,
                    "name": sanitize_layer_name(layer_name),
                    "url": f"{base_url}/{layer_id}"
                })

        logger.info(f"[REST] Discovered {len(layers)} layers")
        return layers

    except Exception as e:
        logger.error(f"[REST] Error discovering layers from {base_url}: {e}")
        return []


def download_layer(layer: Dict[str, Any], out_dir: Path) -> None:
    """Download a single layer to GeoJSON."""
    layer_url = layer["url"]
    layer_name = layer["name"]

    logger.info(f"[REST] Downloading layer: {layer_name}")

    session = RecursionSafeSession()

    # Build query parameters for GeoJSON output
    params = {
        "where": "1=1",  # Get all features
        "outFields": "*",  # Get all fields
        "f": "geojson",  # Output format
        "outSR": 3006,  # SWEREF99 TM
        "resultRecordCount": DEFAULT_RECORD_COUNT
    }

    # Try to download all features
    all_features = []
    offset = 0
    has_more = True

    while has_more:
        try:
            # Add offset for pagination
            query_params = params.copy()
            query_params["resultOffset"] = offset

            # Build query string
            query_string = "&".join([f"{k}={v}" for k, v in query_params.items()])
            url = f"{layer_url}/query?{query_string}"

            logger.debug(f"[REST] Requesting: {url}")

            response = session.safe_get(url)

            if not response or not validate_response_content(response):
                logger.error(f"[REST] Failed to get data from {url}")
                break

            data = safe_json_parse(response.data)
            if not data:
                logger.error(f"[REST] Invalid JSON response from {url}")
                break

            # Extract features
            features = data.get("features", [])
            if not features:
                logger.info("[REST] No more features found, stopping pagination")
                break

            all_features.extend(features)

            # Check if there are more features
            if len(features) < DEFAULT_RECORD_COUNT:
                has_more = False
            else:
                offset += len(features)
                logger.info(f"[REST] Downloaded {len(all_features)} features so far...")

            # Safety limit to prevent infinite loops
            if len(all_features) > 100000:  # 100k features
                logger.warning("[REST] Hit safety limit of 100k features, stopping")
                break

        except Exception as e:
            logger.error(f"[REST] Error downloading features: {e}")
            break

    if not all_features:
        logger.warning(f"[REST] No features downloaded for {layer_name}")
        return

    # Create GeoJSON structure
    geojson = {
        "type": "FeatureCollection",
        "features": all_features
    }

    # Write to file
    output_file = out_dir / f"{layer_name}.geojson"
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(geojson, f, ensure_ascii=False, indent=2)

        logger.info(f"[REST] Saved {len(all_features)} features to {output_file}")

    except Exception as e:
        logger.error(f"[REST] Failed to save {output_file}: {e}")


def build_rest_params(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Build basic REST parameters - simplified version."""
    return {
        "where": "1=1",
        "outFields": "*",
        "f": "geojson",
        "outSR": 3006
    }
