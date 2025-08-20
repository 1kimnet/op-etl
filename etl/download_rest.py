"""
REST API downloader for OP-ETL pipeline.
Fixed to avoid recursion issues.
"""

import logging
import json
from pathlib import Path
from typing import Dict, List
import requests

log = logging.getLogger(__name__)


def run(cfg: dict) -> None:
    """Process all REST API sources."""
    # Extract sources cleanly
    rest_sources = []
    for source in cfg.get("sources", []):
        if source.get("type") == "rest" and source.get("enabled", True):
            # Create clean copy
            rest_sources.append({
                "name": source.get("name"),
                "url": source.get("url"),
                "authority": source.get("authority", "unknown"),
                "raw": source.get("raw", {}).copy() if source.get("raw") else {}
            })

    if not rest_sources:
        log.info("[REST] No REST sources to process")
        return

    downloads_dir = Path(cfg["workspaces"]["downloads"])

    for source in rest_sources:
        try:
            log.info(f"[REST] Processing {source['name']}")
            process_rest_source(source, downloads_dir)
        except Exception as e:
            log.error(f"[REST] Failed {source['name']}: {e}")


def process_rest_source(source: Dict, downloads_dir: Path) -> bool:
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

    # If no layer IDs specified, discover them
    if not layer_ids:
        layer_ids = discover_layers(base_url)
        if not layer_ids:
            # Try with just layer 0
            layer_ids = [0]

    total_features = 0

    for layer_id in layer_ids:
        try:
            layer_url = f"{base_url}/{layer_id}"
            feature_count = download_layer(layer_url, out_dir, f"layer_{layer_id}", raw)
            total_features += feature_count
            log.info(f"[REST] Layer {layer_id}: {feature_count} features")
        except Exception as e:
            log.warning(f"[REST] Failed to download layer {layer_id}: {e}")

    log.info(f"[REST] Total features from {name}: {total_features}")
    return total_features > 0


def discover_layers(base_url: str) -> List[int]:
    """Discover available layers in the service."""
    try:
        # Query service info
        params = {"f": "json"}
        response = requests.get(base_url, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()

        # Extract layer IDs
        layer_ids = []
        layers = data.get("layers", [])
        for layer in layers:
            if isinstance(layer, dict) and "id" in layer:
                layer_ids.append(layer["id"])

        return layer_ids

    except Exception as e:
        log.warning(f"[REST] Failed to discover layers: {e}")
        return []


def download_layer(layer_url: str, out_dir: Path, layer_name: str, raw_config: Dict) -> int:
    """Download all features from a REST layer."""
    try:
        # Get layer info first
        info_params = {"f": "json"}
        info_response = requests.get(f"{layer_url}", params=info_params, timeout=30)
        info_response.raise_for_status()
        layer_info = info_response.json()

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
            "resultRecordCount": 1000
        }

        # Add bbox if configured
        bbox = raw_config.get("bbox")
        if bbox and len(bbox) >= 4:
            params["geometry"] = f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}"
            params["geometryType"] = "esriGeometryEnvelope"
            params["inSR"] = raw_config.get("bbox_sr", 4326)

        # Download features with pagination
        all_features = []
        offset = 0
        page_size = 1000

        while True:
            params["resultOffset"] = offset

            query_url = f"{layer_url}/query"
            response = requests.get(query_url, params=params, timeout=60)
            response.raise_for_status()

            data = response.json()
            features = data.get("features", [])

            if not features:
                break

            all_features.extend(features)

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
            geojson = {
                "type": "FeatureCollection",
                "features": all_features
            }

            out_file = out_dir / f"{layer_name}.geojson"
            with open(out_file, 'w', encoding='utf-8') as f:
                json.dump(geojson, f, ensure_ascii=False, separators=(',', ':'))

            log.info(f"[REST] Saved {len(all_features)} features to {out_file.name}")

        return len(all_features)

    except Exception as e:
        log.error(f"[REST] Failed to download layer: {e}")
        return 0