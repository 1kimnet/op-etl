"""
OGC API Features downloader for OP-ETL pipeline.
Fixed to avoid recursion issues.
"""

import logging
import json
import time
from pathlib import Path
from typing import Dict, List, Optional
import requests

log = logging.getLogger(__name__)


def run(cfg: dict) -> None:
    """Process all OGC API sources."""
    # Extract sources without circular references
    ogc_sources = []
    for source in cfg.get("sources", []):
        if source.get("type") == "ogc" and source.get("enabled", True):
            # Create clean copy
            ogc_sources.append({
                "name": source.get("name"),
                "url": source.get("url"),
                "authority": source.get("authority", "unknown"),
                "raw": source.get("raw", {}).copy() if source.get("raw") else {}
            })

    if not ogc_sources:
        log.info("[OGC] No OGC sources to process")
        return

    downloads_dir = Path(cfg["workspaces"]["downloads"])

    # Get global bbox if configured
    global_bbox = None
    if cfg.get("use_bbox_filter") and cfg.get("global_ogc_bbox"):
        bbox_cfg = cfg["global_ogc_bbox"]
        if bbox_cfg.get("coords"):
            global_bbox = bbox_cfg["coords"]

    for source in ogc_sources:
        try:
            log.info(f"[OGC] Processing {source['name']}")
            process_ogc_source(source, downloads_dir, global_bbox)
        except Exception as e:
            log.error(f"[OGC] Failed {source['name']}: {e}")


def process_ogc_source(source: Dict, downloads_dir: Path, global_bbox: Optional[List] = None) -> bool:
    """Process a single OGC source."""
    base_url = source["url"]
    authority = source["authority"]
    name = source["name"]
    raw = source.get("raw", {})

    # Create output directory
    out_dir = downloads_dir / authority / name
    out_dir.mkdir(parents=True, exist_ok=True)

    # Get configuration
    collections = raw.get("collections", [])
    page_size = raw.get("page_size", 1000)
    supports_bbox_crs = raw.get("supports_bbox_crs", False)

    # Use source bbox or global bbox
    bbox = raw.get("bbox") or global_bbox

    try:
        # Auto-discover collections if none specified
        if not collections:
            collections = discover_collections(base_url)
            if not collections:
                log.warning(f"[OGC] No collections found for {name}")
                return False

        total_features = 0

        # Process each collection
        for collection in collections:
            feature_count = fetch_collection(
                base_url, collection, out_dir,
                page_size=page_size,
                bbox=bbox,
                supports_bbox_crs=supports_bbox_crs
            )
            total_features += feature_count
            log.info(f"[OGC] Collection {collection}: {feature_count} features")

        log.info(f"[OGC] Total features from {name}: {total_features}")
        return total_features > 0

    except Exception as e:
        log.error(f"[OGC] Error processing {name}: {e}")
        return False


def discover_collections(base_url: str) -> List[str]:
    """Discover available collections."""
    try:
        collections_url = f"{base_url.rstrip('/')}/collections"

        response = requests.get(collections_url, timeout=30)
        response.raise_for_status()

        data = response.json()
        collections = data.get("collections", [])

        collection_ids = []
        for coll in collections:
            if isinstance(coll, dict) and "id" in coll:
                collection_ids.append(coll["id"])

        return collection_ids

    except Exception as e:
        log.warning(f"[OGC] Failed to discover collections: {e}")
        return []


def fetch_collection(
    base_url: str,
    collection: str,
    out_dir: Path,
    page_size: int = 1000,
    bbox: Optional[List] = None,
    supports_bbox_crs: bool = False
) -> int:
    """Fetch all features from a collection."""
    items_url = f"{base_url.rstrip('/')}/collections/{collection}/items"

    # Build parameters
    params = {"limit": str(page_size)}

    if bbox and len(bbox) >= 4:
        params["bbox"] = ",".join(str(v) for v in bbox)
        if supports_bbox_crs:
            params["bbox-crs"] = "EPSG:3006"

    part_count = 0
    total_features = 0
    current_url = items_url
    current_params = params

    delay = 0.1  # Rate limiting

    while current_url:
        try:
            # Make request
            if current_params:
                response = requests.get(current_url, params=current_params, timeout=60)
            else:
                response = requests.get(current_url, timeout=60)

            response.raise_for_status()

            data = response.json()
            features = data.get("features", [])

            if not features:
                break

            # Save page
            part_count += 1
            total_features += len(features)

            out_file = out_dir / f"{collection}_part_{part_count:04d}.geojson"
            with open(out_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, separators=(',', ':'))

            # Find next page
            next_url = None
            links = data.get("links", [])
            for link in links:
                if isinstance(link, dict) and link.get("rel") == "next":
                    next_url = link.get("href")
                    break

            current_url = next_url
            current_params = None  # Next URL has params built in

            # Rate limit
            time.sleep(delay)

        except Exception as e:
            log.error(f"[OGC] Request failed for {collection}: {e}")
            break

    return total_features