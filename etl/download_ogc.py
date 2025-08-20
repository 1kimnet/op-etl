"""
OGC API Features handler for OP-ETL pipeline.
Supports pagination, bbox filtering, and collection discovery.
"""

import logging
import json
import time
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urljoin
import requests

from .download_http import ensure_dir


def run(cfg: dict) -> None:
    """Process all OGC API sources in the configuration."""
    ogc_sources = [s for s in cfg.get("sources", [])
                   if s.get("type") == "ogc_api" and s.get("enabled", True)]

    if not ogc_sources:
        logging.info("[OGC] No OGC API sources found")
        return

    downloads_dir = Path(cfg["workspaces"]["downloads"])
    processed_count = 0

    for source in ogc_sources:
        try:
            success = process_ogc_source(source, downloads_dir, cfg)
            if success:
                processed_count += 1
                logging.info(f"[OGC] ✓ {source['name']}")
            else:
                logging.warning(f"[OGC] ✗ {source['name']} failed")
        except Exception as e:
            logging.error(f"[OGC] Error processing {source['name']}: {e}")

    logging.info(f"[OGC] Processed {processed_count} OGC API sources")


def process_ogc_source(source: dict, downloads_dir: Path, cfg: dict) -> bool:
    """Process a single OGC API source."""
    base_url = source["url"]
    authority = source.get("authority", "unknown")
    name = source.get("name", "unnamed")

    # Check for recursion issues flag
    if source.get("extra", {}).get("recursion_issues"):
        logging.warning(f"[OGC] Skipping {name} (recursion issues - needs manual fix)")
        return False

    raw_config = source.get("extra", {})

    # Extract configuration
    collections = raw_config.get("collections", [])
    page_size = raw_config.get("page_size", 1000)
    supports_bbox_crs = raw_config.get("supports_bbox_crs", True)

    # Get bbox from source or global config
    bbox = raw_config.get("bbox")
    bbox_sr = raw_config.get("bbox_sr", 3006)

    if not bbox:
        # Try global bbox from config
        global_bbox = cfg.get("global_ogc_bbox", {})
        if global_bbox.get("coords"):
            bbox = global_bbox["coords"]
            bbox_sr = global_bbox.get("crs", "CRS84")
            if bbox_sr == "CRS84":
                bbox_sr = 4326  # WGS84

    try:
        # Create output directory
        auth_dir = ensure_dir(downloads_dir / authority)
        out_dir = ensure_dir(auth_dir / name)

        # Auto-discover collections if none specified
        if not collections:
            collections = discover_collections(base_url)
            if not collections:
                logging.warning(f"[OGC] No collections found for {base_url}")
                return False

        total_features = 0

        # Process each collection
        for collection in collections:
            features_count = fetch_collection_to_files(
                base_url, collection, out_dir,
                page_size=page_size,
                bbox=bbox,
                bbox_sr=bbox_sr,
                supports_bbox_crs=supports_bbox_crs
            )
            total_features += features_count
            logging.debug(f"[OGC] Collection {collection}: {features_count} features")

        if total_features > 0:
            logging.info(f"[OGC] Downloaded {total_features} features from {name}")
            return True

        return False

    except Exception as e:
        logging.error(f"[OGC] Failed to process {base_url}: {e}")
        return False


def discover_collections(base_url: str) -> List[str]:
    """Auto-discover available collections from OGC API."""
    try:
        collections_url = urljoin(base_url.rstrip("/") + "/", "collections")

        response = requests.get(collections_url, timeout=30)
        response.raise_for_status()

        data = response.json()
        collections = data.get("collections", [])

        collection_ids = []
        for collection in collections:
            if isinstance(collection, dict) and "id" in collection:
                collection_ids.append(collection["id"])

        logging.debug(f"[OGC] Discovered {len(collection_ids)} collections")
        return collection_ids

    except Exception as e:
        logging.warning(f"[OGC] Failed to discover collections from {base_url}: {e}")
        return []


def fetch_collection_to_files(
    base_url: str,
    collection: str,
    out_dir: Path,
    page_size: int = 1000,
    bbox: Optional[List[float]] = None,
    bbox_sr: int = 3006,
    supports_bbox_crs: bool = True
) -> int:
    """Fetch all features from a collection and save as GeoJSON files."""

    # Build items URL
    base = base_url.rstrip("/") + "/"
    items_url = urljoin(base, f"collections/{collection}/items")

    # Build initial parameters
    params: Dict[str, str] = {"limit": str(page_size)}

    if bbox:
        params["bbox"] = ",".join(str(v) for v in bbox)
        if supports_bbox_crs and bbox_sr != 4326:
            params["bbox-crs"] = f"EPSG:{bbox_sr}"

    part_count = 0
    total_features = 0
    current_url = items_url
    current_params = params

    # Add delay from config if available
    delay = 0.1  # Default delay

    while current_url:
        try:
            response = requests.get(current_url, params=current_params, timeout=60)
            response.raise_for_status()

            data = response.json()
            features = data.get("features", [])

            if not features:
                break

            # Save this page
            part_count += 1
            total_features += len(features)

            out_file = out_dir / f"{collection}_part_{part_count:04d}.geojson"
            with open(out_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, separators=(',', ':'))

            # Find next page
            current_url = get_next_page_url(data.get("links", []))
            current_params = None  # Parameters are in the next URL

            # Rate limiting
            time.sleep(delay)

        except requests.exceptions.RequestException as e:
            logging.error(f"[OGC] Request failed for {collection}: {e}")
            break
        except Exception as e:
            logging.error(f"[OGC] Unexpected error for {collection}: {e}")
            break

    return total_features


def get_next_page_url(links: List[Dict]) -> Optional[str]:
    """Extract next page URL from OGC API links."""
    if not links:
        return None

    for link in links:
        if isinstance(link, dict) and link.get("rel") == "next":
            return link.get("href")

    return None


def validate_ogc_endpoint(base_url: str) -> bool:
    """Check if URL is a valid OGC API Features endpoint."""
    try:
        # Try to access the landing page
        response = requests.get(base_url, timeout=10)
        response.raise_for_status()

        data = response.json()

        # Look for OGC API indicators
        links = data.get("links", [])
        for link in links:
            if isinstance(link, dict):
                rel = link.get("rel", "")
                if rel in ["service-desc", "service-doc", "collections"]:
                    return True

        # Check for collections endpoint
        collections_url = urljoin(base_url.rstrip("/") + "/", "collections")
        collections_response = requests.get(collections_url, timeout=10)

        return collections_response.status_code == 200

    except Exception:
        return False


def get_collection_info(base_url: str, collection_id: str) -> Optional[Dict]:
    """Get metadata about a specific collection."""
    try:
        collection_url = urljoin(
            base_url.rstrip("/") + "/",
            f"collections/{collection_id}"
        )

        response = requests.get(collection_url, timeout=15)
        response.raise_for_status()

        return response.json()

    except Exception as e:
        logging.debug(f"[OGC] Failed to get collection info for {collection_id}: {e}")
        return None


def estimate_feature_count(base_url: str, collection_id: str) -> Optional[int]:
    """Try to estimate total feature count for a collection."""
    try:
        # Try a small request to see if numberMatched is provided
        items_url = urljoin(
            base_url.rstrip("/") + "/",
            f"collections/{collection_id}/items"
        )

        response = requests.get(items_url, params={"limit": 1}, timeout=15)
        response.raise_for_status()

        data = response.json()
        return data.get("numberMatched")

    except Exception:
        return None