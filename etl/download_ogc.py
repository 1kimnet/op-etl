"""
OGC API Features downloader for OP-ETL pipeline.
Enhanced implementation with recursion depth protection and SR consistency.
"""

import json
import logging
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin

from .http_utils import RecursionSafeSession, safe_json_parse, validate_response_content
from .monitoring import end_monitoring_source, start_monitoring_source
from .sr_utils import (
    SWEREF99_TM,
    WGS84_DD,
    log_sr_validation_summary,
    validate_sr_consistency,
)

log = logging.getLogger(__name__)


def _extract_global_bbox(cfg: dict) -> Tuple[Optional[List[float]], Optional[str]]:
    """Read a global bbox for OGC from config.
    Supports cfg['global_ogc_bbox'] or cfg['global_bbox'] when cfg['use_bbox_filter'] is true.
    Returns (coords, crs_str) where crs_str is CRS84 for compatibility.
    """
    try:
        if not cfg.get("use_bbox_filter", False):
            return None, None
        gb = cfg.get("global_ogc_bbox") or cfg.get("global_bbox") or {}
        coords = gb.get("coords")
        crs = gb.get("crs")

        # Default to CRS84 for OGC API Features compatibility
        if crs is None:
            crs_str = "http://www.opengis.net/def/crs/OGC/1.3/CRS84"
        elif isinstance(crs, int):
            if crs == 4326:
                crs_str = "http://www.opengis.net/def/crs/OGC/1.3/CRS84"
            else:
                crs_str = f"http://www.opengis.net/def/crs/EPSG/0/{crs}"
        else:
            up = str(crs).upper()
            if up in ("WGS84", "CRS84", "4326"):
                crs_str = "http://www.opengis.net/def/crs/OGC/1.3/CRS84"
            elif up.startswith("EPSG:"):
                try:
                    epsg = int(up.split(":", 1)[1])
                    if epsg == 4326:
                        crs_str = "http://www.opengis.net/def/crs/OGC/1.3/CRS84"
                    else:
                        crs_str = f"http://www.opengis.net/def/crs/EPSG/0/{epsg}"
                except Exception:
                    crs_str = "http://www.opengis.net/def/crs/OGC/1.3/CRS84"
            elif "/EPSG/" in up:
                crs_str = crs
            else:
                crs_str = "http://www.opengis.net/def/crs/OGC/1.3/CRS84"
        return coords, crs_str
    except Exception:
        return None, "http://www.opengis.net/def/crs/OGC/1.3/CRS84"


def run(cfg: dict) -> None:
    """Process all OGC sources in configuration."""
    global_bbox, global_crs = _extract_global_bbox(cfg)
    delay_seconds = float(cfg.get("ogc_api_delay", 0.1) or 0)
    ogc_sources = []
    for source in cfg.get("sources", []):
        if source.get("type") == "ogc" and source.get("enabled", True):
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

    for source in ogc_sources:
        start_monitoring_source(source['name'], source['authority'], 'ogc')

        try:
            log.info(f"[OGC] Processing {source['name']}")
            success, feature_count = process_ogc_source(source, downloads_dir, global_bbox, global_crs, delay_seconds)
            end_monitoring_source(success, features=feature_count)  # Features counted in process_ogc_source
        except RecursionError as e:
            log.error(f"[OGC] Recursion error in {source['name']}: {e}")
            end_monitoring_source(False, 'RecursionError', str(e))
        except Exception as e:
            log.error(f"[OGC] Failed {source['name']}: {e}")
            end_monitoring_source(False, type(e).__name__, str(e))


def normalize_base_url(url: str) -> str:
    """Ensure base URL does not end with /collections."""
    u = url.rstrip("/")
    if u.lower().endswith("/collections"):
        u = u[: -len("/collections")]
    return u


def process_ogc_source(source: Dict, downloads_dir: Path,
                       global_bbox: Optional[List[float]], global_crs: Optional[str],
                       delay_seconds: float) -> Tuple[bool, int]:
    base_url = normalize_base_url(source["url"])
    authority = source["authority"]
    name = source["name"]
    raw = source.get("raw", {})

    out_dir = downloads_dir / authority / name
    out_dir.mkdir(parents=True, exist_ok=True)

    # Discover collections
    collections = discover_collections(base_url)
    if not collections:
        log.warning(f"[OGC] No collections discovered for {name}")
        return False, 0

    # Filter by explicit list or include patterns
    selected_ids: List[str] = []
    explicit = raw.get("collections")
    includes = raw.get("include") or raw.get("includes")
    if explicit:
        selected_ids = [c["id"] for c in collections if c.get("id") in explicit]
    elif includes:
        import fnmatch
        pats = [p.lower() for p in includes]
        for c in collections:
            cid = str(c.get("id", "")).lower()
            title = str(c.get("title", "")).lower()
            if any(fnmatch.fnmatchcase(cid, p) or fnmatch.fnmatchcase(title, p) for p in pats):
                selected_ids.append(c["id"])
    else:
        selected_ids = [c["id"] for c in collections]

    total_features = 0
    for cid in selected_ids:
        try:
            cnt = fetch_collection_items(
                base_url,
                cid,
                out_dir,
                raw,
                global_bbox,
                global_crs,
                delay_seconds,
            )
            total_features += cnt
            log.info(f"[OGC] Collection {cid}: {cnt} features")
        except Exception as e:
            log.warning(f"[OGC] Failed collection {cid}: {e}")

    log.info(f"[OGC] Total features from {name}: {total_features}")

    if total_features == 0:
        log.info(f"[OGC] No features found in bbox for {name} - this is acceptable (not an error)")

    # Success if we successfully processed collections (even if 0 features)
    # Only fail if no collections were discovered or other errors occurred
    return True, total_features


def discover_collections(base_url: str) -> List[Dict]:
    """Discover collections from OGC API with enhanced error handling."""
    session = RecursionSafeSession()

    try:
        url = urljoin(base_url + "/", "collections")
        params = {"f": "json"}
        headers = {"Accept": "application/json"}

        log.info(f"[OGC] Discovering collections: {url}")

        response = session.safe_get(url, params=params, headers=headers, timeout=60)
        if not response:
            log.error(f"[OGC] Failed to get collections from {url}")
            return []

        if not validate_response_content(response):
            log.error(f"[OGC] Invalid collections response from {url}")
            return []

        data = safe_json_parse(response.content)
        if not data:
            log.error(f"[OGC] Failed to parse collections from {url}")
            return []

        cols = data.get("collections", [])
        results: List[Dict] = []
        for c in cols:
            if isinstance(c, dict) and c.get("id"):
                results.append({
                    "id": c.get("id"),
                    "title": c.get("title") or c.get("id"),
                })

        log.info(f"[OGC] Discovered {len(results)} collections")
        return results

    except RecursionError as e:
        log.error(f"[OGC] Recursion error discovering collections: {e}")
        return []
    except Exception as e:
        log.error(f"[OGC] Discover collections failed: {e}")
        return []


def fetch_collection_items(base_url: str, collection_id: str, out_dir: Path, raw: Dict,
                           global_bbox: Optional[List[float]], global_crs: Optional[str],
                           delay_seconds: float) -> int:
    """Fetch collection items with enhanced error handling."""
    out_dir.mkdir(parents=True, exist_ok=True)
    session = RecursionSafeSession()

    url = urljoin(base_url.rstrip("/") + "/", f"collections/{collection_id}/items")
    page_size = int(raw.get("page_size", 1000) or 1000)
    params = {}

    # Get SR configuration for source (OGC-specific handling) - handled inline per request

    # Check if server supports EPSG:3006 - define early to ensure it's always bound
    supports_3006 = raw.get("supports_epsg_3006", False)

    # Only add limit parameter if explicitly configured and not the default
    if raw.get("page_size") and raw.get("page_size") != 1000:
        params["limit"] = page_size

    headers = {
        # Ask for GeoJSON; many services honor Accept first
        "Accept": "application/geo+json, application/json;q=0.9",
    }

    bbox = raw.get("bbox") or global_bbox

    if bbox and len(bbox) >= 4:
        params["bbox"] = ",".join(str(v) for v in bbox[:4])
        if supports_3006:
            # Explicitly request SWEREF99 TM when server supports it
            params["bbox-crs"] = f"http://www.opengis.net/def/crs/EPSG/0/{SWEREF99_TM}"
            params["crs"] = f"http://www.opengis.net/def/crs/EPSG/0/{SWEREF99_TM}"
            log.info(f"[OGC] Using EPSG:{SWEREF99_TM} for {collection_id}")
        else:
            # Do NOT send bbox-crs for CRS84; most servers assume CRS84 by default
            log.info(f"[OGC] Using default CRS84 for {collection_id}")

    validation_results = {}
    total = 0
    page = 1
    all_features: List[Dict] = []
    next_url: Optional[str] = url
    next_params = params.copy()

    try:
        log.info(f"[OGC] Fetching collection items: {collection_id}")

        while next_url:
            response = session.safe_get(
                next_url,
                params=next_params if next_url == url else None,
                timeout=120,
                headers=headers,
            )

            if not response:
                log.error(f"[OGC] Failed to fetch page {page} for {collection_id}")
                break

            if not validate_response_content(response):
                log.error(f"[OGC] Invalid response content for page {page} of {collection_id}")
                break

            data = safe_json_parse(response.content)
            if not data:
                log.error(f"[OGC] Failed to parse response for page {page} of {collection_id}")
                break

            # Validate SR consistency on first page
            if page == 1:
                expected_sr = SWEREF99_TM if supports_3006 else WGS84_DD
                sr_valid, detected_sr = validate_sr_consistency(data, expected_sr)
                validation_results["sr_consistency"] = sr_valid
                if not sr_valid:
                    log.warning(f"[OGC] SR validation failed for {collection_id} - expected {expected_sr}, detected {detected_sr}")

            features = data.get("features", [])
            if features:
                all_features.extend(features)
                total += len(features)
                log.debug(f"[OGC] Page {page}: {len(features)} features")
                page += 1

            # Find next link and preserve CRS parameters
            next_link = _find_next_link(data.get("links", []))
            next_url = next_link
            next_params = None

            # Ensure CRS params are maintained across pagination (only when explicitly set)
            if next_url and supports_3006:
                if "?" in next_url:
                    next_url += f"&crs=http://www.opengis.net/def/crs/EPSG/0/{SWEREF99_TM}"
                else:
                    next_url += f"?crs=http://www.opengis.net/def/crs/EPSG/0/{SWEREF99_TM}"

            if not next_url:
                break

            # Safety guard
            if page > 1000:
                log.warning("[OGC] Pagination exceeded 1000 pages, stopping.")
                break

            # Respect configured inter-request delay to be gentle on APIs
            if delay_seconds and delay_seconds > 0:
                time.sleep(delay_seconds)

        # Write a single merged file per collection
        if all_features:
            out_file = out_dir / f"{collection_id}.geojson"
            with open(out_file, "w", encoding="utf-8") as f:
                json.dump({"type": "FeatureCollection", "features": all_features}, f, ensure_ascii=False, separators=(",", ":"))

            log.info(f"[OGC] Saved {total} features to {out_file.name}")

            # Log validation summary
            if validation_results:
                log_sr_validation_summary(collection_id, validation_results)

        return total

    except RecursionError as e:
        log.error(f"[OGC] Recursion error fetching {collection_id}: {e}")
        return 0
    except Exception as e:
        log.error(f"[OGC] Error fetching {collection_id}: {e}")
        return 0


def _find_next_link(links: List[Dict]) -> Optional[str]:
    try:
        for link in links or []:
            if link.get("rel") == "next" and link.get("href"):
                return link["href"]
    except Exception:
        pass
    return None
