"""
OGC API Features downloader for OP-ETL pipeline.
Enhanced implementation with recursion depth protection.
"""

import logging
import json
from pathlib import Path
import time
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin

from .http_utils import RecursionSafeSession, safe_json_parse, validate_response_content
from .monitoring import start_monitoring_source, end_monitoring_source

log = logging.getLogger(__name__)


def _extract_global_bbox(cfg: dict) -> Tuple[Optional[List[float]], Optional[str]]:
    """Read a global bbox for OGC from config.
    Supports cfg['global_ogc_bbox'] or cfg['global_bbox'] when cfg['use_bbox_filter'] is true.
    Returns (coords, crs_str) where crs_str is either an EPSG URL or CRS84.
    """
    try:
        if not cfg.get("use_bbox_filter", False):
            return None, None
        gb = cfg.get("global_ogc_bbox") or cfg.get("global_bbox") or {}
        coords = gb.get("coords")
        crs = gb.get("crs")
        if crs is None:
            crs_str = None
        elif isinstance(crs, int):
            crs_str = f"http://www.opengis.net/def/crs/EPSG/0/{crs}"
        else:
            up = str(crs).upper()
            if up in ("WGS84", "CRS84"):
                crs_str = "CRS84"
            elif up.startswith("EPSG:"):
                try:
                    epsg = int(up.split(":", 1)[1])
                    crs_str = f"http://www.opengis.net/def/crs/EPSG/0/{epsg}"
                except Exception:
                    crs_str = None
            elif "/EPSG/" in up:
                crs_str = crs
            else:
                crs_str = crs
        return coords, crs_str
    except Exception:
        return None, None


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
        metric = start_monitoring_source(source['name'], source['authority'], 'ogc')
        
        try:
            log.info(f"[OGC] Processing {source['name']}")
            success = process_ogc_source(source, downloads_dir, global_bbox, global_crs, delay_seconds)
            end_monitoring_source(success, features=0)  # Features counted in process_ogc_source
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
                       delay_seconds: float) -> bool:
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
        return False

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
    return total_features > 0


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
    params = {
        "limit": page_size,
        # Prefer explicit OGC Features JSON representation
        "f": "application/vnd.ogc.fg+json",
    }

    headers = {
        # Ask for GeoJSON/Features JSON; many services honor Accept first
        "Accept": "application/geo+json, application/vnd.ogc.fg+json, application/json;q=0.9",
    }

    bbox = raw.get("bbox") or global_bbox
    if bbox and len(bbox) >= 4:
        params["bbox"] = ",".join(str(v) for v in bbox[:4])
        sr = raw.get("bbox_sr") or raw.get("bbox-crs") or global_crs
        supports_bbox_crs = bool(raw.get("supports_bbox_crs", True))
        if sr and supports_bbox_crs:
            sr_str = str(sr)
            sr_upper = sr_str.upper()
            is_epsg_int = isinstance(sr, int) or (isinstance(sr, str) and sr_str.isdigit())

            # Only send bbox-crs when not using default CRS84
            is_crs84_token = sr_upper == "CRS84"
            is_crs84_uri = sr_upper in (
                "HTTP://WWW.OPENGIS.NET/DEF/CRS/OGC/1.3/CRS84",
                "HTTPS://WWW.OPENGIS.NET/DEF/CRS/OGC/1.3/CRS84",
            )
            if not (is_crs84_token or is_crs84_uri):
                if is_epsg_int:
                    params["bbox-crs"] = f"http://www.opengis.net/def/crs/EPSG/0/{int(sr)}"
                else:
                    params["bbox-crs"] = sr_str

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

            features = data.get("features", [])
            if features:
                all_features.extend(features)
                total += len(features)
                log.debug(f"[OGC] Page {page}: {len(features)} features")
                page += 1

            # Find next link
            next_link = _find_next_link(data.get("links", []))
            next_url = next_link
            next_params = None

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
        out_file = out_dir / f"{collection_id}.geojson"
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump({"type": "FeatureCollection", "features": all_features}, f, ensure_ascii=False, separators=(",", ":"))
        
        log.info(f"[OGC] Saved {total} features to {out_file.name}")
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