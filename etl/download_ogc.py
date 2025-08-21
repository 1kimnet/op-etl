"""
OGC API Features downloader for OP-ETL pipeline.
Supports bbox filtering and include-based collection selection.
"""

import logging
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin

import requests

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
        try:
            log.info(f"[OGC] Processing {source['name']}")
            process_ogc_source(source, downloads_dir, global_bbox, global_crs)
        except Exception as e:
            log.error(f"[OGC] Failed {source['name']}: {e}")


def normalize_base_url(url: str) -> str:
    """Ensure base URL does not end with /collections."""
    u = url.rstrip("/")
    if u.lower().endswith("/collections"):
        u = u[: -len("/collections")]
    return u


def process_ogc_source(source: Dict, downloads_dir: Path,
                       global_bbox: Optional[List[float]], global_crs: Optional[str]) -> bool:
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
            cnt = fetch_collection_items(base_url, cid, out_dir / cid, raw, global_bbox, global_crs)
            total_features += cnt
            log.info(f"[OGC] Collection {cid}: {cnt} features")
        except Exception as e:
            log.warning(f"[OGC] Failed collection {cid}: {e}")

    log.info(f"[OGC] Total features from {name}: {total_features}")
    return total_features > 0


def discover_collections(base_url: str) -> List[Dict]:
    try:
        url = urljoin(base_url + "/", "collections")
        params = {"f": "json"}
        r = requests.get(url, params=params, timeout=60)
        r.raise_for_status()
        data = r.json()
        cols = data.get("collections", [])
        results: List[Dict] = []
        for c in cols:
            if isinstance(c, dict) and c.get("id"):
                results.append({
                    "id": c.get("id"),
                    "title": c.get("title") or c.get("id"),
                })
        return results
    except Exception as e:
        log.error(f"[OGC] Discover collections failed: {e}")
        return []


def fetch_collection_items(base_url: str, collection_id: str, out_dir: Path, raw: Dict,
                           global_bbox: Optional[List[float]], global_crs: Optional[str]) -> int:
    out_dir.mkdir(parents=True, exist_ok=True)

    url = urljoin(base_url.rstrip("/") + "/", f"collections/{collection_id}/items")
    params = {
        "limit": 1000,
        "f": "json",
    }

    bbox = raw.get("bbox") or global_bbox
    if bbox and len(bbox) >= 4:
        params["bbox"] = ",".join(str(v) for v in bbox[:4])
        sr = raw.get("bbox_sr") or raw.get("bbox-crs") or global_crs
        if sr:
            if isinstance(sr, int) or (isinstance(sr, str) and str(sr).isdigit()):
                params["bbox-crs"] = f"http://www.opengis.net/def/crs/EPSG/0/{int(sr)}"
            else:
                params["bbox-crs"] = str(sr)

    total = 0
    page = 1
    next_url: Optional[str] = url
    next_params = params.copy()

    while next_url:
        r = requests.get(next_url, params=next_params if next_url == url else None, timeout=120)
        r.raise_for_status()
        data = r.json()

        features = data.get("features", [])
        if features:
            out_file = out_dir / f"part_{page:03d}.geojson"
            with open(out_file, "w", encoding="utf-8") as f:
                json.dump({"type": "FeatureCollection", "features": features}, f, ensure_ascii=False, separators=(",", ":"))
            total += len(features)
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

    return total


def _find_next_link(links: List[Dict]) -> Optional[str]:
    try:
        for link in links or []:
            if link.get("rel") == "next" and link.get("href"):
                return link["href"]
    except Exception:
        pass
    return None