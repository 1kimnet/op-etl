"""
WFS downloader for OP-ETL pipeline.
Enhanced implementation with recursion depth protection.
"""

import logging
import json
from pathlib import Path
from typing import Dict, Optional, List, Tuple

from .http_utils import RecursionSafeSession, safe_json_parse, safe_xml_parse, validate_response_content
from .monitoring import start_monitoring_source, end_monitoring_source

log = logging.getLogger(__name__)


def _extract_global_bbox(cfg: dict) -> Tuple[Optional[List[float]], Optional[int]]:
    try:
        if not cfg.get("use_bbox_filter", False):
            return None, None
        gb = cfg.get("global_bbox") or cfg.get("global_ogc_bbox") or {}
        coords = gb.get("coords")
        crs = gb.get("crs")
        sr = None
        if isinstance(crs, int):
            sr = crs
        elif isinstance(crs, str):
            up = crs.upper()
            if up in ("WGS84", "CRS84"):
                sr = 4326
            elif up.startswith("EPSG:"):
                try:
                    sr = int(up.split(":", 1)[1])
                except Exception:
                    sr = None
            elif "/EPSG/" in up:
                try:
                    sr = int(up.rstrip("/").split("/")[-1])
                except Exception:
                    sr = None
        return coords, sr
    except Exception:
        return None, None


def run(cfg: dict) -> None:
    """Process all WFS sources."""
    global_bbox, global_sr = _extract_global_bbox(cfg)
    # Extract sources cleanly
    wfs_sources = []
    for source in cfg.get("sources", []):
        if source.get("type") == "wfs" and source.get("enabled", True):
            # Create clean copy
            wfs_sources.append({
                "name": source.get("name"),
                "url": source.get("url"),
                "authority": source.get("authority", "unknown"),
                "raw": source.get("raw", {}).copy() if source.get("raw") else {}
            })

    if not wfs_sources:
        log.info("[WFS] No WFS sources to process")
        return

    downloads_dir = Path(cfg["workspaces"]["downloads"])

    for source in wfs_sources:
        metric = start_monitoring_source(source['name'], source['authority'], 'wfs')
        
        try:
            log.info(f"[WFS] Processing {source['name']}")
            success = process_wfs_source(source, downloads_dir, global_bbox, global_sr)
            end_monitoring_source(success, files=1 if success else 0)
        except RecursionError as e:
            log.error(f"[WFS] Recursion error in {source['name']}: {e}")
            end_monitoring_source(False, 'RecursionError', str(e))
        except Exception as e:
            log.error(f"[WFS] Failed {source['name']}: {e}")
            end_monitoring_source(False, type(e).__name__, str(e))


def process_wfs_source(source: Dict, downloads_dir: Path,
                      global_bbox: Optional[List[float]], global_sr: Optional[int]) -> bool:
    """Process a single WFS source."""
    url = source["url"]
    authority = source["authority"]
    name = source["name"]

    # Create output directory
    out_dir = downloads_dir / authority
    out_dir.mkdir(parents=True, exist_ok=True)

    try:
        # Check if URL is already a GetFeature request
        if "GetFeature" in url and "typeName" in url:
            # Direct GetFeature URL
            return download_direct_wfs(url, out_dir, name)
        else:
            # WFS service URL
            return download_wfs_service(url, source, out_dir, name, global_bbox, global_sr)

    except Exception as e:
        log.error(f"[WFS] Error processing {name}: {e}")
        return False


def download_direct_wfs(url: str, out_dir: Path, name: str) -> bool:
    """Download from direct GetFeature URL with enhanced error handling."""
    session = RecursionSafeSession()
    
    try:
        # Ensure GeoJSON output
        if "outputFormat=" not in url:
            separator = "&" if "?" in url else "?"
            url = f"{url}{separator}outputFormat=application/json"

        log.info(f"[WFS] Downloading direct WFS: {url}")
        
        response = session.safe_get(url, timeout=120)
        if not response:
            log.error(f"[WFS] Failed to fetch {url}")
            return False
        
        if not validate_response_content(response):
            log.error(f"[WFS] Invalid response content from {url}")
            return False

        # Save response
        try:
            data = safe_json_parse(response.content)
            if data:
                out_file = out_dir / f"{name}.geojson"
                with open(out_file, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False)
                log.info(f"[WFS] Saved {name} as GeoJSON")
                return True
            else:
                # Try XML parsing if JSON failed
                root = safe_xml_parse(response.content)
                if root:
                    out_file = out_dir / f"{name}.gml"
                    out_file.write_text(response.text, encoding='utf-8')
                    log.info(f"[WFS] Saved {name} as GML")
                    return True
        except Exception as e:
            log.error(f"[WFS] Failed to save response: {e}")
            return False

        return False

    except RecursionError as e:
        log.error(f"[WFS] Recursion error downloading: {e}")
        return False
    except Exception as e:
        log.error(f"[WFS] Download failed: {e}")
        return False


def download_wfs_service(url: str, source: Dict, out_dir: Path, name: str,
                         global_bbox: Optional[List[float]], global_sr: Optional[int]) -> bool:
    """Download from WFS service URL with enhanced error handling."""
    session = RecursionSafeSession()
    raw = source.get("raw", {})

    # Extract typename from raw config
    typename = raw.get("typename") or raw.get("typeName")
    if not typename:
        # Try to extract from URL if present
        if "typeName=" in url:
            typename = url.split("typeName=")[1].split("&")[0]
        else:
            log.warning(f"[WFS] No typename specified for {name}")
            return False

    try:
        log.info(f"[WFS] Downloading WFS service: {typename}")
        
        # Build GetFeature request
        params = {
            "service": "WFS",
            "version": "2.0.0",
            "request": "GetFeature",
            "typeName": typename,
            "outputFormat": "application/json"
        }

        # Add bbox if configured
        bbox = raw.get("bbox") or global_bbox
        if bbox and len(bbox) >= 4:
            params["bbox"] = ",".join(str(v) for v in bbox[:4])
            bbox_sr = raw.get("bbox_sr") or global_sr or 4326
            params["srsName"] = f"EPSG:{bbox_sr}"

        response = session.safe_get(url, params=params, timeout=120)
        if not response:
            log.error(f"[WFS] Failed to fetch WFS service: {url}")
            return False
        
        if not validate_response_content(response):
            log.error(f"[WFS] Invalid response content from WFS service: {url}")
            return False

        # Save response
        try:
            data = safe_json_parse(response.content)
            if data:
                out_file = out_dir / f"{name}.geojson"
                with open(out_file, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False)
                log.info(f"[WFS] Saved {typename} as GeoJSON")
                return True
            else:
                # Try XML parsing if JSON failed  
                root = safe_xml_parse(response.content)
                if root:
                    out_file = out_dir / f"{name}.gml"
                    out_file.write_text(response.text, encoding='utf-8')
                    log.info(f"[WFS] Saved {typename} as GML")
                    return True
        except Exception as e:
            log.error(f"[WFS] Failed to save response: {e}")
            return False

        return False

    except RecursionError as e:
        log.error(f"[WFS] Recursion error requesting service: {e}")
        return False
    except Exception as e:
        log.error(f"[WFS] Service request failed: {e}")
        return False