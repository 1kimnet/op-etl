"""
WFS downloader for OP-ETL pipeline.
Fixed to avoid recursion issues.
"""

import logging
import json
from pathlib import Path
from typing import Dict, Optional, List, Tuple
import requests

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
        try:
            log.info(f"[WFS] Processing {source['name']}")
            process_wfs_source(source, downloads_dir, global_bbox, global_sr)
        except Exception as e:
            log.error(f"[WFS] Failed {source['name']}: {e}")


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
    """Download from direct GetFeature URL."""
    try:
        # Ensure GeoJSON output
        if "outputFormat=" not in url:
            separator = "&" if "?" in url else "?"
            url = f"{url}{separator}outputFormat=application/json"

        response = requests.get(url, timeout=120)
        response.raise_for_status()

        # Save response
        try:
            data = response.json()
            out_file = out_dir / f"{name}.geojson"
            with open(out_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False)
            log.info(f"[WFS] Saved {name} as GeoJSON")
        except json.JSONDecodeError:
            # Save as GML
            out_file = out_dir / f"{name}.gml"
            out_file.write_text(response.text, encoding='utf-8')
            log.info(f"[WFS] Saved {name} as GML")

        return True

    except Exception as e:
        log.error(f"[WFS] Download failed: {e}")
        return False


def download_wfs_service(url: str, source: Dict, out_dir: Path, name: str,
                         global_bbox: Optional[List[float]], global_sr: Optional[int]) -> bool:
    """Download from WFS service URL."""
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

        response = requests.get(url, params=params, timeout=120)
        response.raise_for_status()

        # Save response
        try:
            data = response.json()
            out_file = out_dir / f"{name}.geojson"
            with open(out_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False)
            log.info(f"[WFS] Saved {typename} as GeoJSON")
        except json.JSONDecodeError:
            out_file = out_dir / f"{name}.gml"
            out_file.write_text(response.text, encoding='utf-8')
            log.info(f"[WFS] Saved {typename} as GML")

        return True

    except Exception as e:
        log.error(f"[WFS] Service request failed: {e}")
        return False