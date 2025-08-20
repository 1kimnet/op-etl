"""
WFS (Web Feature Service) handler for OP-ETL pipeline.
Handles sources like SJV's INSPIRE WFS services.
"""

import logging
import json
from pathlib import Path
import requests

from .download_http import ensure_dir


def run(cfg: dict) -> None:
    """Process all WFS sources in configuration."""
    wfs_sources = [s for s in cfg.get("sources", [])
                   if s.get("type") == "wfs" and s.get("enabled", True)]

    if not wfs_sources:
        logging.info("[WFS] No WFS sources found")
        return

    downloads_dir = Path(cfg["workspaces"]["downloads"])
    processed_count = 0

    for source in wfs_sources:
        try:
            success = process_wfs_source(source, downloads_dir)
            if success:
                processed_count += 1
                logging.info(f"[WFS] ✓ {source['name']}")
            else:
                logging.warning(f"[WFS] ✗ {source['name']} failed")
        except Exception as e:
            logging.error(f"[WFS] Error processing {source['name']}: {e}")

    logging.info(f"[WFS] Processed {processed_count} WFS sources")


def process_wfs_source(source: dict, downloads_dir: Path) -> bool:
    """Process a single WFS source."""
    url = source["url"]
    authority = source.get("authority", "unknown")
    name = source.get("name", "unnamed")

    # Check for recursion issues flag
    if source.get("extra", {}).get("recursion_issues"):
        logging.warning(f"[WFS] Skipping {name} (recursion issues - needs manual fix)")
        return False

    # Check if URL already contains GetFeature request
    if "GetFeature" in url and "typeName" in url:
        return download_wfs_direct(url, downloads_dir, authority, name)
    else:
        return download_wfs_service(url, source, downloads_dir, authority, name)


def download_wfs_direct(url: str, downloads_dir: Path, authority: str, name: str) -> bool:
    """Download from direct WFS GetFeature URL."""
    try:
        auth_dir = ensure_dir(downloads_dir / authority)

        # Force GeoJSON output if not specified
        if "outputFormat=" not in url:
            separator = "&" if "?" in url else "?"
            url = f"{url}{separator}outputFormat=application/json"

        response = requests.get(url, timeout=120)
        response.raise_for_status()

        # Try to save as GeoJSON
        try:
            data = response.json()
            out_file = auth_dir / f"{name}.geojson"
            with open(out_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False)
        except json.JSONDecodeError:
            # Fallback: save as GML/XML
            out_file = auth_dir / f"{name}.gml"
            out_file.write_text(response.text, encoding='utf-8')

        logging.info(f"[WFS] Downloaded {name} to {out_file}")
        return True

    except Exception as e:
        logging.error(f"[WFS] Failed to download {url}: {e}")
        return False


def download_wfs_service(url: str, source: dict, downloads_dir: Path, authority: str, name: str) -> bool:
    """Download from WFS service base URL."""
    try:
        raw = source.get("raw", {})
        typename = raw.get("typename") or raw.get("typeName")

        if not typename:
            logging.warning(f"[WFS] No typename specified for {name}")
            return False

        # Build GetFeature parameters
        params = {
            "service": "WFS",
            "version": "2.0.0",
            "request": "GetFeature",
            "typeName": typename,
            "outputFormat": "application/json"
        }

        # Add bbox if specified
        bbox = raw.get("bbox")
        if bbox:
            params["bbox"] = ",".join(str(v) for v in bbox)
            params["srsName"] = f"EPSG:{raw.get('bbox_sr', 4326)}"

        auth_dir = ensure_dir(downloads_dir / authority)

        response = requests.get(url, params=params, timeout=120)
        response.raise_for_status()

        try:
            data = response.json()
            out_file = auth_dir / f"{name}.geojson"
            with open(out_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False)
        except json.JSONDecodeError:
            out_file = auth_dir / f"{name}.gml"
            out_file.write_text(response.text, encoding='utf-8')

        logging.info(f"[WFS] Downloaded {typename} to {out_file}")
        return True

    except Exception as e:
        logging.error(f"[WFS] Failed to download from {url}: {e}")
        return False