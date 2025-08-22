"""
ATOM feed downloader for OP-ETL pipeline.
Enhanced implementation with recursion depth protection and bbox support for referenced services.
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .http_utils import RecursionSafeSession, safe_xml_parse, download_with_retries, validate_response_content
from .monitoring import start_monitoring_source, end_monitoring_source

log = logging.getLogger(__name__)


def _extract_global_bbox(cfg: dict) -> Tuple[Optional[List[float]], Optional[int]]:
    """Extract global bbox configuration for ATOM feeds that reference filterable services."""
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
    """Process all ATOM sources in configuration."""
    global_bbox, global_sr = _extract_global_bbox(cfg)
    
    # Get fresh sources list without circular references
    atom_sources = []
    for source in cfg.get("sources", []):
        if source.get("type") == "atom" and source.get("enabled", True):
            # Create clean copy without nested references
            atom_sources.append({
                "name": source.get("name"),
                "url": source.get("url"),
                "authority": source.get("authority", "unknown"),
                "raw": source.get("raw", {}).copy() if source.get("raw") else {}
            })

    if not atom_sources:
        log.info("[ATOM] No ATOM sources to process")
        return

    downloads_dir = Path(cfg["workspaces"]["downloads"])

    for source in atom_sources:
        metric = start_monitoring_source(source['name'], source['authority'], 'atom')
        
        try:
            log.info(f"Processing ATOM source: {source['name']}")
            success = process_atom_source(source, downloads_dir, global_bbox, global_sr)
            end_monitoring_source(success, files=1 if success else 0)
        except RecursionError as e:
            log.error(f"[ATOM] Recursion error in {source['name']}: {e}")
            end_monitoring_source(False, 'RecursionError', str(e))
        except Exception as e:
            log.error(f"[ATOM] Failed {source['name']}: {e}")
            end_monitoring_source(False, type(e).__name__, str(e))


def process_atom_source(source: Dict, downloads_dir: Path, 
                       global_bbox: Optional[List[float]] = None, 
                       global_sr: Optional[int] = None) -> bool:
    """Process a single ATOM source with enhanced error handling and bbox support."""
    url = source["url"]
    authority = source["authority"]
    raw = source.get("raw", {})

    # Create output directory
    out_dir = downloads_dir / authority
    out_dir.mkdir(parents=True, exist_ok=True)

    session = RecursionSafeSession()

    try:
        log.info(f"[ATOM] Fetching feed: {url}")
        
        # Fetch ATOM feed with safety checks
        response = session.safe_get(url, timeout=30)
        if not response:
            log.error(f"[ATOM] Failed to fetch {url}")
            return False
        
        # Validate response before parsing
        if not validate_response_content(response):
            log.error(f"[ATOM] Invalid response content from {url}")
            return False

        # Parse XML safely
        root = safe_xml_parse(response.content)
        if not root:
            log.error(f"[ATOM] Failed to parse XML from {url}")
            return False

        # Define namespace
        ns = {"atom": "http://www.w3.org/2005/Atom"}

        # Find download links
        download_count = 0
        entries = root.findall(".//atom:entry", ns)
        
        log.info(f"[ATOM] Found {len(entries)} entries in feed")

        for entry in entries:
            try:
                links = entry.findall("atom:link", ns)
                for link in links:
                    href = link.get("href")
                    if not href:
                        continue
                        
                    # Check if this is a direct download link (enclosure)
                    if (link.get("rel") == "enclosure" or 
                        link.get("type") in ["application/zip", "application/x-zip-compressed"]):
                        success = download_file(href, out_dir)
                        if success:
                            download_count += 1
                    
                    # Check if this is a service URL that could benefit from bbox filtering
                    elif raw.get("filter_services", False) and is_filterable_service(href):
                        success = download_filterable_service(
                            href, out_dir, source, global_bbox, global_sr
                        )
                        if success:
                            download_count += 1
                            
            except Exception as e:
                log.warning(f"[ATOM] Error processing entry: {e}")
                continue

        log.info(f"[ATOM] Downloaded {download_count} files from {source['name']}")
        return download_count > 0

    except RecursionError as e:
        log.error(f"[ATOM] Recursion error processing {url}: {e}")
        return False
    except Exception as e:
        log.error(f"[ATOM] Error processing {url}: {e}")
        return False


def is_filterable_service(url: str) -> bool:
    """Check if a URL points to a filterable service (WFS, OGC API, etc.)."""
    url_lower = url.lower()
    return (
        "wfs" in url_lower or
        "ogc" in url_lower or 
        "features" in url_lower or
        "collections" in url_lower or
        ("arcgis" in url_lower and ("featureserver" in url_lower or "mapserver" in url_lower))
    )


def download_filterable_service(url: str, out_dir: Path, source: Dict,
                               global_bbox: Optional[List[float]], 
                               global_sr: Optional[int]) -> bool:
    """Download from a filterable service URL with bbox support."""
    try:
        from . import download_wfs, download_ogc, download_rest
        
        raw = source.get("raw", {})
        
        # Determine service type from URL
        url_lower = url.lower()
        
        if "wfs" in url_lower:
            # WFS service
            temp_source = {
                "name": f"{source['name']}_wfs_service",
                "url": url,
                "authority": source["authority"],
                "raw": raw.copy()
            }
            success, _ = download_wfs.process_wfs_source(
                temp_source, out_dir.parent, global_bbox, global_sr
            )
            return success
            
        elif "ogc" in url_lower or "features" in url_lower or "collections" in url_lower:
            # OGC API Features
            temp_source = {
                "name": f"{source['name']}_ogc_service",
                "url": url,
                "authority": source["authority"],
                "raw": raw.copy()
            }
            success, _ = download_ogc.process_ogc_source(
                temp_source, out_dir.parent, global_bbox, 
                f"http://www.opengis.net/def/crs/EPSG/0/{global_sr}" if global_sr else "CRS84",
                0.1
            )
            return success
            
        elif "arcgis" in url_lower and ("featureserver" in url_lower or "mapserver" in url_lower):
            # ArcGIS REST service
            temp_source = {
                "name": f"{source['name']}_rest_service",
                "url": url,
                "authority": source["authority"],
                "raw": raw.copy()
            }
            success, _ = download_rest.process_rest_source(
                temp_source, out_dir.parent, global_bbox, global_sr
            )
            return success
            
        else:
            log.warning(f"[ATOM] Unknown filterable service type for {url}")
            return False
            
    except Exception as e:
        log.error(f"[ATOM] Failed to download filterable service {url}: {e}")
        return False


def download_file(url: str, out_dir: Path) -> bool:
    """Download a single file from URL using robust utilities."""
    try:
        file_name = url.split("/")[-1].split("?")[0] or "download.zip"
        file_path = out_dir / file_name

        success = download_with_retries(url, file_path, max_retries=3, timeout=60)
        
        if success:
            log.info(f"[ATOM] Downloaded {file_name}")
        else:
            log.error(f"[ATOM] Failed to download {url}")
            
        return success

    except Exception as e:
        log.error(f"[ATOM] Failed to download {url}: {e}")
        return False