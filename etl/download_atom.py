"""
ATOM feed downloader for OP-ETL pipeline.
Enhanced implementation with recursion depth protection.
"""

import logging
from pathlib import Path
from typing import Dict

from .http_utils import RecursionSafeSession, safe_xml_parse, download_with_retries, validate_response_content
from .monitoring import start_monitoring_source, end_monitoring_source

log = logging.getLogger(__name__)


def run(cfg: dict) -> None:
    """Process all ATOM sources in configuration."""
    # Get fresh sources list without circular references
    atom_sources = []
    for source in cfg.get("sources", []):
        if source.get("type") == "atom" and source.get("enabled", True):
            # Create clean copy without nested references
            atom_sources.append({
                "name": source.get("name"),
                "url": source.get("url"),
                "authority": source.get("authority", "unknown")
            })

    if not atom_sources:
        log.info("[ATOM] No ATOM sources to process")
        return

    downloads_dir = Path(cfg["workspaces"]["downloads"])

    for source in atom_sources:
        metric = start_monitoring_source(source['name'], source['authority'], 'atom')
        
        try:
            log.info(f"Processing ATOM source: {source['name']}")
            success = process_atom_source(source, downloads_dir)
            end_monitoring_source(success, files=1 if success else 0)
        except RecursionError as e:
            log.error(f"[ATOM] Recursion error in {source['name']}: {e}")
            end_monitoring_source(False, 'RecursionError', str(e))
        except Exception as e:
            log.error(f"[ATOM] Failed {source['name']}: {e}")
            end_monitoring_source(False, type(e).__name__, str(e))


def process_atom_source(source: Dict, downloads_dir: Path) -> bool:
    """Process a single ATOM source with enhanced error handling."""
    url = source["url"]
    authority = source["authority"]

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
                    if link.get("rel") == "enclosure" or link.get("type") in ["application/zip", "application/x-zip-compressed"]:
                        href = link.get("href")
                        if href:
                            success = download_file(href, out_dir)
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