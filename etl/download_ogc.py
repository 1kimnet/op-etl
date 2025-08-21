"""
ATOM feed downloader for OP-ETL pipeline.
Simple, working implementation without recursion issues.
"""

import logging
import requests
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict

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
        try:
            log.info(f"Processing ATOM source: {source['name']}")
            process_atom_source(source, downloads_dir)
        except Exception as e:
            log.error(f"[ATOM] Failed {source['name']}: {e}")


def process_atom_source(source: Dict, downloads_dir: Path) -> bool:
    """Process a single ATOM source."""
    url = source["url"]
    authority = source["authority"]

    # Create output directory
    out_dir = downloads_dir / authority
    out_dir.mkdir(parents=True, exist_ok=True)

    try:
        # Fetch ATOM feed
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        # Parse XML
        root = ET.fromstring(response.content)

        # Define namespace
        ns = {"atom": "http://www.w3.org/2005/Atom"}

        # Find download links
        download_count = 0
        entries = root.findall(".//atom:entry", ns)

        for entry in entries:
            links = entry.findall("atom:link", ns)
            for link in links:
                if link.get("rel") == "enclosure" or link.get("type") in ["application/zip", "application/x-zip-compressed"]:
                    href = link.get("href")
                    if href:
                        success = download_file(href, out_dir)
                        if success:
                            download_count += 1

        log.info(f"[ATOM] Downloaded {download_count} files from {source['name']}")
        return download_count > 0

    except Exception as e:
        log.error(f"[ATOM] Error processing {url}: {e}")
        return False


def download_file(url: str, out_dir: Path) -> bool:
    """Download a single file from URL."""
    try:
        file_name = url.split("/")[-1].split("?")[0] or "download.zip"
        file_path = out_dir / file_name

        response = requests.get(url, timeout=60, stream=True)
        response.raise_for_status()

        with open(file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        log.info(f"[ATOM] Downloaded {file_name}")
        return True

    except Exception as e:
        log.error(f"[ATOM] Failed to download {url}: {e}")
        return False