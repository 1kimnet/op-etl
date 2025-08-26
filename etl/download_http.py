#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
HTTP/File downloader for OP-ETL pipeline.
Handles only direct file downloads, leaves specialized types to their own modules.
"""

import logging
import re
import time
import urllib.error
import urllib.request
import zipfile
from datetime import datetime
from pathlib import Path

log = logging.getLogger(__name__)

# Character mapping for Swedish characters
CHAR_MAP = str.maketrans({
    "å": "a", "Å": "a",
    "ä": "a", "Ä": "a",
    "ö": "o", "Ö": "o",
    "é": "e", "É": "e",
    "ü": "u", "Ü": "u",
    "ß": "ss",
})

SAFE_RE = re.compile(r"[^a-z0-9_\-]+")

def slug(s: str, maxlen: int = 63) -> str:
    """Create safe slug from string."""
    s = (s or "unnamed").strip().lower().translate(CHAR_MAP)
    s = s.replace(" ", "_")
    s = SAFE_RE.sub("_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s[:maxlen] or "unnamed"


def run(cfg: dict) -> None:
    """Process file and HTTP sources only."""
    downloads_dir = Path(cfg["workspaces"]["downloads"])

    # Clean download folder if configured
    if cfg.get("cleanup_downloads_before_run", False) and downloads_dir.exists():
        import shutil
        log.info(f"Cleaning download directory: {downloads_dir}")
        shutil.rmtree(downloads_dir)

    downloads_dir.mkdir(parents=True, exist_ok=True)

    # Filter to only file/http sources
    file_sources = []
    for source in cfg.get("sources", []):
        source_type = source.get("type", "").lower()
        # Only handle plain file downloads, not specialized types
        if source_type in ("file", "http") and source.get("enabled", True):
            file_sources.append(source)

    if not file_sources:
        log.info("[HTTP] No file/HTTP sources to process")
        return

    for source in file_sources:
        try:
            log.info(f"[HTTP] Processing {source['name']}")
            process_file_source(source, downloads_dir)
        except Exception as e:
            log.error(f"[HTTP] Failed {source['name']}: {e}")


def process_file_source(source: dict, downloads_dir: Path) -> bool:
    """
    Process a single file/HTTP source.
    Handles index URLs where multiple layers are downloaded from a base URL.
    """
    authority = source.get("authority", "unknown")
    name = source.get("name", "unnamed")
    out_dir = downloads_dir / authority
    out_dir.mkdir(parents=True, exist_ok=True)

    # Case 1: URL is an index, and we need to download multiple layers
    raw_data = source.get("raw", {})
    if isinstance(raw_data.get("layer_name"), list):
        base_url = source["url"]
        if not base_url.endswith("/"):
            base_url += "/"

        layers = source["raw"]["layer_name"]
        if not layers:
            log.warning(f"Source '{name}' is a file index but contains no layers to download.")
            return False
        log.info(f"Source '{name}' is a file index. Found {len(layers)} layers to download.")
        results = []
        file_extension = source.get("file_extension", ".zip")
        for layer in layers:
            try:
                # Use configurable file extension, defaulting to .zip
                file_url = f"{base_url}{layer}{file_extension}"
                file_path = download_file(file_url, out_dir, layer)

                if file_path.suffix.lower() == ".zip":
                    extract_dir = out_dir / slug(file_path.stem)
                    extract_dir.mkdir(parents=True, exist_ok=True)
                    with zipfile.ZipFile(file_path, 'r') as zf:
                        zf.extractall(extract_dir)
                    log.info(f"[HTTP] Extracted '{layer}' to {extract_dir}")
                results.append(True)
            except Exception as e:
                log.error(f"[HTTP] Failed to process layer '{layer}' from '{name}': {e}")
                results.append(False)
        return all(results)

    # Case 2: URL is a single file
    else:
        url = source.get("url")
        if not url:
            log.warning(f"[HTTP] No URL for {name}")
            return False
        try:
            file_path = download_file(url, out_dir, name)
            if file_path.suffix.lower() == ".zip":
                extract_dir = out_dir / slug(file_path.stem)
                extract_dir.mkdir(parents=True, exist_ok=True)
                with zipfile.ZipFile(file_path, 'r') as zf:
                    zf.extractall(extract_dir)
                log.info(f"[HTTP] Extracted to {extract_dir}")
            return True
        except Exception as e:
            log.error(f"[HTTP] Download failed for {name}: {e}")
            return False


def download_file(url: str, out_dir: Path, hint: str) -> Path:
    """Download a file with retries."""
    from urllib.parse import unquote

    # Generate filename
    url_path = Path(url.split("?")[0])
    original_name = unquote(url_path.name) or "download"

    # Keep original extension
    ext = url_path.suffix
    if not ext or ext.lower() not in (".zip", ".json", ".geojson", ".gdb", ".gpkg", ".csv", ".txt", ".gz"):
        ext = ""

    # Use original name if reasonable
    if len(original_name) <= 50 and original_name != "download":
        fname = original_name
    else:
        base_name = slug(hint, maxlen=40)
        fname = f"{base_name}{ext}"

    dst = out_dir / fname

    # Add timestamp if file exists
    if dst.exists():
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
        name_without_ext = dst.stem
        fname = f"{name_without_ext}_{ts}{dst.suffix}"
        dst = out_dir / fname

    # Download with retries
    max_attempts = 3
    backoff = 2

    for attempt in range(1, max_attempts + 1):
        try:
            # Simple download using urllib
            headers = {"User-Agent": "op-etl/1.0"}
            req = urllib.request.Request(url, headers=headers)

            with urllib.request.urlopen(req, timeout=60) as response:
                with open(dst, "wb") as f:
                    f.write(response.read())

            # Verify file
            if dst.exists() and dst.stat().st_size > 0:
                log.info(f"[HTTP] Downloaded {dst.name} ({dst.stat().st_size} bytes)")
                return dst
            else:
                raise RuntimeError("Downloaded file is empty")

        except Exception as e:
            log.warning(f"[HTTP] Attempt {attempt}/{max_attempts} failed: {e}")
            if attempt < max_attempts:
                time.sleep(backoff)
                backoff *= 2

    # If all attempts fail, raise an error
    raise RuntimeError(f"Failed to download file from {url} after {max_attempts} attempts.")
