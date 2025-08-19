#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Step 1: Download
- Reads sources from sources.yaml (even if it's a bit messy)
- Writes into downloads/<authority>/...
- Expands ZIP archives
- Sanitizes names (incl. å/ä/ö -> a/o) for Windows/ArcGIS sanity
Dependencies: PyYAML, requests (bundled with ArcGIS Pro), stdlib
"""

import argparse
import logging
import re
import sys
import time
import zipfile
import os
import urllib.request
import urllib.error
from datetime import datetime
from pathlib import Path

# Try requests but have urllib as fallback
try:
    import requests
    HAS_REQUESTS = True
except Exception:  # pragma: no cover
    requests = None
    HAS_REQUESTS = False

# Prefer PyYAML; fail loud if missing
try:
    import yaml
except Exception as exc:  # pragma: no cover
    print("PyYAML is required in the ArcGIS Pro env. Error:", exc)
    sys.exit(2)


# ---------- naming helpers ----------
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
    s = (s or "unnamed").strip().lower().translate(CHAR_MAP)
    s = s.replace(" ", "_")
    s = SAFE_RE.sub("_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s[:maxlen] or "unnamed"


# ---------- YAML loader (tolerant-ish) ----------
def load_sources_yaml(path: Path):
    """
    Loads a list of sources from YAML.
    Tries to be tolerant to:
      - leading BOM, tabs
      - top-level being {'sources': [...] } OR just [...]
      - loose truthy strings for include
    """
    raw = path.read_text(encoding="utf-8", errors="ignore")

    # Normalizations for common human crimes
    raw = raw.replace("\t", "  ").lstrip("\ufeff")

    try:
        data = yaml.safe_load(raw)
    except yaml.YAMLError as e:
        print(f"YAML parse failed in {path}: {e}")
        sys.exit(2)

    if data is None:
        return []

    # Accept either {sources:[...]} or [...]
    if isinstance(data, dict) and "sources" in data:
        items = data.get("sources") or []
    elif isinstance(data, list):
        items = data
    else:
        print("YAML must be a list of sources or a mapping with 'sources:'.")
        sys.exit(2)

    norm = []
    for i, src in enumerate(items):
        if not isinstance(src, dict):
            print(f"Skipping item #{i} (not a mapping): {src!r}")
            continue

        # normalize fields
        name = slug(str(src.get("name") or src.get("id") or f"src_{i}"))
        stype = str(src.get("type") or "").strip().lower()
        url = (src.get("url") or src.get("href") or "").strip()

        # type synonyms we will likely see
        if stype in ("http", "file", "http_file", "download"):
            stype = "http" if stype != "file" else "file"
        elif stype in ("rest", "rest_api", "esri_rest", "arcgis_rest"):
            stype = "rest"
        elif stype in ("ogc", "ogc_api", "ogc_features", "ogc_api_features"):
            stype = "ogc"
        elif stype in ("atom", "atom_feed", "rss"):
            stype = "atom"

        include = src.get("include", True)
        if isinstance(include, str):
            include = include.strip().lower() in ("1", "true", "yes", "y")

        # authority: explicit or inferred from name prefix up to first underscore
        authority = slug(str(src.get("authority") or name.split("_", 1)[0]))

        # anything else we keep for later stages
        extra = {k: v for k, v in src.items() if k not in {"name", "id", "type", "href", "url", "include", "authority"}}

        norm.append({
            "name": name,
            "type": stype,
            "url": url,
            "include": bool(include),
            "authority": authority,
            "extra": extra,
            "_raw_index": i,
        })

    return norm


# ---------- filesystem + download ----------
def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)
    return p

def http_download(url: str, out_dir: Path, file_hint: str) -> Path:
    """
    Download a URL to a file inside out_dir and return the final local Path.
    Uses urllib as primary method to avoid any requests-related recursion issues.
    """
    ensure_dir(out_dir)

    # Derive filename from URL if absent
    base_from_url = slug(Path(url.split("?")[0]).name) or "download"
    # Try to keep original extension (zip, gdb.zip, geojson, etc.)
    ext = Path(url.split("?")[0]).suffix
    if ext.lower() not in (".zip", ".json", ".geojson", ".gdb", ".gpkg", ".csv", ".txt", ".gz"):
        ext = ""  # unknown, fine

    fname = f"{slug(file_hint)}__{base_from_url}{ext}"
    dst = out_dir / fname

    # Add timestamp if file exists
    if dst.exists():
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
        dst = out_dir / f"{dst.stem}_{ts}{dst.suffix}"

    # Iterative retry loop with exponential backoff and atomic write
    tries = 3
    backoff = 2
    last_err = None
    tmp_dst = out_dir / f"{dst.name}.part"

    for attempt in range(1, tries + 1):
        try:
            # Use urllib.request to avoid any requests library recursion issues
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=60) as response:
                # Write to temporary file then move atomically
                with open(tmp_dst, "wb") as f:
                    f.write(response.read())

            # Atomically move into place
            os.replace(str(tmp_dst), str(dst))

            # Basic validation: file must be non-empty
            if dst.exists() and dst.stat().st_size > 0:
                logging.info(f"Downloaded {url} -> {dst} ({dst.stat().st_size} bytes)")
                return dst
            else:
                raise RuntimeError("Downloaded file is empty or missing after write")

        except Exception as e:
            last_err = e
            logging.warning("Download attempt %d/%d failed for %s: %s", attempt, tries, url, e)
            # remove partial file if present
            try:
                if tmp_dst.exists():
                    tmp_dst.unlink()
            except Exception:
                pass
            if attempt < tries:
                time.sleep(backoff)
                backoff *= 2

    raise RuntimeError(f"Download failed after retries: {url} :: {last_err}")


def maybe_unzip(path: Path, extract_root: Path) -> Path | None:
    """
    Return the extraction directory if `path` is a ZIP archive, otherwise None.


    If `path` has a ".zip" suffix (case-insensitive), extracts its contents into
    `extract_root/<slug(path.stem)>`, creating that directory if needed, and returns
    the Path to the created directory. For non-ZIP paths returns `None`.
    """
    if path.suffix.lower() != ".zip":
        return None

    target_dir = extract_root / slug(path.stem)
    ensure_dir(target_dir)

    with zipfile.ZipFile(path, "r") as zf:
        zf.extractall(target_dir)

    return target_dir


# ---------- runner ----------
def run(cfg: dict) -> None:
    """
    Run the download stage of the OP-ETL pipeline using the provided configuration.


    This function reads source definitions from cfg["sources"] and a downloads workspace path from
    cfg["workspaces"]["downloads"], normalizes each source, and for sources of type "http" or "file"
    downloads the referenced URL into downloads/<authority>/..., optionally extracting ZIP archives.
    Sources with types "rest", "ogc", "atom", and "atom_feed" are recognized but only recorded as
    TODO (no network interactions are performed for them here). Per-source errors are caught and
    logged; successful operations are logged with elapsed time and the resulting path or status.


    Expected cfg structure (minimal):
    {
      "workspaces": { "downloads": "<path-to-downloads-root>" },
      "sources": [ ... ]  # list of source dicts (see load_sources_yaml for canonical fields)
    }


    Side effects:
    - Creates directories under the configured downloads workspace.
    - Writes downloaded files and extracted archive contents to disk.
    - Emits INFO/ERROR logs for each source.


    Returns:
    - None
    """
    raw_sources = cfg.get("sources", [])
    downloads_dir = Path(cfg["workspaces"]["downloads"])
    ensure_dir(downloads_dir)

    # Normalize sources to match the expected format from load_sources_yaml
    normalized_sources = []
    for i, src in enumerate(raw_sources):
        if not isinstance(src, dict):
            continue

        # Normalize fields to match load_sources_yaml output
        name = slug(str(src.get("name") or src.get("id") or f"src_{i}"))
        stype = str(src.get("type") or "").strip().lower()
        url = (src.get("url") or src.get("href") or "").strip()

        # Type synonyms we will likely see
        if stype in ("http", "file", "http_file", "download"):
            stype = "http" if stype != "file" else "file"
        elif stype in ("rest", "rest_api", "esri_rest", "arcgis_rest"):
            stype = "rest"
        elif stype in ("ogc", "ogc_api", "ogc_features", "ogc_api_features"):
            stype = "ogc"
        elif stype in ("atom", "atom_feed", "rss"):
            stype = "atom"

        # Handle enabled/include field
        include = src.get("include", src.get("enabled", True))
        if isinstance(include, str):
            include = include.strip().lower() in ("1", "true", "yes", "y")

        # Authority: explicit or inferred from name prefix up to first underscore
        authority = slug(str(src.get("authority") or name.split("_", 1)[0]))

        # Anything else we keep for later stages
        extra = {k: v for k, v in src.items()
                 if k not in {"name", "id", "type", "href", "url", "include", "enabled", "authority"}}

        normalized_sources.append({
            "name": name,
            "type": stype,
            "url": url,
            "include": bool(include),
            "authority": authority,
            "extra": extra,
            "_raw_index": i,
        })

    # Filter to file/HTTP-like sources since this is download_http
    srcs = [s for s in normalized_sources if s.get("type") in {"http", "file", "atom_feed", "atom"}]

    for s in srcs:
        if not s["include"]:
            logging.info(f"Skipping {s['name']} (include=false)")
            continue

        t0 = time.time()
        try:
            auth_dir = ensure_dir(downloads_dir / s["authority"])
            # Treat local file downloads and plain HTTP the same: fetch the URL to downloads
            if s["type"] in ("http", "file"):
                file_path = http_download(s["url"], auth_dir, s["name"])
                extracted = maybe_unzip(file_path, auth_dir)
                msg_path = str(extracted or file_path)
                status = "OK"
            elif s["type"] in ("rest", "ogc", "atom", "atom_feed"):
                # Placeholders for later steps; for now we just record intention.
                msg_path = f"planned handler not implemented yet for type={s['type']}"
                status = "TODO"
            else:
                msg_path = f"unknown type={s['type']}"
                status = "SKIP"

            dt = round(time.time() - t0, 2)
            logging.info(f"{s['name']} ({s['type']}): {status} in {dt}s. Path: {msg_path}")

        except Exception as e:
            dt = round(time.time() - t0, 2)
            logging.error(f"{s['name']} ({s['type']}): FAIL in {dt}s. Error: {e}")


def run_download(sources_or_path, downloads_dir):
    # deprecation shim
    import warnings
    warnings.warn("run_download is deprecated; use run(cfg)", DeprecationWarning)
    # build a tiny cfg and call run(...)
    if isinstance(sources_or_path, list):
        cfg = {"sources": sources_or_path, "workspaces": {"downloads": downloads_dir}}
    else:
        from pathlib import Path
        path = Path(sources_or_path)
        srcs = yaml.safe_load(path.read_text(encoding="utf-8"))["sources"]
        cfg = {"sources": srcs, "workspaces": {"downloads": downloads_dir}}
    return run(cfg)


def main():
    ap = argparse.ArgumentParser(description="OP-ETL step 1: download sources (bbox-ready)")
    ap.add_argument("--sources", default="config/sources.yaml")  # moved into config/
    ap.add_argument("--downloads", default="downloads")
    a = ap.parse_args()
    run_download(
        sources_or_path=Path(a.sources),
        downloads_dir=Path(a.downloads),
    )


# HTTP headers for downloads
HEADERS = {"User-Agent": "op-etl/0.1 (+contact@example.org)"}


def query_all(fl, where="1=1", geom=None, out_sr=None, page_size=2000):
    start = 0
    while True:
        fs = fl.query(where=where, geometry_filter=geom, out_fields="*",
                      result_offset=start, result_record_count=page_size, out_sr=out_sr)
        if not fs or len(fs) == 0:
            break
        yield fs
        if len(fs) < page_size:
            break
        start += page_size

# Atom feed handler stub
def handle_atom_feed(feed_url):
    logging.info("[ATOM] Skipped handling feed: %s", feed_url)

if __name__ == "__main__":
    main()
