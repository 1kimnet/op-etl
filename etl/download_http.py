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
import csv
import re
import sys
import time
import zipfile
from datetime import datetime
from pathlib import Path

# Prefer requests if available (Pro should have it)
try:
    import requests
except Exception:  # pragma: no cover
    requests = None

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
            stype = "http"
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
    if not requests:
        raise RuntimeError("requests not available in this Python environment.")
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

    # Simple retry loop
    tries, last_err = 3, None
    for _ in range(tries):
        try:
            with requests.get(url, stream=True, timeout=(10, 120)) as r:
                r.raise_for_status()
                with open(dst, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 256):
                        if chunk:
                            f.write(chunk)
            return dst
        except Exception as e:
            last_err = e
            time.sleep(2)
    raise RuntimeError(f"Download failed after retries: {url} :: {last_err}")

def maybe_unzip(path: Path, extract_root: Path) -> Path | None:
    """
    If path is a ZIP, extract into extract_root/<path_stem> and return that folder.
    Otherwise return None.
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
    """Unified entrypoint that reads from merged cfg."""
    raw_sources = cfg.get("sources", [])
    downloads_dir = Path(cfg["workspaces"]["downloads"])

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
            stype = "http"
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

    # Reuse existing implementation
    run_download(normalized_sources, downloads_dir)

def run_download(sources_or_path, downloads_root: Path, only: set[str] | None = None, log_csv: Path | None = None):
    """
    Backward-compatible:
    - If sources_or_path is a path-like, load YAML from file.
    - If it's already a list[dict], use it as-is.
    """
    if isinstance(sources_or_path, (str, Path)):
        srcs = load_sources_yaml(Path(sources_or_path))
    elif isinstance(sources_or_path, list):
        # Filter to file/HTTP-like sources since this is download_http
        srcs = [s for s in sources_or_path if s.get("type") in {"http", "file", "atom_feed", "atom"}]
    else:
        raise TypeError("run_download expects a path or a list of source dicts")

    ensure_dir(downloads_root)

    if log_csv:
        ensure_dir(log_csv.parent)
        log_f = open(log_csv, "w", newline="", encoding="utf-8")
        writer = csv.writer(log_f)
        writer.writerow(["when", "source", "type", "authority", "status", "path_or_msg", "seconds"])
    else:
        writer = None
        log_f = None

    def _log(row):
        if writer:
            writer.writerow(row)
            log_f.flush()  # type: ignore
        else:
            print(" | ".join(str(x) for x in row))

    for s in srcs:
        if only and s["name"] not in only:
            continue
        if not s["include"]:
            _log([datetime.now().isoformat(timespec="seconds"), s["name"], s["type"], s["authority"], "SKIP", "include=false", 0])
            continue

        t0 = time.time()
        try:
            auth_dir = ensure_dir(downloads_root / s["authority"])
            if s["type"] == "http":
                file_path = http_download(s["url"], auth_dir, s["name"])
                extracted = maybe_unzip(file_path, auth_dir)
                msg_path = str(extracted or file_path)
                status = "OK"
            elif s["type"] in ("rest", "ogc", "atom"):
                # Placeholders for later steps; for now we just record intention.
                msg_path = f"planned handler not implemented yet for type={s['type']}"
                status = "TODO"
            else:
                msg_path = f"unknown type={s['type']}"
                status = "SKIP"

            dt = round(time.time() - t0, 2)
            _log([datetime.now().isoformat(timespec="seconds"), s["name"], s["type"], s["authority"], status, msg_path, dt])

        except Exception as e:
            dt = round(time.time() - t0, 2)
            _log([datetime.now().isoformat(timespec="seconds"), s["name"], s["type"], s["authority"], "FAIL", str(e), dt])

    if log_f:
        log_f.close()


def main():
    ap = argparse.ArgumentParser(description="OP-ETL step 1: download sources (bbox-ready)")
    ap.add_argument("--sources", default="config/sources.yaml")  # moved into config/
    ap.add_argument("--downloads", default="downloads")
    ap.add_argument("--only", nargs="*")
    ap.add_argument("--logcsv", default="logs/download.csv")
    a = ap.parse_args()
    run_download(
        sources_or_path=Path(a.sources),
        downloads_root=Path(a.downloads),
        only=set(a.only) if a.only else None,
        log_csv=Path(a.logcsv) if a.logcsv else None,
    )


if __name__ == "__main__":
    main()
