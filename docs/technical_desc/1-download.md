# OP-ETL — Step 1: Download (bbox-ready)

A clean restart for the ETL repo focusing only on the download step. Reads `sources.yaml` (tolerant), writes to `downloads/<authority>/...`, expands archives, and applies bbox filters for REST and OGC API services. No ArcPy involved.

---

## Scope

* Support source types: `http` (file), `rest` (ArcGIS REST), `ogc` (OGC API Features). `atom` is stubbed for later.
* Optional global `defaults.bbox` with per-source override via `raw.bbox`.
* Output:

  * HTTP: downloaded file path and extracted folder if ZIP
  * REST/OGC: one or more `part_XXXX.geojson` (or `.json`) files in a per-source folder
* Logging to `logs/download.csv`.

---

## Repository bootstrap (minimal)

```
op-etl/
├─ .gitignore
├─ download.py
├─ rest_fetch.py
├─ ogc_fetch.py
├─ config/
│  ├─ config.yaml
│  └─ sources.yaml
├─ logs/              # gitignored output
└─ downloads/         # gitignored output
```

`.gitignore` (suggested):

```
/downloads/
/logs/
/staging.gdb/
__pycache__/
*.pyc
```

---

## `sources.yaml` schema (simple and tolerant)

Top-level can be either a list of sources or a mapping with `sources:`. Optional `defaults:` allows a shared bbox.

```yaml
# Optional global defaults applied when a source lacks raw.bbox
defaults:
  bbox: [608000, 6578000, 668000, 6628000]  # minx, miny, maxx, maxy in EPSG:3006 (SWEREF99 TM)
  bbox_sr: 3006

# Either top-level list…
- name: FM_Roads
  authority: fm                     # folder under downloads/
  type: http                        # synonyms: http_file, file -> http
  url: https://example.com/fm/roads_fgdb.zip
  include: true

- name: NVDB_Vag
  authority: nvdb
  type: rest                        # synonyms: rest_api, esri_rest, arcgis_rest -> rest
  url: https://server/ArcGIS/rest/services/NVDB/FeatureServer/0
  include: true
  raw:
    format: geojson                 # geojson or json (fallback happens automatically)
    where_clause: "1=1"
    out_fields: "*"
    page_size: 5000
    bbox: [608000, 6578000, 668000, 6628000]  # overrides defaults if present
    bbox_sr: 3006

- name: SGU_Erosion
  authority: sgu
  type: ogc                         # synonym: ogc_api -> ogc
  url: https://api.example.com/collections/
  include: true
  raw:
    collections: ["aktiv-erosion"]
    page_size: 1000
    supports_bbox_crs: true         # only set if server supports bbox-crs
    bbox: [608000, 6578000, 668000, 6628000]
    bbox_sr: 3006

# …or mapping with sources:
# sources:
#   - name: ...
```

### Field notes

* `authority` determines `downloads/<authority>/...` destination. If omitted, inferred from name prefix before first underscore.
* `include` may be `true/false/"yes"/1`.
* For ArcGIS MapServer root URLs, provide `raw.layer_ids: [0,1,...]`.
* `ogc.raw.supports_bbox_crs` should be true only if the server accepts `bbox-crs=EPSG:xxxx`.

---

## CLI usage

```bash
python download.py --sources sources.yaml --downloads ./downloads --logcsv ./logs/download.csv
# limit to specific sources
python download.py --sources sources.yaml --only NVDB_Vag SGU_Erosion
```

---

## Implementation

### `download.py`

Single entrypoint that:

* Parses `sources.yaml` (tolerant)
* Normalizes source types and names
* Inherits `defaults.bbox` when `raw.bbox` is missing
* Dispatches to HTTP, REST, OGC fetchers
* Expands ZIP files and logs results

```python
#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse, csv, os, re, sys, time, zipfile
from datetime import datetime
from pathlib import Path

try:
    import requests
except Exception:
    requests = None

import yaml

import rest_fetch, ogc_fetch

CHAR_MAP = str.maketrans({"å":"a","Å":"a","ä":"a","Ä":"a","ö":"o","Ö":"o","é":"e","É":"e","ü":"u","Ü":"u","ß":"ss"})
SAFE_RE = re.compile(r"[^a-z0-9_\-]+")

def slug(s: str, maxlen: int = 63) -> str:
    s = (s or "unnamed").strip().lower().translate(CHAR_MAP)
    s = s.replace(" ", "_")
    s = SAFE_RE.sub("_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s[:maxlen] or "unnamed"

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)
    return p

# ---------- YAML ----------

def _normalize_type(stype: str) -> str:
    t = (stype or "").strip().lower()
    if t in ("http", "file", "http_file", "download"): return "http"
    if t in ("rest", "rest_api", "esri_rest", "arcgis_rest"): return "rest"
    if t in ("ogc", "ogc_api", "ogc_api_features"): return "ogc"
    if t in ("atom", "atom_feed", "rss"): return "atom"
    return t or "unknown"

def load_yaml_with_defaults(path: Path):
    raw = path.read_text(encoding="utf-8", errors="ignore").replace("\t", "  ").lstrip("\ufeff")
    doc = yaml.safe_load(raw) or {}
    if isinstance(doc, list):
        sources = doc
        defaults = {}
    elif isinstance(doc, dict):
        sources = doc.get("sources", [])
        defaults = doc.get("defaults", {}) or {}
    else:
        raise SystemExit("sources.yaml must be a list or a mapping with 'sources'.")
    norm = []
    for i, src in enumerate(sources):
        if not isinstance(src, dict):
            continue
        name = slug(str(src.get("name") or src.get("id") or f"src_{i}"))
        stype = _normalize_type(src.get("type"))
        url = (src.get("url") or src.get("href") or "").strip()
        include = src.get("include", True)
        if isinstance(include, str):
            include = include.strip().lower() in ("1","true","yes","y")
        authority = slug(str(src.get("authority") or name.split("_",1)[0]))
        extra = {k:v for k,v in src.items() if k not in {"name","id","type","href","url","include","authority"}}
        norm.append({
            "name": name, "type": stype, "url": url, "include": bool(include),
            "authority": authority, "extra": extra, "_raw_index": i
        })
    return norm, defaults

# ---------- HTTP ----------

def http_download(url: str, out_dir: Path, file_hint: str) -> Path:
    if not requests:
        raise RuntimeError("requests not available in this Python environment.")
    ensure_dir(out_dir)
    base_from_url = slug(Path(url.split("?")[0]).name) or "download"
    ext = Path(url.split("?")[0]).suffix
    if ext.lower() not in (".zip", ".json", ".geojson", ".gdb", ".gpkg", ".csv", ".txt", ".gz"):
        ext = ""
    fname = f"{slug(file_hint)}__{base_from_url}{ext}"
    dst = out_dir / fname
    if dst.exists():
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
        dst = out_dir / f"{dst.stem}_{ts}{dst.suffix}"
    tries, last_err = 3, None
    for _ in range(tries):
        try:
            with requests.get(url, stream=True, timeout=(10, 120)) as r:
                r.raise_for_status()
                with open(dst, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024*256):
                        if chunk:
                            f.write(chunk)
            return dst
        except Exception as e:
            last_err = e
            time.sleep(2)
    raise RuntimeError(f"Download failed after retries: {url} :: {last_err}")

def maybe_unzip(path: Path, extract_root: Path) -> Path | None:
    if path.suffix.lower() != ".zip":
        return None
    target_dir = extract_root / slug(path.stem)
    ensure_dir(target_dir)
    with zipfile.ZipFile(path, "r") as zf:
        zf.extractall(target_dir)
    return target_dir

# ---------- bbox inheritance ----------

def inherit_raw_defaults(extra: dict, defaults: dict) -> dict:
    raw = (extra or {}).get("raw", {}) or {}
    bbox = raw.get("bbox"); bbox_sr = raw.get("bbox_sr")
    if bbox is None and defaults.get("bbox"): bbox = defaults["bbox"]
    if bbox_sr is None and defaults.get("bbox_sr"): bbox_sr = defaults["bbox_sr"]
    raw["bbox"], raw["bbox_sr"] = bbox, bbox_sr
    return raw

# ---------- run ----------

def run_download(sources_path: Path, downloads_root: Path, only: set[str] | None = None, log_csv: Path | None = None):
    srcs, defaults = load_yaml_with_defaults(sources_path)
    ensure_dir(downloads_root)
    writer = None
    log_f = None
    if log_csv:
        ensure_dir(log_csv.parent)
        log_f = open(log_csv, "w", newline="", encoding="utf-8")
        writer = csv.writer(log_f)
        writer.writerow(["when","source","type","authority","status","path_or_msg","seconds"])

    def _log(row):
        if writer:
            writer.writerow(row); log_f.flush()
        else:
            print(" | ".join(str(x) for x in row))

    for s in srcs:
        if only and s["name"] not in only: continue
        if not s["include"]:
            _log([datetime.now().isoformat(timespec="seconds"), s["name"], s["type"], s["authority"], "SKIP", "include=false", 0])
            continue
        t0 = time.time()
        try:
            auth_dir = ensure_dir(downloads_root / s["authority"])
            raw = inherit_raw_defaults(s.get("extra"), defaults)
            if s["type"] == "http":
                file_path = http_download(s["url"], auth_dir, s["name"])
                extracted = maybe_unzip(file_path, auth_dir)
                msg_path, status = str(extracted or file_path), "OK"
            elif s["type"] == "rest":
                out_folder = auth_dir / s["name"]
                rest_fetch.fetch_to_folder(
                    s["url"], out_folder,
                    bbox=raw.get("bbox"), bbox_sr=int(raw.get("bbox_sr") or 3006),
                    out_format=(raw.get("format") or "geojson"),
                    where=raw.get("where_clause") or "1=1",
                    out_fields=raw.get("out_fields") or "*",
                    page_size=int(raw.get("page_size") or 5000),
                    layer_ids=raw.get("layer_ids")
                )
                msg_path, status = str(out_folder), "OK"
            elif s["type"] == "ogc":
                out_folder = auth_dir / s["name"]
                ogc_fetch.fetch_to_folder(
                    s["url"], out_folder,
                    collections=raw.get("collections") or [],
                    page_size=int(raw.get("page_size") or 1000),
                    bbox=raw.get("bbox"),
                    bbox_sr=int(raw.get("bbox_sr") or 3006),
                    supports_bbox_crs=bool(raw.get("supports_bbox_crs"))
                )
                msg_path, status = str(out_folder), "OK"
            else:
                msg_path, status = f"unknown or unimplemented type={s['type']}", "SKIP"
            dt = round(time.time() - t0, 2)
            _log([datetime.now().isoformat(timespec="seconds"), s["name"], s["type"], s["authority"], status, msg_path, dt])
        except Exception as e:
            dt = round(time.time() - t0, 2)
            _log([datetime.now().isoformat(timespec="seconds"), s["name"], s["type"], s["authority"], "FAIL", str(e), dt])

    if log_f: log_f.close()

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="OP-ETL step 1: download sources (bbox-ready)")
    ap.add_argument("--sources", default="sources.yaml")
    ap.add_argument("--downloads", default="downloads")
    ap.add_argument("--only", nargs="*")
    ap.add_argument("--logcsv", default="logs/download.csv")
    a = ap.parse_args()
    run_download(Path(a.sources), Path(a.downloads), set(a.only) if a.only else None, Path(a.logcsv) if a.logcsv else None)
```

### `rest_fetch.py`

```python
from pathlib import Path
from urllib.parse import urljoin
import json, time
import requests

def _ensure_dir(p: Path): p.mkdir(parents=True, exist_ok=True)

def _is_layer_url(url: str) -> bool:
    return url.rstrip("/").split("/")[-1].isdigit()

def _layer_urls(url: str, layer_ids):
    if _is_layer_url(url):
        return [url.rstrip("/")]
    if not layer_ids:
        raise ValueError("MapServer root requires raw.layer_ids.")
    base = url.rstrip("/") + "/"
    return [urljoin(base, str(i)) for i in layer_ids]

def fetch_to_folder(url: str, out_dir: Path, *, bbox=None, bbox_sr=3006, out_format="geojson",
                    where="1=1", out_fields="*", page_size=5000, layer_ids=None, timeout=(10,180)):
    _ensure_dir(out_dir)
    parts = 0
    for lyr in _layer_urls(url, layer_ids):
        offset = 0
        while True:
            params = {
                "where": where,
                "outFields": out_fields,
                "resultOffset": offset,
                "resultRecordCount": page_size,
                "f": "geojson" if out_format.lower() == "geojson" else "json",
                "outSR": bbox_sr,
            }
            if bbox:
                params.update({
                    "geometryType": "esriGeometryEnvelope",
                    "spatialRel": "esriSpatialRelIntersects",
                    "inSR": bbox_sr,
                    "geometry": json.dumps({
                        "xmin": bbox[0], "ymin": bbox[1],
                        "xmax": bbox[2], "ymax": bbox[3],
                        "spatialReference": {"wkid": bbox_sr}
                    }),
                })
            r = requests.get(lyr + "/query", params=params, timeout=timeout)
            if r.status_code == 400 and out_format.lower() == "geojson":
                params["f"] = "json"
                r = requests.get(lyr + "/query", params=params, timeout=timeout)
            r.raise_for_status()
            text = r.text
            if '"features":[]' in text or '"features" : []' in text:
                break
            parts += 1
            suffix = "geojson" if params["f"] == "geojson" else "json"
            (out_dir / f"part_{parts:04d}.{suffix}").write_text(text, encoding="utf-8")
            offset += page_size
            time.sleep(0.2)
    return out_dir
```

### `ogc_fetch.py`

```python
from pathlib import Path
from urllib.parse import urljoin
import requests, time

def _ensure_dir(p: Path): p.mkdir(parents=True, exist_ok=True)

def _next_link(links):
    if not isinstance(links, list): return None
    for l in links:
        if isinstance(l, dict) and l.get("rel") == "next" and l.get("href"):
            return l["href"]
    return None

def fetch_to_folder(base_collections_url: str, out_dir: Path, *,
                    collections, page_size=1000, bbox=None, bbox_sr=3006,
                    supports_bbox_crs=False, timeout=(10,180)):
    _ensure_dir(out_dir)
    parts = 0
    base = base_collections_url.rstrip("/") + "/"
    for coll in collections:
        coll_items = urljoin(base, f"{coll}/items")
        params = {"limit": page_size}
        if bbox:
            params["bbox"] = ",".join(str(v) for v in bbox)
            if supports_bbox_crs:
                params["bbox-crs"] = f"EPSG:{bbox_sr}"
        url = coll_items
        while url:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            js = r.json()
            feats = js.get("features", [])
            if not feats:
                break
            parts += 1
            (out_dir / f"part_{parts:04d}.geojson").write_text(r.text, encoding="utf-8")
            nxt = _next_link(js.get("links", []))
            url, params = (nxt, None) if nxt else (None, None)
            time.sleep(0.2)
    return out_dir
```

---

## Sanity checklist

* `defaults.bbox` present at top of `sources.yaml` (EPSG:3006 suggested)
* High-value REST/OGC sources have explicit `raw.bbox` if they differ from defaults
* First run produces:

  * HTTP: files under `downloads/<authority>/...` (ZIPs expanded)
  * REST/OGC: per-source folder with `part_*.geojson` or `part_*.json`
* `logs/download.csv` contains rows with `OK` for implemented types, `FAIL` with errors otherwise

---

## Next steps (after downloads are boring)

1. Implement `atom_feed` handler to follow enclosure links and download files.
2. Stage step: import downloaded content into `staging.gdb` with ArcPy, naming rules aligned with `slug()`.
3. Optional geoprocessing: project to target SRID and clip to AOI for non-filterable sources (e.g., ZIP shapefiles).
4. Load to SDE.
