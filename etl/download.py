"""
Unified downloader for OP-ETL.

Dispatches per-source by `type` and uses the existing per-source processors to avoid duplication:
- http|file  -> download_http.process_file_source
- atom       -> download_atom.process_atom_source
- ogc        -> download_ogc.process_ogc_source
- wfs        -> download_wfs.process_wfs_source
- rest       -> download_rest.process_rest_source

A single pass handles optional downloads cleanup and monitoring per source.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

from .monitoring import end_monitoring_source, start_monitoring_source

log = logging.getLogger(__name__)


def _maybe_cleanup_downloads(cfg: dict) -> None:
    downloads_dir = Path(cfg["workspaces"]["downloads"])
    if cfg.get("cleanup_downloads_before_run", False) and downloads_dir.exists():
        import shutil
        log.info(f"Cleaning download directory: {downloads_dir}")
        shutil.rmtree(downloads_dir)
    downloads_dir.mkdir(parents=True, exist_ok=True)


def run(cfg: dict, *, authority: Optional[str] = None, type: Optional[str] = None) -> None:
    """Run unified downloads with optional filters.

    Filters:
    - authority: only run sources for a given authority
    - type: only run sources of a given type
    """
    _maybe_cleanup_downloads(cfg)

    downloads_dir = Path(cfg["workspaces"]["downloads"]).resolve()

    # Configure globals by family
    from . import download_atom as atom
    from . import download_http as http
    from . import download_ogc as ogc
    from . import download_rest as rest
    from . import download_wfs as wfs

    global_bbox_rest, global_sr_rest = rest._extract_global_bbox(cfg)
    global_bbox_wfs, global_sr_wfs = wfs._extract_global_bbox(cfg)
    global_bbox_ogc, global_crs_ogc = ogc._extract_global_bbox(cfg)
    ogc_delay = float(cfg.get("ogc_api_delay", 0.1) or 0)

    sources = [s for s in cfg.get("sources", []) if s.get("enabled", True)]
    if authority:
        sources = [s for s in sources if s.get("authority") == authority]
    if type:
        sources = [s for s in sources if (s.get("type") or "").lower() == type.lower()]

    if not sources:
        log.info("[DL] No sources to process after filtering")
        return

    for s in sources:
        stype = (s.get("type") or "").lower()
        name = s.get("name") or "unnamed"
        auth = s.get("authority") or "unknown"

        start_monitoring_source(name, auth, stype or "download")
        ok = False
        features_or_files = 0
        try:
            if stype in ("http", "file"):
                ok = http.process_file_source(s, downloads_dir)
                features_or_files = 1 if ok else 0

            elif stype == "atom":
                # Atom expects List[float] | None; coerce to List[float] when provided
                if isinstance(global_bbox_rest, (list, tuple)):
                    bbox_atom = [float(v) for v in list(global_bbox_rest)]  # type: ignore[assignment]
                else:
                    bbox_atom = None
                ok = atom.process_atom_source(s, downloads_dir, bbox_atom, global_sr_rest)
                features_or_files = 1 if ok else 0

            elif stype == "ogc":
                ok, features_or_files = ogc.process_ogc_source(
                    s, downloads_dir, global_bbox_ogc, global_crs_ogc, ogc_delay
                )

            elif stype == "wfs":
                ok, files = wfs.process_wfs_source(
                    s, downloads_dir, global_bbox_wfs, global_sr_wfs
                )
                features_or_files = files

            elif stype in ("rest", "rest_api"):
                ok, features_or_files = rest.process_rest_source(
                    s, downloads_dir, global_bbox_rest, global_sr_rest
                )

            else:
                log.info(f"[DL] Skipping unsupported type: {stype}")
                ok = True

        except Exception as e:
            log.error(f"[DL] Failed {name} ({stype}): {e}")
            ok = False

        # Report via monitoring (features for data sources, files for file downloads)
        if stype in ("http", "file", "atom", "wfs"):
            end_monitoring_source(ok, files=features_or_files if ok else 0)
        else:
            end_monitoring_source(ok, features=features_or_files if ok else 0)
