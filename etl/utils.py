"""Small ETL utilities used by staging and other modules.

Keep these helpers minimal: choose best candidate by feature count.
"""
from pathlib import Path
import logging
import sys
from typing import List, Optional


def best_shapefile_by_count(paths: List[Path]) -> Optional[Path]:
    """Return the Path with the highest feature count (>0) or None.

    Imports ``arcpy`` inside the function so this module can be imported
    in non-ArcPy environments for static analysis.
    """
    try:
        import arcpy
    except Exception:
        logging.debug("[UTIL] arcpy not available; cannot count features")
        return None

    best: Optional[Path] = None
    best_count = -1
    for p in paths:
        try:
            res = arcpy.management.GetCount(str(p))
            cnt = int(str(res.getOutput(0)))
        except Exception as e:
            logging.debug(f"[UTIL] GetCount failed for {p}: {e}")
            cnt = -1
        logging.debug(f"[UTIL] candidate {p} count={cnt}")
        if cnt > best_count:
            best_count = cnt
            best = p

    if best_count > 0:
        return best
    return None


def best_layer_in_gpkg(gpkg_path: Path) -> Optional[str]:
    """Return the layer name inside a GPKG with the highest feature count, or None.

    Uses ArcPy to list feature classes inside the geopackage and selects the
    layer with the largest feature count.
    """
    try:
        import arcpy
        import os
    except Exception:
        logging.debug("[UTIL] arcpy not available; cannot inspect GPKG")
        return None
    best: Optional[str] = None
    best_count = -1
    # Use da.Walk to enumerate feature classes inside the geopackage without
    # changing the global arcpy.env.workspace.
    try:
        for dirpath, dirnames, filenames in arcpy.da.Walk(str(gpkg_path), datatype="FeatureClass"):
            for fc in filenames:
                # Build a workspace-resolved path for counting
                candidate = os.path.join(dirpath, fc)
                try:
                    res = arcpy.management.GetCount(candidate)
                    cnt = int(str(res.getOutput(0)))
                except Exception as e:
                    logging.debug(f"[UTIL] GetCount failed for {candidate}: {e}")
                    cnt = -1
                logging.debug(f"[UTIL] gpkg candidate {candidate} count={cnt}")
                if cnt > best_count:
                    best_count = cnt
                    best = fc
    except Exception as e:
        logging.debug(f"[UTIL] da.Walk failed for {gpkg_path}: {e}")

    if best_count > 0:
        return best
    return None


def get_logger() -> logging.Logger:
    log = logging.getLogger("op-etl")
    if log.handlers:
        return log
    log.setLevel(logging.INFO)
    ch = logging.StreamHandler(sys.stdout)
    fh = logging.FileHandler("op-etl.log", encoding="utf-8")
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    ch.setFormatter(fmt)
    fh.setFormatter(fmt)
    log.addHandler(ch)
    log.addHandler(fh)
    return log


def log_http_request(log: logging.Logger, session, method: str, url: str, **kwargs):
    log.info("[HTTP] start method=%s url=%s", method, url)
    response = session.request(method, url, **kwargs)
    log.info("[HTTP] done  method=%s status=%d url=%s", method, response.status_code, url)
    return response
