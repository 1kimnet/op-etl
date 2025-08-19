"""Small ETL utilities used by staging and other modules.

Keep these helpers minimal: choose best candidate by feature count.
"""
from pathlib import Path
import logging
import sys
from typing import List, Optional


def best_shapefile_by_count(paths: List[Path]) -> Optional[Path]:
    """
    Select the Path whose dataset has the largest feature count (must be > 0), or return None.
    
    Parameters:
        paths (List[Path]): Iterable of filesystem Paths pointing to candidate feature datasets
            (e.g., shapefiles). Each path will be counted using ArcPy's GetCount.
    
    Returns:
        Optional[Path]: The Path with the highest feature count when that count is greater than zero;
        otherwise None.
    
    Notes:
        - ArcPy is imported inside the function; if ArcPy cannot be imported the function returns None.
        - Individual GetCount failures for a path are treated as a non-candidate (count <= 0).
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
    """
    Return the name of the feature layer inside a GeoPackage that has the largest feature count, or None.
    
    Attempts to import ArcPy and uses arcpy.da.Walk to enumerate feature classes inside the given GeoPackage path, then calls arcpy.management.GetCount for each candidate to determine feature counts. If ArcPy cannot be imported, if enumeration fails, or if no feature class has a count > 0, the function returns None. On tie, the first encountered layer with the highest count is returned.
    
    Parameters:
        gpkg_path (Path): Path to the .gpkg file to inspect.
    
    Returns:
        Optional[str]: The feature class (layer) name with the greatest feature count, or None if unavailable.
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
    """
    Get a configured logger named "op-etl".
    
    Returns a logger instance named "op-etl". If the logger already has handlers it is returned unchanged; otherwise the function:
    - sets the level to INFO,
    - attaches a StreamHandler writing to stdout and a FileHandler writing to "op-etl.log" (UTF-8),
    - applies a formatter that includes timestamp, level, and message.
    
    Side effects:
    - May create or open the file "op-etl.log" in the current working directory.
    """
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
    """
    Send an HTTP request via the provided session while logging the start and completion.
    
    Parameters:
        method (str): HTTP method (e.g., 'GET', 'POST').
        url (str): Request URL.
    
    Returns:
        The response object returned by session.request (type depends on the session implementation).
    
    Notes:
        Logs an INFO-level "start" entry before calling session.request and an INFO-level "done" entry after completion.
    """
    log.info("[HTTP] start method=%s url=%s", method, url)
    response = session.request(method, url, **kwargs)
    log.info("[HTTP] done  method=%s status=%d url=%s", method, response.status_code, url)
    return response
