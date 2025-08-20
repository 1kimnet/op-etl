"""Small ETL utilities used by staging and other modules.

Keep these helpers minimal: choose best candidate by feature count.
"""
from pathlib import Path
import logging
import sys
import re
import unicodedata
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

    return best if best_count > 0 else None

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


def make_arcpy_safe_name(name: str, max_length: int = 60) -> str:
    """Create ArcPy-safe feature class names that always work.

    Handles Swedish characters, Unicode normalization, and ArcPy naming restrictions.

    Args:
        name: Input name that may contain special characters
        max_length: Maximum length for the output name

    Returns:
        Clean name safe for use as ArcPy feature class name
    """
    if not name:
        return "unnamed_fc"

    # Normalize unicode and remove all accents
    normalized = unicodedata.normalize('NFD', name)
    ascii_name = ''.join(c for c in normalized if unicodedata.category(c) != 'Mn')

    # Convert to ASCII, handling any remaining issues
    try:
        ascii_name = ascii_name.encode('ascii', 'ignore').decode('ascii')
    except Exception:
        ascii_name = "converted_name"

    # Clean for ArcPy rules
    clean = ascii_name.lower().strip()

    # Replace any non-alphanumeric with underscore
    clean = re.sub(r'[^a-z0-9]', '_', clean)

    # Remove multiple underscores
    clean = re.sub(r'_+', '_', clean)

    # Remove leading/trailing underscores
    clean = clean.strip('_')

    # Ensure it starts with a letter (ArcPy requirement)
    if clean and clean[0].isdigit():
        clean = f"fc_{clean}"

    # Handle empty/invalid results
    if not clean or len(clean) < 1:
        clean = "default_fc"

    # Truncate to max length
    clean = clean[:max_length]

    # Handle reserved words (Windows/ArcPy conflicts)
    reserved = {
        'con', 'prn', 'aux', 'nul', 'com1', 'com2', 'com3', 'com4',
        'com5', 'com6', 'com7', 'com8', 'com9', 'lpt1', 'lpt2',
        'lpt3', 'lpt4', 'lpt5', 'lpt6', 'lpt7', 'lpt8', 'lpt9'
    }
    if clean.lower() in reserved:
        clean = f"{clean}_data"

    return clean


def safe_fc_path(gdb_path: str, fc_name: str) -> str:
    """Create safe full path for feature class.

    Args:
        gdb_path: Path to the geodatabase
        fc_name: Feature class name (will be made safe)

    Returns:
        Full path safe for ArcPy operations
    """
    safe_name = make_arcpy_safe_name(fc_name)
    # Use forward slashes for ArcPy compatibility
    return f"{gdb_path.replace(chr(92), '/')}/{safe_name}"
