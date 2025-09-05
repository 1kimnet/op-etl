from pathlib import Path
import re
import unicodedata

def _make_arcpy_safe_name(name: str, max_length: int = 100) -> str:
    """Create ArcPy-safe feature class names that always work."""
    if not name:
        return "unnamed_fc"

    normalized = unicodedata.normalize('NFD', name)
    ascii_name = ''.join(c for c in normalized if unicodedata.category(c) != 'Mn')

    try:
        ascii_name = ascii_name.encode('ascii', 'ignore').decode('ascii')
    except Exception:
        ascii_name = "converted_name"

    clean = ascii_name.lower().strip()
    clean = re.sub(r'[^a-z0-9]', '_', clean)
    clean = re.sub(r'_+', '_', clean)
    clean = clean.strip('_')

    if clean and clean[0].isdigit():
        clean = f"fc_{clean}"

    if not clean or len(clean) < 1:
        clean = "default_fc"

    clean = clean[:max_length]

    reserved = {
        'con', 'prn', 'aux', 'nul', 'com1', 'com2', 'com3', 'com4',
        'com5', 'com6', 'com7', 'com8', 'com9', 'lpt1', 'lpt2',
        'lpt3', 'lpt4', 'lpt5', 'lpt6', 'lpt7', 'lpt8', 'lpt9'
    }
    if clean.lower() in reserved:
        clean = f"{clean}_data"

    return clean

def ensure_workspaces(cfg: dict) -> None:
    ws = cfg["workspaces"]

    downloads = Path(ws["downloads"]).resolve()
    downloads.mkdir(parents=True, exist_ok=True)

    gdb_path = Path(ws["staging_gdb"]).resolve()
    gdb_parent = gdb_path.parent
    gdb_parent.mkdir(parents=True, exist_ok=True)

    try:
        import arcpy
        if not arcpy.Exists(str(gdb_path)):
            arcpy.management.CreateFileGDB(str(gdb_parent), gdb_path.name)
    except Exception:
        pass

def staging_path(cfg: dict, name: str) -> Path:
    """Return canonical FGDB path with ArcPy-safe feature class name."""
    safe_name = _make_arcpy_safe_name(name)
    gdb_path = Path(cfg['workspaces']['staging_gdb'])
    return gdb_path / safe_name