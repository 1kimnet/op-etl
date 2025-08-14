from pathlib import Path
import arcpy


def ensure_workspaces(cfg: dict) -> None:
    ws = cfg["workspaces"]

    downloads = Path(ws["downloads"]).resolve()
    downloads.mkdir(parents=True, exist_ok=True)

    gdb_path = Path(ws["staging_gdb"]).resolve()
    gdb_parent = gdb_path.parent
    gdb_parent.mkdir(parents=True, exist_ok=True)

    # Create FGDB if missing
    if not arcpy.Exists(str(gdb_path)):
        arcpy.management.CreateFileGDB(str(gdb_parent), gdb_path.name)


def staging_path(cfg: dict, name: str) -> str:
    # Return canonical FGDB path forward-slash style for ArcPy
    return f"{cfg['workspaces']['staging_gdb'].replace(chr(92)*2,'/').replace(chr(92),'/')}/{name}"
