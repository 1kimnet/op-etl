from pathlib import Path
import arcpy

def ensure_workspaces(cfg):
    d = Path(cfg["workspaces"]["downloads"])
    d.mkdir(parents=True, exist_ok=True)

    gdb = cfg["workspaces"]["staging_gdb"]
    if not arcpy.Exists(gdb):
        parent = Path(gdb).parent
        parent.mkdir(parents=True, exist_ok=True)
        arcpy.management.CreateFileGDB(str(parent), Path(gdb).name)

def staging_path(cfg, name):
    return f'{cfg["workspaces"]["staging_gdb"]}/{name}'
