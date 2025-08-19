# etl/load_sde.py
import logging
import arcpy

def run(cfg):
    """
    Load feature classes from a staging geodatabase into an SDE connection.
    
    Processes each source listed in cfg["sources"] (a list of dicts with at least "out_name" and optional "include" boolean). For each included source this function:
    - Builds src as "{staging_gdb}/{out_name}" and dest as "{sde_conn}/{out_name}".
    - Skips the source if the src does not exist in the staging GDB (logs a warning).
    - If dest does not exist in SDE, creates an empty feature class in SDE using the source as a template and preserving geometry type and spatial reference.
    - If dest exists, truncates it.
    - Appends all features from src to dest with schema_type="NO_TEST" and logs an info message.
    
    Parameters:
        cfg (dict): Configuration mapping. Required keys:
            - "workspaces": dict containing "sde_conn" (SDE connection path) and "staging_gdb" (staging geodatabase path).
            - "sources": optional list of source dicts; each source must include "out_name" (feature class name) and may include "include" (bool).
    
    Returns:
        None
    """
    sde = cfg["workspaces"]["sde_conn"]
    for s in cfg.get("sources", []):
        if not s.get("include", True):
            continue
        src = f'{cfg["workspaces"]["staging_gdb"]}/{s["out_name"]}'
        dest = f'{sde}/{s["out_name"]}'   # keep same name for now
        # If the source in the staging GDB doesn't exist, skip gracefully.
        if not arcpy.Exists(src):
            logging.warning(f"[LOAD] Source missing in staging, skipping: {src}")
            continue
        if not arcpy.Exists(dest):
            # first time: create empty dest by copying schema
            arcpy.management.CreateFeatureclass(sde, s["out_name"], geometry_type=arcpy.Describe(src).shapeType, template=src, spatial_reference=arcpy.Describe(src).spatialReference)
        else:
            arcpy.management.TruncateTable(dest)

        arcpy.management.Append(inputs=src, target=dest, schema_type="NO_TEST")
        logging.info(f"[LOAD] {src} -> {dest}")
