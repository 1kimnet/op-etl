# etl/load_sde.py
import logging
import arcpy

def run(cfg):
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
