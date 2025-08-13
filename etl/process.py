# etl/process.py
import arcpy

def run(cfg):
    if not cfg.get("geoprocess", {}).get("enabled"):
        return
    aoi = cfg["geoprocess"].get("aoi")
    target_wkid = cfg["geoprocess"].get("target_wkid")

    for s in cfg.get("sources", []):
        if not s.get("include", True):
            continue
        in_fc = f'{cfg["workspaces"]["staging_gdb"]}/{s["out_name"]}'
        tmp = in_fc + "_proc"
        src = in_fc

        if aoi and arcpy.Exists(aoi):
            arcpy.analysis.Clip(src, aoi, tmp)
            src = tmp

        if target_wkid:
            out = in_fc + "_proj"
            arcpy.management.Project(src, out, arcpy.SpatialReference(target_wkid))
            arcpy.management.Delete(in_fc)
            arcpy.management.Rename(out, in_fc)

        if src == tmp:
            arcpy.management.Delete(tmp)

        print(f"[PROCESS] {in_fc} processed")
