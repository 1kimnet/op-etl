# etl/process.py
import logging
import arcpy

def run(cfg):
    gp = cfg.get("geoprocess", {})
    if not gp.get("enabled"):
        return

    aoi = gp.get("aoi")
    target_wkid = gp.get("target_wkid") or gp.get("target_srid")

    for s in cfg.get("sources", []):
        if not s.get("include", True):
            continue

        in_fc = f"{cfg['workspaces']['staging_gdb']}/{s['out_name']}"

        if not arcpy.Exists(in_fc):
            logging.info(f"[PROCESS] {in_fc} skipped (missing)")
            continue

        did_work = False
        tmp = in_fc + "_proc"
        src = in_fc

        try:
            if aoi and arcpy.Exists(aoi):
                arcpy.analysis.Clip(src, aoi, tmp)
                src = tmp
                did_work = True

            if target_wkid:
                out = in_fc + "_proj"
                arcpy.management.Project(src, out, arcpy.SpatialReference(target_wkid))
                if arcpy.Exists(in_fc):
                    arcpy.management.Delete(in_fc)
                arcpy.management.Rename(out, in_fc)
                did_work = True
        finally:
            if src == tmp and arcpy.Exists(tmp):
                arcpy.management.Delete(tmp)

        if did_work:
            logging.info(f"[PROCESS] {in_fc} processed")
        else:
            logging.info(f"[PROCESS] {in_fc} skipped (no-op)")
