# etl/download_rest.py
import arcpy
from arcgis.features import FeatureLayer
from arcgis.geometry import Geometry
from .paths import staging_path

def _query_to_fgdb(fl, where, geom, out_fc):
    # Pull features to a temp memory layer then copy to FGDB to avoid server timeouts on large writes
    fs = fl.query(where=where or "1=1", geometry_filter=geom, return_all_records=True, out_sr=fl.properties.extent['spatialReference'])
    if fs is None or fs.sdf is None or len(fs) == 0:
        return 0
    # Save to FGDB
    fs.save(out_path="/", out_name="in_memory_fc")   # save to memory workspace
    arcpy.conversion.FeatureClassToFeatureClass("in_memory/in_memory_fc", out_path=out_fc.rsplit("/",1)[0], out_name=out_fc.rsplit("/",1)[1])
    return len(fs)

def run(cfg):
    for s in cfg.get("sources", []):
        if not s.get("include", True) or s.get("type") != "rest":
            continue

        urls = []
        if s.get("layer_ids"):
            base = s["url"].rstrip("/")
            urls = [f"{base}/{lid}" for lid in s["layer_ids"]]
        else:
            urls = [s["url"]]

        for url in urls:
            fl = FeatureLayer(url)
            where = s.get("where", "1=1")
            geom = None
            if "bbox" in s and s["bbox"]:
                xmin, ymin, xmax, ymax, wkid = s["bbox"]
                geom = Geometry({"xmin":xmin,"ymin":ymin,"xmax":xmax,"ymax":ymax,"spatialReference":{"wkid":wkid}})
            out_fc = staging_path(cfg, s.get("out_name") or fl.properties.name)
            count = _query_to_fgdb(fl, where, geom, out_fc)
            print(f"[REST] {url} -> {out_fc} features={count}")
