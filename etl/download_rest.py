# etl/download_rest.py
import logging
from arcgis.features import FeatureLayer, FeatureSet
from arcgis.geometry import Geometry
from .paths import staging_path

def _query_to_fgdb(fl, where, geom, out_fc):
    try:
        # Use a more robust pagination approach
        features = []
        start = 0
        page_size = 1000  # Adjust page size as needed
        while True:
            fs = fl.query(
                where=where or "1=1",
                geometry_filter=geom,
                result_offset=start,
                result_record_count=page_size,
                out_sr=fl.properties.extent['spatialReference']
            )
            if not fs or not fs.features:
                break
            features.extend(fs.features)
            if len(fs.features) < page_size:
                break
            start += len(fs.features)

        if not features:
            logging.warning(f"[REST] No features found for {fl.url} with where clause: {where}")
            return 0

        # Create a new feature set from the collected features
        feature_set = FeatureSet(
            features,
            geometry_type=fl.properties.geometryType,
            spatial_reference=fl.properties.extent['spatialReference']
        )

        # Save to FGDB
        save_location, out_name = out_fc.rsplit("/", 1)
        feature_set.save(save_location=save_location, out_name=out_name)
        return len(features)
    except Exception as e:
        logging.error(f"[REST] Failed to process layer {fl.url}: {e}", exc_info=True)
        return 0

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
            logging.info(f"[REST] {url} -> {out_fc} features={count}")
