# Configuration Files

This directory contains the configuration for the OP‑ETL pipeline.

## Active Configuration (current)

- `config.yaml` — Global settings (workspaces, geoprocess, logging, bbox, flags)
- `sources.yaml` — Source definitions list

Notes:
-
- If `sources.yaml` is missing, the loader falls back to `sources_backup.yaml` or `legacy/sources.yaml`.
- Environment overrides are supported: `OPETL_CONFIG`, `OPETL_SOURCES`.

## Legacy/Reference

- `legacy/config.yaml` and `legacy/sources.yaml` — historical examples kept for reference.

## Schema Overview

`config.yaml` (key sections):

```yaml
workspaces:
	downloads: ./data/downloads
	staging_gdb: ./data/staging.gdb
	sde_conn: ./data/connections/prod.sde

geoprocess:
	enabled: true
	aoi_boundary: ./data/connections/municipality_boundary.shp
	target_wkid: 3010   # or target_srid

# Optional bbox filter
use_bbox_filter: true
global_bbox:
	coords: [585826, 6550189, 648593, 6611661]
	crs: 3006  # 3006 | 4326 | "EPSG:3006" | "CRS84"

# Unified downloader, cleanup flags, logging
use_unified_downloader: true
cleanup_downloads_before_run: false
cleanup_staging_before_run: false
logging:
	level: INFO
	file:
		enabled: true
		name: etl.log
		level: DEBUG
```

`sources.yaml`:

```yaml
sources:
	- name: nvdb_vag
		authority: NVDB
		type: rest        # aliases: rest|rest_api
		url: https://.../ArcGIS/rest/services/Vag/MapServer
		raw:
			include: ["*Väg*"]
			out_fields: "*"

	- name: sgu_erosion
		authority: SGU
		type: ogc         # aliases: ogc|ogc_api
		url: https://.../features/v1/
		raw:
			collections: ["aktiv-erosion"]

	- name: open_data_zip
		authority: FM
		type: http        # aliases: http|file
		url: https://.../rikstackande-geodata.zip
```

Type aliases recognized by the loader: `http|file`, `rest|rest_api`, `ogc|ogc_api`, `atom|atom_feed`, `wfs`.

### Geometry Policy (OGC/WFS)

- Explicit geometry in sources is optional. Staging uses robust GeoJSON handling with dominant‑geometry filtering and magnitude checks.
- Mixed-geometry datasets are reduced to the dominant geometry in staging. Provide separate sources if strict separation is required.

For more details, see `docs/spatial-reference-consistency.md` and the top-level `README.md`.