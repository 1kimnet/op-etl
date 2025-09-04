# Copilot Instructions for OP‑ETL

These instructions orient AI coding agents in this repo. Optimize for simplicity and small diffs; prefer composing existing helpers over new utilities.

## Architecture At‑A‑Glance
- Pipeline (modules in `etl/`):
  - `download_*`: Fetch raw data for REST/OGC/HTTP/ATOM/WFS. Keep HTTP simple (stdlib) with light retries.
  - `stage_files.py`: `stage_all_downloads(cfg)` discovers `data/downloads/<AUTH>` and imports into `data/staging.gdb` with SR consistency and robust GeoJSON handling (dominant‑geometry filtering + magnitude checks).
  - `process.py`: Clip to AOI and optionally reproject; writes `data/processed_feature_classes.json` only when AOI is provided.
  - `load_sde.py`: Truncate‑and‑load to SDE, strictly filtered by the processed list if the JSON file exists.
- Orchestration: `run.py` wires unified logging and executes download → stage → process → load.
- Config load: `etl/config.py` merges `config/config.yaml` with `config/sources.yaml` (fallback to `config/sources_backup.yaml` or legacy). Required keys: `workspaces.downloads`, `workspaces.staging_gdb`, `workspaces.sde_conn`; sources normalized via type‑specific defaults.

## Config Essentials (repo‑specific)
- Section names: use `geoprocess` (legacy `geoprocessing` auto‑merged).
- `geoprocess.enabled` (bool), `geoprocess.aoi_boundary` (FC path), `geoprocess.target_wkid` or `geoprocess.target_srid` (e.g., 3010).
- `sources[].type` accepted aliases: `http|file`, `rest|rest_api`, `ogc|ogc_api`, `atom|atom_feed`, `wfs`.
- Global bbox (optional): `use_bbox_filter: true` and `global_bbox: { coords: [xmin,ymin,xmax,ymax], crs: 3006|4326|"EPSG:3006"|"CRS84" }`.

## Conventions & Patterns
- Logging: call `etl.logging.setup_pipeline_logging`; avoid `basicConfig`. Tag messages with `[REST]`, `[STAGE]`, `[PROCESS]`, `[LOAD]`. For Windows `cmd.exe` ASCII fallback set `OP_ETL_ASCII_CONSOLE=1`.
- ArcPy: import lazily inside functions; enumerate with `arcpy.da.Walk` (don’t mutate `arcpy.env.workspace`).
- Spatial refs:
  - Staging defines/projects to EPSG:3006 (SWEREF99 TM).
  - Processing reprojects to `geoprocess.target_wkid` (commonly EPSG:3010) when set.
  - REST requests: prefer EPSG:3006 for in/out SR (Esri JSON). GeoJSON uses 4326.
  - OGC API Features: default CRS84 (EPSG:4326); omit `bbox-crs` unless 3006 is confirmed.
- Staging names: `create_safe_name()` ensures a single authority prefix and Swedish character sanitization. Example: `raa_raa_ri_kulturmiljovard_mb3kap6` → `raa_ri_kulturmiljovard_mb3kap6`.
- Processed gating: `load_sde` compares relative `dataset/feature` names from `arcpy.da.Walk`; gating depends on file existence, not list truthiness.

## Developer Workflows
- Full pipeline (Windows cmd.exe):
  - `python run.py --config config/config.yaml --sources config/sources.yaml`
  - Or explicitly: `"%LOCALAPPDATA%\ESRI\conda\envs\arcgispro-py3\python.exe" run.py`
- Step filters and unified downloader:
  - Flags: `--download`, `--process`, `--load_sde`, `--authority RAA`, `--type ogc`.
  - Enable unified downloader via `config/config.yaml`: `use_unified_downloader: true`.
- Cleanup controls: `cleanup_downloads_before_run` and `cleanup_staging_before_run`.
- Baseline capture: `python tests/baseline/capture_baseline.py --config config/config.yaml --sources config/sources.yaml --max-sources 5` → writes `tests/baseline/baseline_report.md` and `logs/baseline_tests.log`.
- Data locations: downloads in `data/downloads/<AUTH>`; staging GDB at `data/staging.gdb`.

## Guardrails
- Keep edits surgical; do not reformat/rename broadly. No heavy new deps.
- Respect SR rules and AOI semantics (no processed list when AOI is disabled).
- Prefer extending `download_*`, `stage_files`, `process`, `load_sde` rather than adding new helpers.

## Key Files
- `run.py`, `etl/stage_files.py`, `etl/process.py`, `etl/load_sde.py`, `etl/config.py`, `etl/download_*.py`, and `docs/spatial-reference-consistency.md`.

## Codacy MCP Obligations
- After any file edit, run Codacy MCP `codacy_cli_analyze` for each edited file. For dependency changes, also run with `tool=trivy` (see `./.github/instructions/codacy.instructions.md`). If Codacy MCP tools aren’t available, note this in your response and suggest the troubleshooting steps listed there.