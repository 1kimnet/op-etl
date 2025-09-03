# Copilot Instructions for OP‑ETL

These instructions orient AI coding agents working in this repository. Keep solutions minimal and aligned with the ongoing simplification initiative (reduce complexity and total LOC; target core runtime < ~1000 LOC). Favor clarity over cleverness.

## Architecture At‑A‑Glance
- Pipeline phases (modules in `etl/`):
  - `download_*`: Fetch raw data for REST/OGC/HTTP/ATOM/WFS. Prefer simple stdlib (`urllib`) with light retries.
  - `stage_files.py`: Single entry `stage_all_downloads(cfg)` discovers files in `data/downloads/<AUTH>` and imports into `data/staging.gdb`. Enforces SR consistency, robust GeoJSON handling with dominant-geometry filtering.
  - `process.py`: Clip to AOI only; project to `processing.target_wkid` (default 3010); writes `data/staging/processed_feature_classes.json` using relative FC paths.
  - `load_sde.py`: Truncate-and-load to SDE, filtering strictly by the processed list if the JSON exists.
- Orchestration: `run.py` wires logging and executes download → stage → process → load.
- Config: `etl/config.py` merges `config/config.yaml` with `config/sources.yaml` (fallback to `config/sources_backup.yaml` or legacy). Keys: `workspaces.downloads`, `workspaces.staging_gdb`, `workspaces.sde_conn`, `processing.*`, `sources[]`.

## Conventions & Patterns
- Logging: Use `etl.logging.setup_pipeline_logging`; avoid `basicConfig`. Tag messages with phases: `[HTTP]`, `[STAGE]`, `[PROCESS]`, `[LOAD]`.
- Spatial references:
  - REST/Esri JSON: prefer EPSG:3006 where supported.
  - OGC API Features: default CRS84 (EPSG:4326); omit `bbox-crs` unless 3006 is known supported.
  - Staging defines/project to EPSG:3006; processing targets EPSG:3010 by default.
- Staging names: Use `create_safe_name()`; ensure a single authority prefix and sanitize Swedish characters.
- Processed list filtering: Compare by relative `dataset/feature` names from `arcpy.da.Walk(dirpath, name)`; gate by file existence, not list truthiness.
- GeoJSON robustness: detect dominant geometry, filter mixed types, validate coordinate magnitudes, then import; fallback forces explicit geometry type.

## Developer Workflows
- Full pipeline:
  - `python run.py --config config/config.yaml --sources config/sources.yaml`
- Baseline capture:
  - `python tests/baseline/capture_baseline.py --config config/config.yaml --sources config/sources.yaml --max-sources 5`
  - Use `--max-sources 9999` to run all. Outputs `tests/baseline/baseline_report.md` and `logs/baseline_tests.log`.
- Data locations: downloads under `data/downloads/<AUTH>`; staging in `data/staging.gdb`.
- ArcPy: import lazily inside functions; prefer `arcpy.da.Walk` rather than mutating `arcpy.env.workspace`.

## Guardrails (Project‑Specific)
- Keep edits surgical; don’t reformat or rename broadly. No new heavy deps without clear need.
- Respect current OGC/REST SR rules and AOI semantics (no processed JSON when AOI disabled).
- Aim to shrink code paths; prefer composing existing helpers over new utilities.

## Key Files
- `run.py`, `etl/stage_files.py`, `etl/process.py`, `etl/load_sde.py`, `etl/config.py`, and `docs/improvements.md`.

## Codacy MCP Obligations
- This repo uses Codacy MCP rules. After any file edit, run the MCP tool `codacy_cli_analyze` for each edited file. For dependency changes, also run with `tool=trivy`. See details in `./.github/instructions/codacy.instructions.md`.