# OP-ETL - Geospatial Data ETL Pipeline

## Overview
OP-ETL is a lightweight ETL pipeline for geospatial data in Esri environments. It downloads open data from multiple sources, stages it in a File Geodatabase (FGDB) for use with ArcPy tools, and loads it into a production ArcSDE geodatabase.

## Features

- **Single config file** (`config.yaml`) — all sources, workspaces, and settings in one place
- **Multiple source types:**
  - HTTP downloads (ZIP archives of Shapefiles or FGDBs)
  - ArcGIS REST services with **parallel OID-batch downloading** for large datasets
  - OGC API Features (planned)
- **Native Esri format staging** — FileGDB staging ensures smooth ArcPy processing
- **Optional geoprocessing** — clip to AOI, reproject to target SRID
- **Simple SDE load** — truncate-and-load workflow to SQL Server (or other supported RDBMS)
- **Spatial Reference Consistency** — enforced SR handling with SWEREF99 TM (EPSG:3006) target
- **High-performance downloads** — 3-10× faster REST downloads via parallel OID batching

## Basic Workflow

```mermaid
flowchart LR
    A[Download Sources] --> B[Stage in FGDB]
    B --> C[Optional Geoprocessing]
    C --> D[Load to SDE]

    A --> E[SR Validation]
    E --> B
    B --> F[Project to SWEREF99 TM]
    F --> C
```

1. **Download**: Fetch data from HTTP, REST, or OGC sources with SR consistency
2. **Validate**: Check coordinate magnitudes and spatial reference integrity
3. **Stage**: Store datasets in FileGDB with proper SR (EPSG:3006)
4. **Process** (optional): Clip to AOI and/or reproject
5. **Load**: Push processed data into ArcSDE

## Spatial Reference Handling

The pipeline enforces consistent spatial reference handling:

- **REST APIs**: Use SWEREF99 TM (EPSG:3006) for bbox, inSR, and outSR
- **OGC APIs**: Default to CRS84, support EPSG:3006 when available
- **Staging**: All feature classes have defined SR, project to SWEREF99 TM
- **Validation**: Coordinate magnitude checks, no "Unknown" SR allowed

See [Spatial Reference Consistency Documentation](docs/spatial-reference-consistency.md) for details.

## Requirements

- Python 3.11 (ArcGIS Pro environment)
- ArcPy (bundled with ArcGIS Pro)
- PyYAML for config parsing

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/1kimnet/op-etl.git
   cd op-etl
   ```

1. Install dependencies:

  ```bash
  pip install -r requirements.txt
  ```

1. Create or edit `config.yaml` to define:

   ```yaml
   workspaces:
     downloads: ./_downloads
     staging_gdb: ./staging.gdb
     sde_conn: C:/path/to/your.sde

   geoprocess:
     enabled: true
     aoi: C:/path/to/aoi_fc
     target_srid: 3006

   sources:
     - name: nvdb_vag
       type: rest
       url: https://services.example.com/ArcGIS/rest/services/Vag/MapServer
       layer_ids: [0, 1, 2]
       include: true
       raw:
         use_oid_sweep: true    # Enable parallel downloading for large datasets
         page_size: 1000        # Batch size (default: 1000)
         max_workers: 6         # Concurrent threads (default: 6)
   ```

   For large REST layers (50k+ features), enable parallel OID-batch downloading for 3-10× performance improvement. See [OID-Batch Parallelism Documentation](docs/oid-batch-parallelism.md) for details.

## Usage

Run the pipeline from ArcGIS Pro's Python environment:

```bash
python run.py
```

On Windows `cmd.exe`, using ArcGIS Pro's conda Python explicitly (adjust path/env name as needed):

```cmd
"%LOCALAPPDATA%\ESRI\conda\envs\arcgispro-py3\python.exe" run.py --download --process --load_sde
```

Run specific steps:

```cmd
REM Only download
python run.py --download

REM Only process
python run.py --process

REM Only load to SDE
python run.py --load_sde

REM Cleanup staging/downloads (when enabled)
python run.py --cleanup
```

> Note: If no flags are provided, all steps will run in sequence.

### Unified Downloader (optional)

You can enable a single, unified downloads pass that dispatches to existing downloaders per source type. Add this to `config/config.yaml`:

```yaml
use_unified_downloader: true
```

It honors `--authority` and `--type` filters. Example:

```cmd
"%LOCALAPPDATA%\ESRI\conda\envs\arcgispro-py3\python.exe" run.py --download --authority RAA --type ogc
```

### Logging

- Console shows `INFO` and above by default (configured via `logging.console_level`).
- Summary log (`logs/etl.log`) follows `logging.level` (default `WARNING`). It may be empty if no warnings/errors occur.
- Debug log (`logs/etl.debug.log`) captures detailed output when `logging.debug_file` is set.

To increase verbosity and include `INFO` in the summary file, edit `config/config.yaml`:

```yaml
logging:
  level: "INFO"          # Root level (controls summary file)
  console_level: "INFO"  # Console override
  summary_file: "logs/etl.log"
  debug_file: "logs/etl.debug.log"
```

If you see no immediate output, ensure you are using the ArcGIS Pro Python interpreter. ArcPy is lazily imported so logging initializes before heavy modules load.

To avoid emoji/encoding issues in Windows `cmd.exe`, console logs auto-fallback to ASCII when UTF-8 isn’t detected. You can force ASCII via:

```cmd
set OP_ETL_ASCII_CONSOLE=1
```

## Roadmap

- [ ] Add OGC API Features support
- [ ] Add more geoprocessing tools
- [ ] Add simple logging to CSV/JSON
- [ ] Add unit tests for handlers
