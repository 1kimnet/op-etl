# OP-ETL - Geospatial Data ETL Pipeline

## Overview
OP-ETL is a lightweight ETL pipeline for geospatial data in Esri environments. It downloads open data from multiple sources, stages it in a File Geodatabase (FGDB) for use with ArcPy tools, and loads it into a production ArcSDE geodatabase.

## Features

- **Single config file** (`config.yaml`) — all sources, workspaces, and settings in one place
- **Multiple source types:**
  - HTTP downloads (ZIP archives of Shapefiles or FGDBs)
  - ArcGIS REST services
  - OGC API Features (planned)
- **Native Esri format staging** — FileGDB staging ensures smooth ArcPy processing
- **Optional geoprocessing** — clip to AOI, reproject to target SRID
- **Simple SDE load** — truncate-and-load workflow to SQL Server (or other supported RDBMS)

## Basic Workflow

```mermaid
flowchart LR
    A[Download Sources] --> B[Stage in FGDB]
    B --> C[Optional Geoprocessing]
    C --> D[Load to SDE]
```

1. **Download**: Fetch data from HTTP, REST, or OGC sources
2. **Stage**: Store datasets in a local FileGDB
3. **Process** (optional): Clip to AOI and/or reproject
4. **Load**: Push processed data into ArcSDE

## Requirements

- Python 3.11 (ArcGIS Pro environment)
- ArcPy (bundled with ArcGIS Pro)
- PyYAML for config parsing

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/<your-org>/atlaspipe.git
   cd atlaspipe
   ```

2. Create or edit `config.yaml` to define:
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
   ```

## Usage

Run the pipeline from ArcGIS Pro's Python environment:

```bash
python run.py
```

Or run specific steps:

```bash
python run.py --download
python run.py --process
python run.py --load_sde
python run.py --cleanup
```

> **Note**: If no flags are provided, all steps will run in sequence.

## Roadmap

- [ ] Add OGC API Features support
- [ ] Add more geoprocessing tools
- [ ] Add simple logging to CSV/JSON
- [ ] Add unit tests for handlers
