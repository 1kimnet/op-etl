# Spatial Reference (SR) Consistency Implementation

This document describes the implemented spatial reference consistency features in the OP-ETL pipeline.

## Overview

The SR consistency implementation enforces proper spatial reference handling throughout the ETL pipeline to prevent common issues like:
- Fetching wrong features due to incorrect bbox SR
- Writing wrong numbers into the right SR
- Writing correct numbers into an Unknown SR
- Mixing degrees and meters in coordinate systems

## Key Components

### 1. SR Utilities Module (`etl/sr_utils.py`)

Provides core functions for SR validation and consistency checking:

- **`validate_coordinates_magnitude()`**: Checks if coordinates are reasonable for expected SR
- **`validate_sr_consistency()`**: Validates SR consistency in response data
- **`validate_bbox_vs_envelope()`**: Ensures response envelope matches requested bbox
- **`get_sr_config_for_source()`**: Returns appropriate SR configuration per source type
- **`log_sr_validation_summary()`**: Logs validation results

### 2. Enhanced Download Modules

#### REST API (`etl/download_rest.py`)
- **Enforces SWEREF99 TM (EPSG:3006)** for bbox_sr, inSR, and outSR
- Validates SR consistency in responses
- Checks `exceededTransferLimit` for proper pagination
- Logs SR configuration for transparency

#### OGC API (`etl/download_ogc.py`)
- **Defaults to CRS84** for maximum compatibility
- **Supports EPSG:3006** when explicitly configured via `supports_epsg_3006: true`
- Maintains CRS parameters across pagination
- Validates SR consistency in GeoJSON responses

### 3. Enhanced Staging (`etl/stage_files.py`)
- **Detects SR from imported files** (GeoJSON, Shapefile, GPKG)
- **Defines projection** for files with unknown SR
- **Projects to SWEREF99 TM** as target SR for consistency
- Validates coordinate magnitudes during import

### 4. Configuration Patterns

#### REST API Configuration

**EsriJSON Format (Default)**
```yaml
- name: Example REST Source (EsriJSON)
  type: rest
  url: https://example.com/rest/services/Data/MapServer
  raw:
    response_format: esrijson   # Request f=json, set outSR=3006, expect meters
    bbox_sr: 3006
    in_sr: 3006
    out_sr: 3006
    layer_ids: [0, 1]
    # Results staged in EPSG:3006, no projection needed
```

**GeoJSON Format (for servers that ignore outSR)**
```yaml
- name: Example REST Source (GeoJSON)
  type: rest  
  url: https://example.com/rest/services/Data/MapServer
  raw:
    response_format: geojson    # Request f=geojson, no outSR, expect degrees
    layer_ids: [0, 1]
    # Results staged in EPSG:4326, projected to EPSG:3006
```

**Backward Compatible (defaults to esrijson)**
```yaml
- name: Example REST Source (Legacy)
  type: rest
  url: https://example.com/rest/services/Data/MapServer
  raw:
    # No response_format specified = esrijson (backward compatible)
    bbox_sr: 3006
    in_sr: 3006
    out_sr: 3006
    layer_ids: [0, 1]
```

#### OGC API Configuration
```yaml
- name: Example OGC Source
  type: ogc
  url: https://example.com/ogc/features/v1/collections/
  raw:
    collections: [data-collection]
    # Use EPSG:3006 if server supports it
    supports_epsg_3006: true
    bbox_crs: "EPSG:3006"
    stage_sr: 3006
    target_sr: 3006
```

Or for servers that don't support EPSG:3006:
```yaml
- name: Example OGC Source (CRS84)
  type: ogc
  url: https://example.com/ogc/features/v1/collections/
  raw:
    collections: [data-collection]
    # Fallback to CRS84, stage in WGS84, project to SWEREF99 TM
    supports_epsg_3006: false
    bbox_crs: "CRS84"
    stage_sr: 4326
    target_sr: 3006
```

## Best Practices Enforced

### For REST APIs
1. **Choose appropriate response format**:
   - `response_format: esrijson` (default) for servers that respect `outSR` parameter
   - `response_format: geojson` for servers that ignore `outSR` with GeoJSON
2. **Express filter bbox in SR 3006** (meters, SWEREF99 TM)
3. **EsriJSON path**: `f=json`, `outSR=3006`, stage directly in EPSG:3006
4. **GeoJSON path**: `f=geojson`, no `outSR`, stage in EPSG:4326, project to EPSG:3006
5. **Page until `exceededTransferLimit=false`** and `last_count < page_size`

### For OGC APIs
1. **Default to CRS84** (degrees) for maximum compatibility
2. **Use `crs=EPSG:3006` explicitly** if server supports it
3. **Handle axis order correctly** (CRS84 is [lon, lat], EPSG:4326 is [lat, lon])
4. **Follow `next` links** for pagination
5. **Keep CRS params on every page**

### For All Sources
1. **Every staged FC has a defined SR** (no "Unknown")
2. **GeoJSON-sourced FCs are projected to 3006** before SDE load
3. **Use Project, not Define Projection** when fixing SR mistakes

## Sanity Checks

The implementation includes automated sanity checks:

### 1. Magnitude Check
Validates that coordinate values are reasonable for the expected SR:
- SWEREF99 TM: X: 200,000-900,000, Y: 6,100,000-7,700,000
- WGS84: Longitude: -180 to 180, Latitude: -90 to 90

### 2. SR Presence Check
Ensures no feature classes have "Unknown" spatial reference.

### 3. Envelope vs Bbox Check
Compares response envelope with requested bbox to detect incorrect filtering.

### 4. Count Delta Check
Validates that larger bbox returns more (or equal) features than smaller bbox.

## Configuration

### Global Configuration (`config.yaml`)

```yaml
# Spatial Reference Configuration
spatial_reference:
  enforce_consistency: true
  
  rest_api:
    bbox_sr: 3006
    in_sr: 3006
    out_sr: 3006
    target_sr: 3006
    
  ogc_api:
    bbox_crs: "CRS84"
    prefer_crs: "EPSG:3006"
    stage_sr: 4326
    target_sr: 3006
    
  validation:
    magnitude_check: true
    envelope_check: true
    sr_presence_check: true
```

### Global Bbox (uses appropriate SR per protocol)
```yaml
# For REST APIs: SWEREF99 TM coordinates
global_bbox:
  coords: [610000, 6550000, 700000, 6650000]
  crs: 3006

# For OGC APIs: CRS84 coordinates  
global_ogc_bbox:
  coords: [16.5008129, 59.0906713, 17.6220373, 59.6050281]
  crs: "CRS84"
```

## Error Detection and Logging

The implementation provides detailed logging for SR-related operations:

```
[REST] Using SR config - bbox_sr: 3006, inSR: 3006, outSR: 3006
[REST] SR validation failed - expected 3006, detected 4326
[OGC] Using EPSG:3006 for collection-name
[STAGE] Projected file.geojson from EPSG:4326 to EPSG:3006
[SR] Validation summary for source-name:
[SR]   sr_consistency: ✓
[SR]   magnitude_check: ✓
[SR] Overall: 2/2 checks passed
```

## Testing

A comprehensive test suite validates the SR consistency implementation:

```bash
python /tmp/test_sr_consistency.py
```

Tests cover:
- Coordinate magnitude validation
- SR configuration for different source types
- GeoJSON SR detection and validation
- Error detection for mismatched coordinates

## Impact

This implementation ensures:
1. **Consistent coordinate systems** throughout the pipeline
2. **Proper data fetching** with correct spatial filters
3. **Accurate staging** with defined spatial references
4. **Reliable projections** to target coordinate system
5. **Early error detection** for SR-related issues

The changes are minimal and backward-compatible, focusing on enforcement and validation rather than breaking existing functionality.