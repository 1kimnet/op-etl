# ArcGIS REST OID-Batch Parallelism

This document describes the enhanced parallel OID-based pagination feature for ArcGIS REST API downloads.

## Overview

Large ArcGIS REST layers (e.g., NVDB road datasets with 50k+ features) can now be downloaded using parallel OID-batch processing instead of sequential offset pagination. This provides 3-10× faster downloads while respecting server limits.

## Configuration

### Basic Usage

```yaml
- name: NVDB_vag
  type: rest
  url: https://example.com/rest/services/NVDB/MapServer/2
  raw:
    use_oid_sweep: true      # Enable parallel OID downloading
    page_size: 1000          # Features per batch (default: 1000)
    max_workers: 6           # Concurrent threads (default: 6)
    out_sr: 3006
    geometry_sr: 3006
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `use_oid_sweep` | boolean | `false` | Enable parallel OID-based pagination |
| `page_size` | integer | `1000` | Number of features per batch |
| `max_workers` | integer | `6` | Maximum concurrent download threads |

### Conservative Settings for Slow Servers

```yaml
raw:
  use_oid_sweep: true
  page_size: 500           # Smaller batches
  max_workers: 3           # Fewer concurrent requests
```

## How It Works

### 1. Discovery Phase
- GET `/query?returnIdsOnly=true&f=json&where=1=1`
- Captures all Object IDs and the `objectIdFieldName`
- Logs: `"discovered N object IDs (field: OBJECTID)"`

### 2. Batch Creation
- Splits Object IDs into chunks of `page_size`
- Creates batches: `OBJECTID IN (1,2,3,...,1000)`

### 3. Parallel Fetching
- Uses ThreadPoolExecutor with `max_workers` threads
- Each thread fetches one batch concurrently
- Respects server back-pressure (Retry-After headers)
- Hard cap of 8 concurrent requests per layer

## Metrics and Logging

### Progress Logging
```
[REST] layer_name: using parallel OID-based pagination (page_size=1000, max_workers=6)
[REST] layer_name: discovered 50000 object IDs (field: OBJECTID)
[REST] layer_name: created 50 batches, fetching with 6 workers
[REST] layer_name: parallel fetch completed - 50/50 batches successful, 50000 total features in 51 requests
```

### Metrics Tracked
- `oids_total`: Total Object IDs discovered
- `batches_total`: Number of batches created
- `batches_ok`: Successfully completed batches
- `features_total`: Total features downloaded
- `request_count`: Total HTTP requests made

### Final Summary
```
[REST] layer_name: OID sweep metrics - oids_total: 50000, batches_total: 50, batches_ok: 50, features_total: 50000
```

## Fallback Behavior

### When `use_oid_sweep: false` (default)
1. Try offset-based pagination (`resultOffset` + `resultRecordCount`)
2. If `TransferLimitExceededError` → fall back to sequential OID pagination
3. If OID pagination not supported → continue with partial results

### When `use_oid_sweep: true`
1. Check if service supports advanced queries and has `objectIdField`
2. If supported → use parallel OID pagination
3. If not supported → fall back to offset pagination with warning

## Server Compatibility

### Requirements
- Service must support `supportsAdvancedQueries: true`
- Service must have an `objectIdField` (usually "OBJECTID")
- Service must support `returnIdsOnly=true` parameter

### Error Handling
- Respects `Retry-After` headers (up to 30 seconds)
- Exponential backoff on failed requests
- Maximum 3 retries per batch
- Failed batches are logged but don't stop the download

## Performance Expectations

### Expected Improvements
- **3-10× faster** for layers with 50k+ features
- Fewer total requests vs. offset pagination
- More reliable (no offset drift during server edits)
- Better progress visibility

### Server Impact
- Maximum 8 concurrent requests per layer (hard cap)
- Conservative default of 6 workers
- Respects server back-pressure signals
- Exponential backoff on failures

## Rollback

To revert to the previous behavior:
```yaml
raw:
  use_oid_sweep: false  # or remove the parameter entirely
```

This will use offset pagination with sequential OID fallback, no code changes required.

## Examples

See `docs/example-oid-sweep-config.yaml` for complete configuration examples.